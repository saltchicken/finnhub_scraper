import finnhub
import os
import time
import threading
from dotenv import load_dotenv
import json
from datetime import datetime
from .database import DatabaseClient
from .models import MetricSnapshot
from .errors import ConfigError, FinnhubAPIError # ‼️ Import custom errors

KEY_MAPPING = {
    "52WeekHigh": "week52_high",
    "52WeekHighDate": "week52_high_date",
    "52WeekLow": "week52_low",
    "3MonthAverageTradingVolume": "month3_average_trading_volume",
    "dividendPerShareTTM": "dividend_per_share_ttm",
    "10DayAverageTradingVolume": "day10_average_trading_volume",
    "beta": "beta",
    "epsTTM": "eps_ttm",
    "epsGrowth5Y": "eps_growth_5y",
    "revenueGrowth5Y": "revenue_growth_5y",
    "focfCagr5Y": "focf_cagr_5y",
    "netProfitMarginTTM": "net_profit_margin_ttm",
    "grossMarginTTM": "gross_margin_ttm",
    "operatingMarginTTM": "operating_margin_ttm",
    "roeTTM": "roe_ttm",
    "roaTTM": "roa_ttm",
    "roiTTM": "roi_ttm",
    "cashFlowPerShareTTM": "cash_flow_per_share_ttm",
    "peTTM": "pe_ttm",
    "pfcfShareTTM": "pfcf_share_ttm",
    "psTTM": "ps_ttm",
    "pbTTM": "pb_ttm",
    "currentDividendYieldTTM": "current_dividend_yield_ttm",
    "dividendGrowthRate5Y": "dividend_growth_rate_5y",
    "payoutRatioTTM": "payout_ratio_ttm",
    "longTermDebt/equityQuarterly": "long_term_debt_equity_quarterly",
    "currentRatioQuarterly": "current_ratio_quarterly",
}

# --- Dependencies ---
load_dotenv()
API_KEY = os.getenv("FINNHUB_API_KEY")
if not API_KEY:
    # ‼️ Use our specific ConfigError
    raise ConfigError("Missing FINNHUB_API_KEY. Please set it in your .env file.")

class RateLimiter:
    """
    A decorator class to limit the rate of function calls.
    """
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self, func):
        def wrapped(*args, **kwargs):
            with self.lock:
                now = time.time()
                self.calls = [t for t in self.calls if now - t < self.period]
                
                if len(self.calls) >= self.max_calls:
                    sleep_time = self.period - (now - self.calls[0])
                    print(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds.")
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    
                    now = time.time()
                    self.calls = [t for t in self.calls if now - t < self.period]
                
                self.calls.append(time.time())
            return func(*args, **kwargs)
        return wrapped

# --- Finnhub Client ---
class FinnHubClient:
    """
    Minimal client containing only the requested functionality.
    """
    def __init__(self, api_key=API_KEY):
        self.client = finnhub.Client(api_key=api_key)

    @RateLimiter(1, 1.25)
    def get_company_basic_financials(self, symbol: str):
        """
        Gets company basic financials from Finnhub.
        """
        print(f"Fetching basic financials for {symbol}...")
        try:
            metrics = self.client.company_basic_financials(symbol=symbol, metric="all")
            return metrics
        except Exception as e:
            # ‼️ Print the error, but also raise our custom exception
            # ‼️ This stops the program from proceeding with 'None'
            print(f"Error getting metrics for {symbol}: {e}")
            raise FinnhubAPIError(f"API call failed for {symbol}: {e}") from e

def main():
    """
    Main function to fetch all symbols from the DB,
    check if they need updates, and save new snapshots.
    """
    db = None
    try:
        # ‼️ This can now raise ConfigError, which we catch below
        db = DatabaseClient()
        db.init_db()
        
        # if not db.is_within_allowed_update_window():
        #     print("Not within the allowed update window (6 PM - 2 AM PT). Exiting.")
        #     return
        print("Within allowed update window. Proceeding...")
        client = FinnHubClient()
        
        symbols_to_process = db.get_all_symbols()
        print(f"Found {len(symbols_to_process)} symbols to process.")
        processed_count = 0
        skipped_count = 0
        
        # Loop through all symbols
        for i, symbol in enumerate(symbols_to_process):
            
            if db.was_updated_in_nightly_window(symbol):
                # Only print skip message periodically to avoid log spam
                if skipped_count % 100 == 0:
                    print(f"'{symbol}' was already updated in this window. Skipping. (Total skipped: {skipped_count+1})")
                skipped_count += 1
                continue
            
            # ‼️ Wrap processing for a single symbol in a try/except
            try:
                print(f"Processing {symbol} ({i+1}/{len(symbols_to_process)})")
                
                # ‼️ This can now raise FinnhubAPIError
                financials = client.get_company_basic_financials(symbol)
                
                if not financials or 'metric' not in financials:
                    print(f"Failed to retrieve data or 'metric' key missing for {symbol}.")
                    continue
                    
                metrics_data = financials.get('metric', {})
                
                snapshot_data = {}
                for api_key, db_key in KEY_MAPPING.items():
                    value = metrics_data.get(api_key, None)
                    
                    if value is None:
                        snapshot_data[db_key] = None
                        continue

                    if db_key == "week52_high_date":
                        try:
                            snapshot_data[db_key] = datetime.strptime(value, "%Y-%m-%d").date()
                        except (ValueError, TypeError):
                            print(f"Warning: Could not parse date '{value}' for {symbol}")
                            snapshot_data[db_key] = None
                    else:
                        try:
                            snapshot_data[db_key] = float(value)
                        except (ValueError, TypeError):
                            print(f"Warning: Could not parse float '{value}' for {symbol}")
                            snapshot_data[db_key] = None
                
                snapshot = MetricSnapshot(
                    symbol=symbol.upper(),
                    **snapshot_data
                )
                db.session.add(snapshot)
                processed_count += 1
            
            # ‼️ Catch the specific API error, log it, and continue the loop
            except FinnhubAPIError as e:
                print(f"Skipping {symbol} due to API Error: {e.args[0]}")
            # ‼️ Catch any other error during snapshot creation
            except Exception as e:
                print(f"Error creating MetricSnapshot object for {symbol}: {e}")

        if processed_count > 0:
            print(f"\nCommitting {processed_count} new snapshots to the database...")
            db.session.commit()
            print(f"Successfully committed {processed_count} snapshots.")
        else:
            print("\nNo new snapshots to commit.")
        
        print(f"Total processed: {processed_count}, Total skipped: {skipped_count}")
    
    # ‼️ Catch our custom config error at startup
    except ConfigError as e:
        print(f"Configuration Error: {e}")
        
    except Exception as e:
        print(f"An unhandled error occurred in main: {e}")
        if db:
            db.session.rollback()
    finally:
        if db:
            db.session.close()
            print("Database session closed.")

# --- Example Usage ---
if __name__ == "__main__":
    main()
