import finnhub
import os
import time
import threading
from dotenv import load_dotenv
import json
import argparse  # ‼️ Added to accept symbols as arguments
from datetime import datetime  # ‼️ Added for date parsing

# ‼️ Import DB session, init function, and models
from .database import SessionLocal, init_db
from .models import Company, MetricSnapshot
from sqlalchemy import select # ‼️ Added for querying

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

# --- Dependencies from the original file ---
load_dotenv()
API_KEY = os.getenv("FINNHUB_API_KEY")

# ‼️ Also check for DATABASE_URL (though database.py does this too)
DATABASE_URL = os.getenv("DATABASE_URL")

if not API_KEY:
    raise ValueError("Missing FINNHUB_API_KEY. Please set it in your .env file.")
if not DATABASE_URL:
     raise ValueError("Missing DATABASE_URL. Please set it in your .env file.")

class RateLimiter:
    """
    A decorator class to limit the rate of function calls.
    Copied from src/trader/external_api/finnhub_client.py
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

# --- Extracted Functionality ---
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
            print(f"Error getting metrics for {symbol}: {e}")
            return None

# ‼️ Helper function to ensure the company exists in the DB
def get_or_create_company(db_session, symbol: str) -> Company:
    """
    Fetches a company by symbol or creates it if it doesn't exist.
    """
    # Try to fetch the company
    statement = select(Company).where(Company.symbol == symbol)
    company = db_session.execute(statement).scalar_one_or_none()
    
    if company:
        print(f"Found existing company: {symbol}")
        return company
    else:
        # Create it
        print(f"Creating new company: {symbol}")
        new_company = Company(symbol=symbol)
        db_session.add(new_company)
        # We commit here to ensure the company exists before the snapshot
        # is added, which has a foreign key constraint.
        # A more complex setup might add all companies, commit, 
        # then add all snapshots. This is simpler.
        try:
            db_session.commit()
            db_session.refresh(new_company)
            print(f"Committed new company: {symbol}")
            return new_company
        except Exception as e:
            print(f"Error creating company {symbol}: {e}")
            db_session.rollback()
            return None

# ‼️ Main function updated for DB operations
def main():
    """
    Main function to fetch metrics and save them to the database.
    """
    # ‼️ Set up argument parsing to accept symbols
    parser = argparse.ArgumentParser(description="Fetch Finnhub metrics and save to DB.")
    parser.add_argument(
        "symbols", 
        metavar="SYMBOL", 
        type=str, 
        nargs="+", 
        help="One or more stock symbols to fetch (e.g., AAPL MSFT)"
    )
    args = parser.parse_args()
    
    # ‼️ 1. Initialize the database (create tables if they don't exist)
    init_db()
    
    client = FinnHubClient()
    
    # ‼️ 2. Get a database session
    with SessionLocal() as db_session:
        for symbol in args.symbols:
            # ‼️ 3. Ensure the parent Company exists
            company = get_or_create_company(db_session, symbol.upper())
            if not company:
                print(f"Skipping snapshot for {symbol} due to company creation error.")
                continue

            # ‼️ 4. Fetch the financial data
            financials = client.get_company_basic_financials(symbol)
            
            if not financials or 'metric' not in financials:
                print(f"Failed to retrieve data or 'metric' key missing for {symbol}.")
                continue
                
            metrics_data = financials.get('metric', {})
            
            # ‼️ 5. Build the MetricSnapshot object data
            snapshot_data = {}
            for api_key, db_key in KEY_MAPPING.items():
                value = metrics_data.get(api_key, None)
                
                # Handle NULLs: if value is None, it will be stored as NULL
                if value is None:
                    snapshot_data[db_key] = None
                    continue

                # ‼️ Specific handling for date column
                if db_key == "week52_high_date":
                    try:
                        # Parse "YYYY-MM-DD" string into a date object
                        snapshot_data[db_key] = datetime.strptime(value, "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        print(f"Warning: Could not parse date '{value}' for {symbol}")
                        snapshot_data[db_key] = None
                else:
                    # For all other (Float) columns
                    try:
                        snapshot_data[db_key] = float(value)
                    except (ValueError, TypeError):
                        print(f"Warning: Could not parse float '{value}' for {symbol}")
                        snapshot_data[db_key] = None

            # ‼️ 6. Create the snapshot instance
            try:
                snapshot = MetricSnapshot(
                    symbol=symbol.upper(),
                    **snapshot_data
                )
                
                # ‼️ 7. Add to the session
                db_session.add(snapshot)
                print(f"Staged snapshot for {symbol} for commit.")
            
            except Exception as e:
                print(f"Error creating MetricSnapshot object for {symbol}: {e}")

        # ‼️ 8. Commit all snapshots at the end of the loop
        try:
            db_session.commit()
            print(f"\nSuccessfully committed snapshots for: {', '.join(args.symbols)}")
        except Exception as e:
            print(f"\nError committing snapshots to database: {e}")
            db_session.rollback()

# --- Example Usage ---
if __name__ == "__main__":
    main()
