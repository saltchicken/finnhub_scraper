import os
import argparse 
import numpy as np
import pandas as pd # ‼️ Added missing pandas import
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy.exc import IntegrityError 

from .database import DatabaseClient
from .models import MetricSnapshot, FinancialSnapshot 
from .errors import ConfigError, FinnhubAPIError
from .client import FinnHubClient 

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
    raise ConfigError("Missing FINNHUB_API_KEY. Please set it in your .env file.")


def run_metrics_update(db: DatabaseClient, client: FinnHubClient):
    """
    Main function to fetch all symbols from the DB,
    check if they need updates, and save new *metric* snapshots.
    """
    print("Starting daily metrics update...")
    
    # if not db.is_within_allowed_update_window():
    #     print("Not within the allowed update window (6 PM - 2 AM PT). Exiting.")
    #     return
    print("Within allowed update window. Proceeding...")
    
    symbols_to_process = db.get_all_symbols()
    print(f"Found {len(symbols_to_process)} symbols to process for metrics.")
    processed_count = 0
    skipped_count = 0
    
    # Loop through all symbols
    for i, symbol in enumerate(symbols_to_process):
        
        if db.was_updated_in_nightly_window(symbol):
            if skipped_count % 100 == 0:
                print(f"'{symbol}' was already updated in this window. Skipping. (Total skipped: {skipped_count+1})")
            skipped_count += 1
            continue
        
        try:
            print(f"Processing {symbol} ({i+1}/{len(symbols_to_process)})")
            
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
        
        except FinnhubAPIError as e:
            print(f"Skipping {symbol} due to API Error: {e.args[0]}")
        except Exception as e:
            print(f"Error creating MetricSnapshot object for {symbol}: {e}")

    if processed_count > 0:
        print(f"\nCommitting {processed_count} new metric snapshots to the database...")
        db.session.commit()
        print(f"Successfully committed {processed_count} snapshots.")
    else:
        print("\nNo new metric snapshots to commit.")
    
    print(f"Total processed: {processed_count}, Total skipped: {skipped_count}")


def run_financials_update(db: DatabaseClient, client: FinnHubClient):
    """
    Fetches and stores quarterly financial reports for all symbols.
    """
    print("Starting quarterly financials update...")
    
    symbols_to_process = db.get_all_symbols()
    print(f"Found {len(symbols_to_process)} symbols to process for financials.")
    processed_count = 0
    skipped_count = 0

    for i, symbol in enumerate(symbols_to_process):
        print(f"Processing financials for {symbol} ({i+1}/{len(symbols_to_process)})")
        try:
            financials = client.get_financials_reported(symbol)
            
            if not financials or "data" not in financials:
                print(f"No financial data returned for {symbol}. Skipping.")
                skipped_count += 1
                continue

            # Loop through ALL reported quarters from the API
            for data in financials["data"]:
                year = data.get("year")
                quarter = data.get("quarter")
                report = data.get("report", {})

                if not year or not quarter:
                    print(f"Skipping report for {symbol} due to missing year/quarter.")
                    continue

                revenue = np.nan
                earnings_per_share_diluted = np.nan
                net_income_loss = np.nan
                
                # Iterate through all report types (ic, bs, cf) to find concepts
                for report_type in report.keys():
                    for item in report[report_type]:
                        concept = item.get("concept")
                        value = item.get("value")

                        if concept in (
                            "us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax",
                            "Revenues",
                        ):
                            revenue = value
                        elif concept == "us-gaap_EarningsPerShareDiluted":
                            earnings_per_share_diluted = value
                        elif concept == "us-gaap_NetIncomeLoss":
                            net_income_loss = value

                # ‼️ Use pd.isna() here (which requires pandas)
                if pd.isna(revenue) and pd.isna(net_income_loss) and pd.isna(earnings_per_share_diluted):
                    # print(f"No useful financial data found for {symbol} Q{quarter} {year}.")
                    continue
                
                net_profit_margin = np.nan
                # ‼️ Use pd.isna() here
                if not pd.isna(net_income_loss) and not pd.isna(revenue):
                    try:
                        float_revenue = float(revenue)
                        if float_revenue != 0:
                            net_profit_margin = (float(net_income_loss) / float_revenue) * 100
                    except (ValueError, TypeError, ZeroDivisionError):
                        net_profit_margin = np.nan
                
                try:
                    snapshot = FinancialSnapshot(
                        symbol=symbol.upper(),
                        year=year,
                        quarter=quarter,
                        revenue=revenue,
                        earnings_per_share_diluted=earnings_per_share_diluted,
                        net_income_loss=net_income_loss,
                        net_profit_margin=net_profit_margin,
                    )
                    db.session.add(snapshot)
                    db.session.commit()
                    processed_count += 1
                    print(f"  > Added financial snapshot for {symbol} Q{quarter} {year}")

                except IntegrityError:
                    # This is normal if the data already exists
                    # print(f"  > Snapshot for {symbol} Q{quarter} {year} already exists. Skipping.")
                    db.session.rollback()
                except Exception as e:
                    print(f"  > Error adding snapshot for {symbol} Q{quarter} {year}: {e}")
                    db.session.rollback()
        
        except FinnhubAPIError as e:
            print(f"Skipping {symbol} due to API Error: {e.args[0]}")
            skipped_count += 1
        except Exception as e:
            print(f"An unhandled error occurred for {symbol}: {e}") # ‼️ This is where your error was reported
            skipped_count += 1

    print(f"\nSuccessfully added {processed_count} new quarterly reports.")
    print(f"Total symbols skipped due to errors or no data: {skipped_count}")


def main():
    parser = argparse.ArgumentParser(description="Finnhub Scraper CLI")
    parser.add_argument(
        "--task",
        type=str,
        choices=["metrics", "financials"],
        default="metrics",
        help="The task to run: 'metrics' (default) for daily basic financials, or 'financials' for quarterly reports."
    )
    args = parser.parse_args()

    db = None
    try:
        db = DatabaseClient()
        db.init_db()
        client = FinnHubClient()
        
        if args.task == "financials":
            run_financials_update(db, client)
        else:
            # Default to metrics
            run_metrics_update(db, client)

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
