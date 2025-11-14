import finnhub
import os
import time
import threading
from dotenv import load_dotenv
import json

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
# Load .env file for API keys
load_dotenv()
API_KEY = os.getenv("FINNHUB_API_KEY")

if not API_KEY:
    print("Warning: FINNHUB_API_KEY not found in .env file.")
    # You can set it manually for testing:
    # API_KEY = "your_key_here"

if not API_KEY:
    raise ValueError("Missing FINNHUB_API_KEY. Please set it in your .env file or manually.")

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
                # Remove timestamps older than the period
                self.calls = [t for t in self.calls if now - t < self.period]
                
                if len(self.calls) >= self.max_calls:
                    # Calculate sleep time
                    sleep_time = self.period - (now - self.calls[0])
                    print(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds.")
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    
                    # After sleeping, update the timestamps list
                    now = time.time()
                    self.calls = [t for t in self.calls if now - t < self.period]

                # Add new call timestamp and execute function
                self.calls.append(time.time())
            return func(*args, **kwargs)
        return wrapped

# --- Extracted Functionality ---
class FinnHubClient:
    """
    Minimal client containing only the requested functionality.
    """
    def __init__(self, api_key=API_KEY):
        """
        Initializes the Finnhub client.
        """
        self.client = finnhub.Client(api_key=api_key)

    @RateLimiter(1, 1.25)  # Original file limits to 1 call every 1.25 seconds
    def get_company_basic_financials(self, symbol: str):
        """
        Gets company basic financials from Finnhub.
        This was named 'get_metrics' in the original file.
        """
        print(f"Fetching basic financials for {symbol}...")
        try:
            # This is the specific SDK call you asked about
            metrics = self.client.company_basic_financials(symbol=symbol, metric="all")
            return metrics
        except Exception as e:
            print(f"Error getting metrics for {symbol}: {e}")
            return None

# ‼️ Wrap your executable code in a main() function
def main():
    """
    Main function to run the client example.
    """
    # 1. Ensure you have a .env file in the same directory
    #    with the line: FINNHUB_API_KEY="your_api_key_here"
    # 2. Ensure you have the required libraries:
    #    pip install finnhub-python python-dotenv
    
    client = FinnHubClient()
    
    symbol_to_test = "AAPL"
    financials = client.get_company_basic_financials(symbol_to_test)
    
    if financials and 'metric' in financials:
        print(f"\nSuccessfully retrieved data for {symbol_to_test}:")
        print("---")
        
        # ‼️ Get the nested metrics dictionary
        metrics_data = financials.get('metric', {})
        
        # ‼️ Iterate over your KEY_MAPPING and print the values
        for api_key, db_key in KEY_MAPPING.items():
            # Find the value in the returned data using the api_key
            value = metrics_data.get(api_key, "N/A (Key not found)")
            print(f"{api_key}: {value}")
            
        print("---")
        # You can uncomment this if you still want to see the full JSON
        # print("\nFull JSON response:")
        # print(json.dumps(financials, indent=2))
        
    else:
        print(f"\nFailed to retrieve data or 'metric' key missing for {symbol_to_test}.")

# --- Example Usage ---
if __name__ == "__main__":
    main() # ‼️ Call the main() function
