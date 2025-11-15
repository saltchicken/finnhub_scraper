
import finnhub
import os
import time
import threading
from dotenv import load_dotenv
from .errors import ConfigError, FinnhubAPIError

# --- Dependencies ---
load_dotenv()
API_KEY = os.getenv("FINNHUB_API_KEY")
if not API_KEY:
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
    Client for Finnhub API, including rate limiting.
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
            raise FinnhubAPIError(f"API call failed for {symbol}: {e}") from e


    @RateLimiter(1, 1.25)
    def get_financials_reported(self, symbol: str):
        """
        Gets quarterly financials_reported from Finnhub.
        """
        print(f"Fetching quarterly financials for {symbol}...")
        try:
            financials = self.client.financials_reported(
                symbol=symbol, freq="quarterly"
            )
            return financials
        except Exception as e:
            print(f"Error getting financials_reported for {symbol}: {e}")
            raise FinnhubAPIError(f"API call failed for {symbol}: {e}") from e