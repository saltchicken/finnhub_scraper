# ‼️ New file for custom exceptions
"""
Defines custom exception types for the application,
inspired by the idea of creating specific errors for each case.
"""

class FinnhubScraperError(Exception):
    """Base exception for this application."""
    pass

class ConfigError(FinnhubScraperError):
    """Raised when a required environment variable is missing."""
    pass

class FinnhubAPIError(FinnhubScraperError):
    """Raised when the Finnhub API client fails a request."""
    pass
