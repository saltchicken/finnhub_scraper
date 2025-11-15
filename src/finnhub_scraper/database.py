# ‼️ This file has been refactored into a class
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from datetime import datetime, timedelta # ‼️ Added
from zoneinfo import ZoneInfo # ‼️ Added
# ‼️ Import Base and models from our models.py file
from .models import Base, MetricSnapshot, Company, FinancialSnapshot # ‼️ Added FinancialSnapshot
from .errors import ConfigError # ‼️ Import our custom error

# ‼️ Load .env file to get the DATABASE_URL
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    # ‼️ Use our specific ConfigError
    raise ConfigError("Missing DATABASE_URL. Please set it in your .env file (e.g., postgresql://user:pass@host/db)")

# ‼️ This class now manages the database connection and session
class DatabaseClient:
    def __init__(self):
        """
        Initializes the database engine and session.
        """
        self.engine = create_engine(DATABASE_URL)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.session = SessionLocal()
        print("Database session started.")

    def init_db(self):
        """
        Initializes the database by creating all tables
        defined by models inheriting from Base.
        """
        print("Initializing database tables...")
        Base.metadata.create_all(bind=self.engine)
        print("Database tables created.")

    def get_all_symbols(self):
        """
        ‼️ Queries the database and returns a list of all symbols
        from the Company table.
        """
        print("Fetching all symbols from 'companies' table...")
        return [row.symbol for row in self.session.query(Company.symbol).all()]

    def is_within_allowed_update_window(self):
        """
        ‼️ Checks if the current time is within the nightly update window
        (6 PM - 2 AM Pacific Time).
        """
        now = datetime.now(ZoneInfo("America/Los_Angeles"))
        # Window is 6 PM (18:00) to 2 AM (02:00)
        if now.hour >= 18 or now.hour < 2:
            return True
        return False

    def was_updated_in_nightly_window(self, symbol: str):
        """
        ‼️ Checks if a symbol has already received a MetricSnapshot
        during the current nightly window.
        """
        now = datetime.now(ZoneInfo("America/Los_Angeles"))
        
        # Determine the 6PM start time of the *current* window
        if now.hour < 2:
            # We are in the early morning (e.g., 1AM on June 29)
            # The window started yesterday (June 28, 6PM)
            window_start = (now - timedelta(days=1)).replace(
                hour=18, minute=0, second=0, microsecond=0
            )
        else:
            # We are in the evening (e.g., 8PM on June 29)
            # The window started today (June 29, 6PM)
            window_start = now.replace(hour=18, minute=0, second=0, microsecond=0)
        
        # Window end is 8 hours after start (2 AM next day)
        window_end = window_start + timedelta(hours=8)

        # Convert to UTC for database query (since DB timestamps are naive UTC)
        start_utc = window_start.astimezone(ZoneInfo("UTC"))
        end_utc = window_end.astimezone(ZoneInfo("UTC"))

        # Check if a snapshot exists for this symbol within the window
        return (
            self.session.query(MetricSnapshot)
            .filter(
                MetricSnapshot.symbol == symbol,
                MetricSnapshot.timestamp >= start_utc,
                MetricSnapshot.timestamp < end_utc,
            )
            .first()
            is not None
        )
