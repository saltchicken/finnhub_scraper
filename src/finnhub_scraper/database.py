# ‼️ New file to manage database connections
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# ‼️ Load .env file to get the DATABASE_URL
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("Missing DATABASE_URL. Please set it in your .env file (e.g., postgresql://user:pass@host/db)")

# ‼️ Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# ‼️ Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ‼️ Create a Base class for your models to inherit from
Base = declarative_base()

def init_db():
    """
    Initializes the database by creating all tables
    defined by models inheriting from Base.
    """
    print("Initializing database tables...")
    Base.metadata.create_all(bind=engine)
    print("Database tables created.")
