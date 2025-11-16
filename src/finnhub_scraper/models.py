
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey, func
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import UniqueConstraint

Base = declarative_base()

class Company(Base):
    """
    A company with a unique stock symbol.
    This is required to satisfy the foreign key in MetricSnapshot.
    """
    __tablename__ = "companies"
    symbol = Column(String, primary_key=True, index=True)
    description = Column(String)
    ipo = Column(Date)
    weburl = Column(String)
    sector = Column(String)
    
    snapshots = relationship(
        "MetricSnapshot",  
        back_populates="company", 
        cascade="all, delete-orphan"
    )
    
    financial_snapshots = relationship(
        "FinancialSnapshot",
        back_populates="company",
        cascade="all, delete-orphan"
    )

class MetricSnapshot(Base):
    """ This is your existing MetricSnapshot model """
    __tablename__ = "metric_snapshots"
    id = Column(Integer, primary_key=True)
    symbol = Column(String, ForeignKey("companies.symbol"), nullable=False, index=True) 
    timestamp = Column(DateTime, default=func.now())
    
    week52_high = Column(Float)
    week52_high_date = Column(Date)
    week52_low = Column(Float)
    month3_average_trading_volume = Column(Float)
    dividend_per_share_ttm = Column(Float)
    day10_average_trading_volume = Column(Float)
    beta = Column(Float)
    eps_ttm = Column(Float)
    eps_growth_5y = Column(Float)
    revenue_growth_5y = Column(Float)
    focf_cagr_5y = Column(Float)
    net_profit_margin_ttm = Column(Float)
    gross_margin_ttm = Column(Float)
    operating_margin_ttm = Column(Float)
    roe_ttm = Column(Float)
    roa_ttm = Column(Float)
    roi_ttm = Column(Float)
    cash_flow_per_share_ttm = Column(Float)
    pe_ttm = Column(Float)
    pfcf_share_ttm = Column(Float)
    ps_ttm = Column(Float)
    pb_ttm = Column(Float)
    current_dividend_yield_ttm = Column(Float)
    dividend_growth_rate_5y = Column(Float)
    payout_ratio_ttm = Column(Float)
    long_term_debt_equity_quarterly = Column(Float)
    current_ratio_quarterly = Column(Float)
    
    company = relationship("Company", back_populates="snapshots")



class FinancialSnapshot(Base):
    __tablename__ = "financial_snapshots"
    id = Column(Integer, primary_key=True)
    symbol = Column(String, ForeignKey("companies.symbol"), nullable=False, index=True)
    timestamp = Column(DateTime, default=func.now())
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    
    revenue = Column(Float)
    earnings_per_share_diluted = Column(Float)
    net_income_loss = Column(Float)
    net_profit_margin = Column(Float)
    

    company = relationship("Company", back_populates="financial_snapshots")
    

    __table_args__ = (
        UniqueConstraint("symbol", "year", "quarter", name="uix_financial_snapshot"),
    )