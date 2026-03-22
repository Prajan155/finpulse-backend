from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column
from app.core.base import Base

class Stock(Base):
    __tablename__ = "stocks"

    symbol: Mapped[str] = mapped_column(String(16), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    exchange: Mapped[str | None] = mapped_column(String(50), nullable=True)
    market: Mapped[str | None] = mapped_column(String(10), nullable=True)

class SymbolOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(validation_alias="ticker", serialization_alias="symbol")
    symbol : str
    name : str | None = None
    exchange : str | None = None

class SymbolSearchOut(BaseModel):
    q: str
    results: list[SymbolOut]

class Candle(BaseModel):
    t: str                 # ISO timestamp
    o: Optional[float] = None
    h: Optional[float] = None
    l: Optional[float] = None
    c: Optional[float] = None
    v: Optional[float] = None


class HistoryOut(BaseModel):
    ticker: str
    interval: str
    range: str
    candles: list[Candle]

class QuoteOut(BaseModel):
    ticker: str
    price: Optional[float] = None
    change: Optional[float] = None
    changePercent: Optional[float] = None
    currency: Optional[str] = None
    marketState: Optional[str] = None
    asOf: str  # ISO timestamp string


class BatchQuotesIn(BaseModel):
    tickers: list[str]

class RatiosOut(BaseModel):
    ticker: str
    current_ratio: float | None = None
    debt_to_equity: float | None = None
    profit_margin: float | None = None
    return_on_equity: float | None = None

class ForecastPoint(BaseModel):
    t: str
    y: float


class ForecastOut(BaseModel):
    ticker: str
    horizon_days: int
    last_close: float | None = None
    forecast_close: float | None = None
    points: list[ForecastPoint]
    model: str
    error: str | None = None

class CorrelationOut(BaseModel):
    tickers: list[str]
    range: str
    matrix: list[list[float | None]]  # NxN correlation values

class RevExpPoint(BaseModel):
    period: str              # e.g. "2024-09-30"
    revenue: float | None = None
    expenses: float | None = None


class RevenueExpensesOut(BaseModel):
    ticker: str
    frequency: str           # "annual" or "quarterly"
    points: list[RevExpPoint]
    error: str | None = None

class SymbolOut(BaseModel):
    ticker: str
    name: str | None = None
    exchange: str | None = None
    market: str | None = None
    currency: str | None = None

