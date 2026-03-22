from pydantic import BaseModel, Field, ConfigDict
from datetime import date
from pydantic import BaseModel
from typing import List, Dict

class SymbolOut(BaseModel):
    # if your service still returns "ticker", this will accept it
    model_config = ConfigDict(populate_by_name=True)
    symbol: str = Field(validation_alias="ticker")
    name: str | None = None
    exchange: str | None = None
    market: str | None = None
    currency: str | None = None

class SymbolSearchItem(BaseModel):
    symbol: str
    name: str
    exchange: str
    market: str
    currency: str

class SymbolSearchOut(BaseModel):
    query: str
    results: list[SymbolSearchItem]

class HistoryPoint(BaseModel):
    date: date
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None

class HistoryOut(BaseModel):
    symbol: str
    points: list[HistoryPoint]

class QuoteOut(BaseModel):
    symbol: str
    name: str | None = None
    exchange: str | None = None
    market: str | None = None
    currency: str | None = None
    price: float | None = None
    change: float | None = None
    changePercent: float | None = None
    marketState: str | None = None
    asOf: str | None = None

class RatiosOut(BaseModel):
    symbol: str
    market_cap: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    ps_ttm: float | None = None
    dividend_yield: float | None = None
    beta: float | None = None

    profit_margin: float | None = None
    operating_margin: float | None = None

    roe: float | None = None
    roa: float | None = None
    debt_to_equity: float | None = None
    current_ratio: float | None = None
    quick_ratio: float | None = None

class ForecastPoint(BaseModel):
    date: str
    value: float | None = None

class ForecastOut(BaseModel):
    symbol: str
    model: str = "ARIMA"
    interval: str
    horizon_days: int
    last_close: float | None = None
    forecast_close: float | None = None
    error: str | None = None
    points: list[ForecastPoint]

class CorrelationIn(BaseModel):
    symbols: list[str] = Field(min_length=2)
    window_days: int 


class CorrelationOut(BaseModel):
    symbols: list[str]
    window_days: int
    matrix: list[list[float | None]]

class RevenueExpensesPoint(BaseModel):
    date: date
    revenue: float | None = None
    expenses: float | None = None


class RevenueExpensesOut(BaseModel):
    symbol: str
    points: list[RevenueExpensesPoint]
    currency: str | None = None

class RecommendationOut(BaseModel):
    symbol: str
    regime: str
    sector: str
    recommendation: str
    score: float
    confidence: float
    current_price: float
    target_price: float
    upside_percent: float
    signals: Dict[str, str]
    reasons: List[str]

class StockOverviewOut(BaseModel):
    symbol: str
    quote: QuoteOut
    ratios: RatiosOut
    recommendation: RecommendationOut
    forecast: ForecastOut | None = None
    revenue_expenses: RevenueExpensesOut | None = None

    sector: str | None = None
    industry: str | None = None
    market_cap: float | None = None
    currency: str | None = None
    summary: str | None = None

class BatchQuotesIn(BaseModel):
    symbols: List[str]