from __future__ import annotations

from typing import Dict, List, Optional
from pydantic import BaseModel


class CompareRecommendationOut(BaseModel):
    label: str
    score: float


class CompareOverviewOut(BaseModel):
    summary: str


class CompareCompanyOut(BaseModel):
    symbol: str
    company_name: str
    sector: Optional[str] = None
    current_price: float = 0.0
    change: float = 0.0
    change_percent: float = 0.0
    market_cap: Optional[float] = None

    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None

    roe: Optional[float] = None
    debt_to_equity: Optional[float] = None
    revenue_growth: Optional[float] = None
    profit_margin: Optional[float] = None

    recommendation: CompareRecommendationOut
    overview: CompareOverviewOut


class CompareHistoryPointOut(BaseModel):
    date: str
    values: Dict[str, float]


class CompareMetricWinnerOut(BaseModel):
    metric: str
    winner: Optional[str] = None


class CompareResponseOut(BaseModel):
    symbols: List[str]
    companies: List[CompareCompanyOut]
    history: List[CompareHistoryPointOut]
    metric_winners: Dict[str, Optional[str]]