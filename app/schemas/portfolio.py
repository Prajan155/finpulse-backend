from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict
from datetime import date


class PortfolioPositionIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


    symbol: str
    quantity: float = Field(0.0, ge=0)
    average_price: float = Field(0.0, ge=0)
    shares: float = 0.0
    avg_buy_price: float = 0.0

    def model_post_init(self, context):
        if self.quantity > 0:
            self.shares = self.quantity
        if self.average_price > 0:
            self.avg_buy_price = self.average_price    
        return super().model_post_init(context)


class PortfolioRecommendationIn(BaseModel):
    positions: List[PortfolioPositionIn]


class PortfolioOverviewIn(BaseModel):
    positions: List[PortfolioPositionIn]


class PortfolioRebalanceOut(BaseModel):
    symbol: str
    action: str
    current_weight: float
    target_weight: float
    score: float
    confidence: float
    reason: str


class PortfolioRecommendationOut(BaseModel):
    portfolio_score: float
    diversification_score: float
    risk_level: str
    sector_concentration: Dict[str, float]
    recommendations: List[PortfolioRebalanceOut]
    warnings: List[str]


class PortfolioHoldingOut(BaseModel):
    symbol: str
    shares: float
    avg_buy_price: float
    weight: float
    recommendation: str
    score: float
    confidence: float
    current_price: float
    target_price: float
    upside_percent: float
    sector: str
    regime: str

    invested_value: float = 0.0
    current_value: float = 0.0
    gain_loss: float = 0.0
    return_percent: float = 0.0


class PortfolioRecommendationOutItem(BaseModel):
    symbol: str
    action: str
    current_weight: float
    target_weight: float
    score: float
    confidence: float
    reason: str


class PortfolioOverviewOut(BaseModel):
    total_invested: float = 0.0
    total_value: float = 0.0
    total_gain_loss: float = 0.0
    total_return_percent: float = 0.0

    recommendation: str
    portfolio_score: float
    confidence: float
    diversification_score: float
    concentration_score: float
    risk_score: float
    risk_level: str
    sector_exposure: Dict[str, float]
    holdings: List[PortfolioHoldingOut]
    recommendations: List[PortfolioRecommendationOutItem]
    top_risks: List[str]
    top_opportunities: List[str]


class PortfolioPerformancePoint(BaseModel):
    date: date
    portfolio_value: float
    portfolio_return: float
    nifty_return: float | None = None
    sensex_return: float | None = None


class PortfolioPerformanceOut(BaseModel):
    points: List[PortfolioPerformancePoint]
    current_value: float
    total_return: float
    best_symbol: str | None = None
    worst_symbol: str | None = None