from pydantic import BaseModel


class PortfolioHoldingCreate(BaseModel):
    symbol: str
    quantity: float
    average_price: float


class PortfolioHoldingUpdate(BaseModel):
    quantity: float
    average_price: float


class PortfolioHoldingOut(BaseModel):
    id: int
    user_id: int
    symbol: str
    quantity: float
    average_price: float

    class Config:
        from_attributes = True