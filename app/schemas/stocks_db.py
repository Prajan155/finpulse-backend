from pydantic import BaseModel

class StockCreate(BaseModel):
    symbol: str
    name: str | None = None
    exchange: str | None = None
    market: str | None = None

class StockOut(StockCreate):
    pass