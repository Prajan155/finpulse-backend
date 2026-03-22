from pydantic import BaseModel

class WatchlistItem(BaseModel):
    symbol: str