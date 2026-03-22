import pandas as pd
from sqlalchemy.orm import Session
from app.models.price_history import PriceHistory


def get_chart_data(db: Session, symbol: str, limit: int = 300):
    rows = (
        db.query(PriceHistory)
        .filter(PriceHistory.symbol == symbol.upper())
        .order_by(PriceHistory.date.asc())
        .limit(limit)
        .all()
    )

    if not rows:
        return []

    df = pd.DataFrame([
        {
            "date": r.date,
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "volume": r.volume
        }
        for r in rows
    ])

    df = df.round({
        "open": 2,
        "high": 2,
        "low": 2,
        "close": 2,
        "sma20": 2,
        "ema20": 2})

    return df.tail(200).to_dict(orient="records")