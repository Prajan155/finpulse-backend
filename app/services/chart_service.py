import pandas as pd
from sqlalchemy.orm import Session

from app.models.price_history import PriceHistory


def get_chart_data(db: Session, symbol: str, limit: int = 300):
    symbol = symbol.upper().strip()

    rows = (
        db.query(PriceHistory)
        .filter(PriceHistory.symbol == symbol)
        .order_by(PriceHistory.date.asc())
        .limit(limit)
        .all()
    )

    if not rows:
        return []

    df = pd.DataFrame(
        [
            {
                "date": r.date,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
            }
            for r in rows
        ]
    )

    if df.empty:
        return []

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("date").reset_index(drop=True)
    df["sma20"] = df["close"].rolling(20).mean()
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()

    round_map = {
        col: 2
        for col in ["open", "high", "low", "close", "sma20", "ema20"]
        if col in df.columns
    }
    if round_map:
        df = df.round(round_map)

    if "volume" in df.columns:
        df["volume"] = df["volume"].fillna(0)

    return df.tail(min(limit, 200)).to_dict(orient="records")
