import math
import pandas as pd
from sqlalchemy.orm import Session
from app.models.price_history import PriceHistory


def get_indicator_data(db: Session, symbol: str, limit: int = 300):
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
        [{"date": r.date, "close": r.close} for r in rows]
    )

    # Drop rows where close is missing
    df = df.dropna(subset=["close"]).copy()
    if df.empty:
        return [
          ]

    df["close"] = df["close"].astype(float)

    # Indicators
    df["sma_20"] = df["close"].rolling(20, min_periods=20).mean()
    df["ema_20"] = df["close"].ewm(span=20, adjust=False, min_periods=20).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    avg_gain = gain.rolling(14, min_periods=14).mean()
    avg_loss = loss.rolling(14, min_periods=14).mean()

    rs = avg_gain / avg_loss
    df["rsi_14"] = 100 - (100 / (1 + rs))

    # Convert to JSON-safe types:
    # - date -> ISO string
    # - NaN/inf -> None
    out = []
    for _, r in df.tail(200).iterrows():
        def clean(x):
            if x is None:
                return None
            try:
                if pd.isna(x):
                    return None
                xf = float(x)
                if math.isinf(xf) or math.isnan(xf):
                    return None
                return xf
            except Exception:
                return None

        out.append(
            {
                "date": r["date"].isoformat(),
                "close": clean(r["close"]),
                "sma_20": clean(r.get("sma_20")),
                "ema_20": clean(r.get("ema_20")),
                "rsi_14": clean(r.get("rsi_14")),
            }
        )

    return out