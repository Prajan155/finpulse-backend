from datetime import date
import pandas as pd
import yfinance as yf
from sqlalchemy.orm import Session

from app.models.price_history import PriceHistory

def _to_float(x):
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def _to_int(x):
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None


def refresh_price_history(db: Session, symbol: str, period: str = "1y", interval: str = "1d"):
    symbol = symbol.upper().strip()

    df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False)

    if df is None or df.empty:
        return 0

    df = df.reset_index()
    # Drop duplicate column names (prevents row["Open"] returning a Series)
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # detect datetime column
    if "Date" in df.columns:
        dt_col = "Date"
    elif "Datetime" in df.columns:
        dt_col = "Datetime"
    else:
        dt_col = df.columns[0]

    inserted = 0

    for _, row in df.iterrows():
        raw_dt = row[dt_col]

        # If we accidentally got a Series (e.g., duplicate column name), take the first value
        if isinstance(raw_dt, pd.Series):
            raw_dt = raw_dt.iloc[0]
        dt = pd.to_datetime(raw_dt, errors="coerce")
        if pd.isna(dt):
            continue
        d: date = dt.date()

        existing = (
            db.query(PriceHistory)
            .filter(PriceHistory.symbol == symbol, PriceHistory.date == d)
            .first()
        )

        def clean(x):
            # If we somehow still get a Series, take the first value
            if isinstance(x, pd.Series):
                x = x.iloc[0]
            if x is None or pd.isna(x):
                return None
            return float(x)
        def clean_int(x):
            if isinstance(x, pd.Series):
                x = x.iloc[0]
            if x is None:
                return None
            x = pd.to_numeric(x, errors="coerce")
            if pd.isna(x):
                return None
            return int(x)

        o = clean(row.get("Open"))
        h = clean(row.get("High"))
        l = clean(row.get("Low"))
        c = clean(row.get("Close"))
        v = clean_int(row.get("Volume"))
        if existing:
            existing.open = o
            existing.high = h
            existing.low = l
            existing.close = c
            existing.volume = v
        else:
            db.add(
                PriceHistory(
                    symbol=symbol,
                    date=d,
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    volume=v,
                )
            )
            inserted += 1

    db.commit()
    return inserted


def get_price_history(db: Session, symbol: str, limit: int = 252):
    symbol = symbol.upper().strip()

    rows = (
        db.query(PriceHistory)
        .filter(PriceHistory.symbol == symbol)
        .order_by(PriceHistory.date.asc())
        .limit(limit)
        .all()
    )

    return rows