from __future__ import annotations

from typing import List, Dict
import pandas as pd

from app.services.yahoo_service import get_history


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _normalize_positions(positions: List[dict]) -> List[dict]:
    cleaned = []
    total_value = 0.0

    for p in positions:
        symbol = str(p.get("symbol", "")).upper().strip()
        shares = _safe_float(p.get("shares"), 0.0)
        avg_buy_price = _safe_float(p.get("avg_buy_price"), 0.0)

        if not symbol or shares <= 0:
            continue

        invested_value = shares * avg_buy_price
        total_value += invested_value

        cleaned.append(
            {
                "symbol": symbol,
                "shares": shares,
                "avg_buy_price": avg_buy_price,
                "invested_value": invested_value,
            }
        )

    if not cleaned:
        return []

    # derive weights from actual invested values
    for item in cleaned:
        item["weight"] = (
            item["invested_value"] / total_value if total_value > 0 else 0.0
        )

    return cleaned


def _history_to_close_series(symbol: str, range_: str = "6mo") -> pd.Series | None:
    data = get_history(symbol, interval="1d", range_=range_)
    candles = data.get("candles", [])

    if not candles:
        return None

    rows = []
    for c in candles:
        dt_value = c.get("t")
        close = c.get("c")
        if dt_value is None or close is None:
            continue
        rows.append((pd.to_datetime(dt_value), float(close)))

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=["date", "close"]).drop_duplicates(subset=["date"])
    df = df.sort_values("date")
    df = df.set_index("date")

    series = df["close"].dropna()
    return series if not series.empty else None


def _build_benchmark_series(symbol: str, range_: str = "6mo") -> pd.Series | None:
    series = _history_to_close_series(symbol, range_=range_)
    if series is None or series.empty:
        return None
    return series


def get_portfolio_performance(positions: List[dict]) -> Dict:
    norm_positions = _normalize_positions(positions)

    if not norm_positions:
        return {
            "points": [],
            "current_value": 0.0,
            "total_return": 0.0,
            "best_symbol": None,
            "worst_symbol": None,
        }

    total_invested = sum(p["invested_value"] for p in norm_positions)

    symbol_series = {}
    symbol_returns = {}

    for position in norm_positions:
        symbol = position["symbol"]
        series = _history_to_close_series(symbol, range_="6mo")

        if series is None or len(series) < 2:
            continue

        symbol_series[symbol] = series
        start_price = float(series.iloc[0])
        end_price = float(series.iloc[-1])

        if start_price > 0:
            symbol_returns[symbol] = ((end_price / start_price) - 1.0) * 100
        else:
            symbol_returns[symbol] = 0.0

    if not symbol_series:
        return {
            "points": [],
            "current_value": 0.0,
            "total_return": 0.0,
            "best_symbol": None,
            "worst_symbol": None,
        }

    value_frames = []

    for position in norm_positions:
        symbol = position["symbol"]
        shares = position["shares"]

        series = symbol_series.get(symbol)
        if series is None or series.empty:
            continue

        holding_value_series = series * shares
        value_frames.append(holding_value_series.rename(symbol))

    if not value_frames:
        return {
            "points": [],
            "current_value": 0.0,
            "total_return": 0.0,
            "best_symbol": None,
            "worst_symbol": None,
        }

    portfolio_df = pd.concat(value_frames, axis=1).sort_index()
    portfolio_df = portfolio_df.ffill().dropna(how="all")

    if portfolio_df.empty:
        return {
            "points": [],
            "current_value": 0.0,
            "total_return": 0.0,
            "best_symbol": None,
            "worst_symbol": None,
        }

    portfolio_value = portfolio_df.sum(axis=1)
    portfolio_return = (
        ((portfolio_value / total_invested) - 1.0) * 100.0 if total_invested > 0 else 0.0
    )

    nifty_series = _build_benchmark_series("^NSEI", range_="6mo")
    sensex_series = _build_benchmark_series("^BSESN", range_="6mo")

    benchmark_df = pd.DataFrame(index=portfolio_value.index)

    if nifty_series is not None and not nifty_series.empty:
        nifty_series = nifty_series.reindex(benchmark_df.index).ffill()
        benchmark_df["nifty_return"] = ((nifty_series / nifty_series.iloc[0]) - 1.0) * 100.0

    if sensex_series is not None and not sensex_series.empty:
        sensex_series = sensex_series.reindex(benchmark_df.index).ffill()
        benchmark_df["sensex_return"] = ((sensex_series / sensex_series.iloc[0]) - 1.0) * 100.0

    points = []
    for dt_index in portfolio_value.index:
        current_portfolio_value = float(portfolio_value.loc[dt_index])
        gain_loss = current_portfolio_value - total_invested

        current_portfolio_return = (
            ((current_portfolio_value / total_invested) - 1.0) * 100.0
            if total_invested > 0
            else 0.0
        )

        points.append(
            {
                "date": pd.Timestamp(dt_index).date().isoformat(),
                "portfolio_value": round(current_portfolio_value, 2),
                "invested_value": round(total_invested, 2),
                "gain_loss": round(gain_loss, 2),
                "portfolio_return": round(current_portfolio_return, 2),
                "nifty_return": round(float(benchmark_df.loc[dt_index, "nifty_return"]), 2)
                if "nifty_return" in benchmark_df.columns and pd.notna(benchmark_df.loc[dt_index, "nifty_return"])
                else None,
                "sensex_return": round(float(benchmark_df.loc[dt_index, "sensex_return"]), 2)
                if "sensex_return" in benchmark_df.columns and pd.notna(benchmark_df.loc[dt_index, "sensex_return"])
                else None,
            }
        )

    best_symbol = max(symbol_returns, key=symbol_returns.get) if symbol_returns else None
    worst_symbol = min(symbol_returns, key=symbol_returns.get) if symbol_returns else None

    return {
        "points": points,
        "current_value": round(float(portfolio_value.iloc[-1]), 2),
        "total_return": round(float(portfolio_return.iloc[-1]), 2) if total_invested > 0 else 0.0,
        "best_symbol": best_symbol,
        "worst_symbol": worst_symbol,
    }