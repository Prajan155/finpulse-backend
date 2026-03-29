from __future__ import annotations

from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from app.services.yahoo_service import get_history
from app.core.cache import ttl_cache


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

    for item in cleaned:
        item["weight"] = (
            item["invested_value"] / total_value if total_value > 0 else 0.0
        )

    return cleaned


def _history_to_series(symbol: str) -> tuple[str, pd.Series | None]:
    try:
        data = get_history(symbol, interval="1d", range_="6mo")
        candles = data.get("candles", [])

        if not candles:
            return symbol, None

        df = pd.DataFrame(candles)
        if df.empty:
            return symbol, None

        df["t"] = pd.to_datetime(df["t"])
        df = df.sort_values("t")

        series = pd.Series(df["c"].values, index=df["t"]).dropna()
        return symbol, series if len(series) > 1 else None

    except Exception:
        return symbol, None


@ttl_cache(prefix="portfolio_performance", ttl_seconds=120)
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

    symbols = [p["symbol"] for p in norm_positions]
    all_symbols = symbols + ["^NSEI", "^BSESN"]

    # 🚀 PARALLEL FETCH
    series_map = {}

    with ThreadPoolExecutor(max_workers=min(8, len(all_symbols))) as executor:
        futures = {
            executor.submit(_history_to_series, symbol): symbol
            for symbol in all_symbols
        }

        for future in as_completed(futures):
            symbol, series = future.result()
            if series is not None:
                series_map[symbol] = series

    if not series_map:
        return {
            "points": [],
            "current_value": 0.0,
            "total_return": 0.0,
            "best_symbol": None,
            "worst_symbol": None,
        }

    # 📊 Build portfolio
    value_frames = []
    symbol_returns = {}

    for position in norm_positions:
        symbol = position["symbol"]
        shares = position["shares"]

        series = series_map.get(symbol)
        if series is None:
            continue

        value_series = series * shares
        value_frames.append(value_series.rename(symbol))

        start = series.iloc[0]
        end = series.iloc[-1]
        if start > 0:
            symbol_returns[symbol] = ((end / start) - 1.0) * 100

    if not value_frames:
        return {
            "points": [],
            "current_value": 0.0,
            "total_return": 0.0,
            "best_symbol": None,
            "worst_symbol": None,
        }

    portfolio_df = pd.concat(value_frames, axis=1).ffill().dropna()
    portfolio_value = portfolio_df.sum(axis=1)

    # 📈 Benchmarks
    benchmark_df = pd.DataFrame(index=portfolio_value.index)

    if "^NSEI" in series_map:
        nifty = series_map["^NSEI"].reindex(benchmark_df.index).ffill()
        benchmark_df["nifty_return"] = ((nifty / nifty.iloc[0]) - 1.0) * 100

    if "^BSESN" in series_map:
        sensex = series_map["^BSESN"].reindex(benchmark_df.index).ffill()
        benchmark_df["sensex_return"] = ((sensex / sensex.iloc[0]) - 1.0) * 100

    # 📊 Points
    points = []

    for dt_index in portfolio_value.index:
        val = float(portfolio_value.loc[dt_index])
        gain = val - total_invested

        ret = ((val / total_invested) - 1.0) * 100 if total_invested > 0 else 0.0

        points.append(
            {
                "date": dt_index.date().isoformat(),
                "portfolio_value": round(val, 2),
                "invested_value": round(total_invested, 2),
                "gain_loss": round(gain, 2),
                "portfolio_return": round(ret, 2),
                "nifty_return": round(float(benchmark_df.loc[dt_index, "nifty_return"]), 2)
                if "nifty_return" in benchmark_df else None,
                "sensex_return": round(float(benchmark_df.loc[dt_index, "sensex_return"]), 2)
                if "sensex_return" in benchmark_df else None,
            }
        )

    best_symbol = max(symbol_returns, key=symbol_returns.get) if symbol_returns else None
    worst_symbol = min(symbol_returns, key=symbol_returns.get) if symbol_returns else None

    return {
        "points": points,
        "current_value": round(float(portfolio_value.iloc[-1]), 2),
        "total_return": round(points[-1]["portfolio_return"], 2) if points else 0.0,
        "best_symbol": best_symbol,
        "worst_symbol": worst_symbol,
    }