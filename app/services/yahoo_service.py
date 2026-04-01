from __future__ import annotations

import datetime as dt
import logging
import os
import time

import requests
from cachetools import TTLCache
import yfinance as yf
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA

from app.core.cache import ttl_cache


logger = logging.getLogger(__name__)


def _iso(dt_like) -> str:
    if hasattr(dt_like, "to_pydatetime"):
        dt_like = dt_like.to_pydatetime()
    if isinstance(dt_like, dt.datetime) and dt_like.tzinfo is None:
        dt_like = dt_like.replace(tzinfo=dt.timezone.utc)
    return dt_like.isoformat()


def _safe_float(value, default=None):
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _safe_float_or_none(value):
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _safe_div(numerator, denominator, default=None):
    try:
        n = _safe_float_or_none(numerator)
        d = _safe_float_or_none(denominator)
        if n is None or d is None or d == 0:
            return default
        return n / d
    except Exception:
        return default


def _round_or_none(value, digits: int = 2):
    val = _safe_float_or_none(value)
    return round(val, digits) if val is not None else None


def _normalize_download_df(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df

    if isinstance(df.columns, pd.MultiIndex):
        try:
            if len(df.columns.levels) > 1:
                df = df.copy()
                df.columns = df.columns.get_level_values(0)
        except Exception:
            pass

    return df


def _default_market_currency(ticker: str) -> tuple[str, str]:
    ticker = (ticker or "").upper().strip()
    if ticker.endswith(".NS") or ticker.endswith(".BO"):
        return "India", "INR"
    return "US", "USD"


def _is_valid_price(value) -> bool:
    val = _safe_float_or_none(value)
    return val is not None and val > 0


def _safe_nonempty_str(*values):
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _extract_fast_value(fast, *keys):
    if not fast:
        return None
    getter = getattr(fast, "get", None)
    if callable(getter):
        for key in keys:
            try:
                value = getter(key)
            except Exception:
                value = None
            if value is not None:
                return value
    for key in keys:
        try:
            value = fast[key]
        except Exception:
            value = getattr(fast, key, None)
        if value is not None:
            return value
    return None


def _empty_quote(ticker: str) -> dict:
    market, currency = _default_market_currency(ticker)
    return {
        "symbol": ticker,
        "name": ticker,
        "exchange": None,
        "market": market,
        "currency": currency,
        "price": None,
        "change": None,
        "changePercent": None,
        "marketState": None,
        "asOf": _now_iso(),
    }


def _empty_profile(symbol: str) -> dict:
    _, currency = _default_market_currency(symbol)
    return {
        "symbol": symbol,
        "sector": "",
        "industry": "",
        "market_cap": None,
        "currency": currency,
        "summary": "",
    }


def _empty_ratios(symbol: str) -> dict:
    return {
        "symbol": symbol,
        "market_cap": None,
        "pe_ttm": None,
        "pb": None,
        "ps_ttm": None,
        "dividend_yield": None,
        "beta": None,
        "profit_margin": None,
        "operating_margin": None,
        "roe": None,
        "roa": None,
        "debt_to_equity": None,
        "current_ratio": None,
        "quick_ratio": None,
    }


def _fetch_history_df(
    ticker: str,
    period: str,
    interval: str,
    *,
    for_label: str = "history",
) -> pd.DataFrame | None:
    df = None

    if _finnhub_enabled():
        try:
            df = _finnhub_fetch_candles(ticker, period, interval)
        except Exception:
            logger.exception(
                "Finnhub %s fetch failed for ticker=%s interval=%s period=%s",
                for_label,
                ticker,
                interval,
                period,
            )

    if df is not None and not df.empty:
        return _normalize_download_df(df)

    t = yf.Ticker(ticker)

    try:
        df = t.history(
            period=period,
            interval=interval,
            auto_adjust=False,
            actions=False,
        )
    except Exception:
        logger.exception(
            "yfinance primary %s fetch failed for ticker=%s interval=%s period=%s",
            for_label,
            ticker,
            interval,
            period,
        )

    df = _normalize_download_df(df)

    if df is None or df.empty:
        try:
            df = yf.download(
                ticker,
                period=period,
                interval=interval,
                auto_adjust=False,
                progress=False,
                threads=False,
            )
        except Exception:
            logger.exception(
                "yfinance fallback %s download failed for ticker=%s interval=%s period=%s",
                for_label,
                ticker,
                interval,
                period,
            )
            return None

    df = _normalize_download_df(df)
    return df




FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
_FINNHUB_TIMEOUT = 12
_FINNHUB_SESSION = requests.Session()
_finnhub_symbol_cache = TTLCache(maxsize=5000, ttl=86400)


def _get_finnhub_api_key() -> str | None:
    return (
        os.getenv("FINNHUB_API_KEY")
        or os.getenv("FINNHUB_TOKEN")
        or os.getenv("FINNHUB_KEY")
    )


def _finnhub_enabled() -> bool:
    return bool(_get_finnhub_api_key())


def _to_finnhub_symbol(symbol: str) -> str:
    symbol = (symbol or "").upper().strip()
    if not symbol:
        return symbol
    if ":" in symbol:
        return symbol
    if symbol.endswith(".NS"):
        return f"NSE:{symbol[:-3]}"
    if symbol.endswith(".BO"):
        return f"BSE:{symbol[:-3]}"
    return symbol


def _from_finnhub_symbol(symbol: str) -> str:
    symbol = (symbol or "").upper().strip()
    if symbol.startswith("NSE:"):
        return f"{symbol.split(':', 1)[1]}.NS"
    if symbol.startswith("BSE:"):
        return f"{symbol.split(':', 1)[1]}.BO"
    return symbol


def _finnhub_resolution(interval: str) -> str | None:
    interval = (interval or "").lower().strip()
    mapping = {
        "1m": "1",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "60m": "60",
        "90m": "60",
        "1h": "60",
        "1d": "D",
        "1wk": "W",
        "1mo": "M",
    }
    return mapping.get(interval)


def _range_to_timestamps(range_: str) -> tuple[int, int]:
    now = int(time.time())
    value = (range_ or "6mo").strip().lower()

    seconds_map = {
        "1d": 86400,
        "5d": 5 * 86400,
        "7d": 7 * 86400,
        "1mo": 31 * 86400,
        "3mo": 92 * 86400,
        "6mo": 183 * 86400,
        "1y": 366 * 86400,
        "2y": 732 * 86400,
        "5y": 5 * 366 * 86400,
        "10y": 10 * 366 * 86400,
        "ytd": 120 * 86400,
        "max": 20 * 366 * 86400,
    }
    delta = seconds_map.get(value, 183 * 86400)
    return now - delta, now


def _finnhub_request(path: str, params: dict | None = None) -> dict | list | None:
    api_key = _get_finnhub_api_key()
    if not api_key:
        return None

    payload = dict(params or {})
    payload["token"] = api_key

    try:
        resp = _FINNHUB_SESSION.get(
            f"{FINNHUB_BASE_URL}{path}",
            params=payload,
            timeout=_FINNHUB_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.exception("Finnhub request failed for path=%s params=%s", path, params)
        return None


def _infer_market_from_exchange(exchange: str | None, symbol: str) -> str:
    exchange_upper = (exchange or "").upper()
    if symbol.endswith(".NS") or "NSE" in exchange_upper:
        return "India"
    if symbol.endswith(".BO") or "BSE" in exchange_upper:
        return "India"
    return "US"


def _finnhub_fetch_candles(symbol: str, period: str, interval: str) -> pd.DataFrame | None:
    finnhub_symbol = _to_finnhub_symbol(symbol)
    resolution = _finnhub_resolution(interval)
    if not resolution:
        return None

    start_ts, end_ts = _range_to_timestamps(period)
    data = _finnhub_request(
        "/stock/candle",
        {
            "symbol": finnhub_symbol,
            "resolution": resolution,
            "from": start_ts,
            "to": end_ts,
        },
    )

    if not isinstance(data, dict) or data.get("s") != "ok":
        return None

    timestamps = data.get("t") or []
    if not timestamps:
        return None

    df = pd.DataFrame(
        {
            "Open": data.get("o") or [],
            "High": data.get("h") or [],
            "Low": data.get("l") or [],
            "Close": data.get("c") or [],
            "Volume": data.get("v") or [],
        },
        index=pd.to_datetime(timestamps, unit="s", utc=True),
    )
    return df if not df.empty else None


def _finnhub_search_symbols(q: str, market: str = "ALL", limit: int = 10) -> list[dict]:
    data = _finnhub_request("/search", {"q": q})
    if not isinstance(data, dict):
        return []

    market = (market or "ALL").upper().strip()
    out = []

    for item in data.get("result") or []:
        raw_symbol = (item.get("symbol") or "").upper().strip()
        description = item.get("description") or ""
        display_symbol = (item.get("displaySymbol") or raw_symbol).upper().strip()

        if not raw_symbol:
            continue

        app_symbol = _from_finnhub_symbol(raw_symbol)
        exchange = raw_symbol.split(":", 1)[0] if ":" in raw_symbol else item.get("mic") or "Unknown"

        if market == "NSE" and not app_symbol.endswith(".NS"):
            continue
        if market == "BSE" and not app_symbol.endswith(".BO"):
            continue
        if market == "US" and (app_symbol.endswith(".NS") or app_symbol.endswith(".BO")):
            continue

        _, currency = _default_market_currency(app_symbol)
        out.append(
            {
                "symbol": app_symbol,
                "name": description or display_symbol or app_symbol,
                "exchange": exchange,
                "market": exchange if market == "ALL" else market,
                "currency": currency,
            }
        )
        if len(out) >= limit:
            break

    return out


def _finnhub_quote(symbol: str) -> dict | None:
    finnhub_symbol = _to_finnhub_symbol(symbol)
    data = _finnhub_request("/quote", {"symbol": finnhub_symbol})
    if not isinstance(data, dict):
        return None

    current = _safe_float_or_none(data.get("c"))
    previous_close = _safe_float_or_none(data.get("pc"))
    if current is None:
        return None

    change = _safe_float_or_none(data.get("d"))
    change_percent = _safe_float_or_none(data.get("dp"))
    if change is None and previous_close is not None:
        change = current - previous_close
    if change_percent is None and previous_close not in (None, 0):
        change_percent = ((current - previous_close) / previous_close) * 100

    return {
        "price": current,
        "change": change,
        "changePercent": change_percent,
        "prevClose": previous_close,
        "high": _safe_float_or_none(data.get("h")),
        "low": _safe_float_or_none(data.get("l")),
        "open": _safe_float_or_none(data.get("o")),
        "timestamp": data.get("t"),
    }


def _finnhub_company_profile(symbol: str) -> dict | None:
    finnhub_symbol = _to_finnhub_symbol(symbol)
    profile = _finnhub_request("/stock/profile2", {"symbol": finnhub_symbol})
    if not isinstance(profile, dict) or not profile:
        return None

    return {
        "symbol": symbol,
        "name": profile.get("name") or symbol,
        "exchange": profile.get("exchange"),
        "currency": profile.get("currency"),
        "country": profile.get("country"),
        "industry": profile.get("finnhubIndustry") or "",
        "ipo": profile.get("ipo"),
        "market_cap": _safe_float_or_none(profile.get("marketCapitalization")),
        "weburl": profile.get("weburl"),
    }


def _finnhub_basic_financials(symbol: str) -> dict | None:
    finnhub_symbol = _to_finnhub_symbol(symbol)
    data = _finnhub_request("/stock/metric", {"symbol": finnhub_symbol, "metric": "all"})
    if not isinstance(data, dict):
        return None
    metric = data.get("metric")
    if not isinstance(metric, dict) or not metric:
        return None
    return metric

def _prepare_series_for_arima(series: pd.Series, freq: str) -> pd.Series:
    s = series.dropna().copy()
    s.index = pd.DatetimeIndex(s.index)
    s = s.sort_index()
    s = s.asfreq(freq)
    s = s.ffill().dropna()
    return s


@ttl_cache(prefix="symbol_search", ttl_seconds=120)
def search_symbols(q: str, market: str = "ALL", limit: int = 10) -> dict:
    q = (q or "").strip()
    market = (market or "ALL").upper().strip()

    if not q:
        return {"query": q, "results": []}

    if _finnhub_enabled():
        try:
            results = _finnhub_search_symbols(q, market=market, limit=limit)
            if results:
                return {"query": q, "results": results}
        except Exception:
            logger.exception("Finnhub symbol search failed for query=%s market=%s", q, market)

    results = []

    try:
        search = yf.Search(q)
        quotes = getattr(search, "quotes", None) or []

        for item in quotes[:50]:
            symbol = item.get("symbol")
            name = item.get("shortname") or item.get("longname") or item.get("name") or ""
            exchange = item.get("exchange") or item.get("exchDisp") or item.get("exchangeDisp") or "Unknown"

            if not symbol:
                continue

            if market == "NSE" and not symbol.endswith(".NS"):
                continue
            if market == "BSE" and not symbol.endswith(".BO"):
                continue
            if market == "US" and (symbol.endswith(".NS") or symbol.endswith(".BO")):
                continue

            market_value = exchange if market == "ALL" else market

            currency = item.get("currency")
            if not currency:
                currency = "INR" if (symbol.endswith(".NS") or symbol.endswith(".BO")) else "USD"

            results.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "exchange": exchange,
                    "market": market_value,
                    "currency": currency,
                }
            )

            if len(results) >= limit:
                break

    except Exception:
        logger.exception("Failed symbol search for query=%s market=%s", q, market)
        return {"query": q, "results": []}

    return {"query": q, "results": results}


@ttl_cache(prefix="history", ttl_seconds=600)
def get_history(ticker: str, interval: str = "1d", range_: str = "6mo") -> dict:
    ticker = ticker.strip().upper()

    try:
        df = _fetch_history_df(ticker, range_, interval, for_label="history")

        if df is None or df.empty or "Close" not in df.columns:
            return {"symbol": ticker, "interval": interval, "range": range_, "candles": []}

        candles = []
        for ts, row in df.iterrows():
            candles.append(
                {
                    "t": _iso(ts),
                    "o": _safe_float(row.get("Open")),
                    "h": _safe_float(row.get("High")),
                    "l": _safe_float(row.get("Low")),
                    "c": _safe_float(row.get("Close")),
                    "v": _safe_float(row.get("Volume")),
                }
            )

        return {"symbol": ticker, "interval": interval, "range": range_, "candles": candles}

    except Exception:
        logger.exception("Failed history fetch for ticker=%s interval=%s range=%s", ticker, interval, range_)
        return {"symbol": ticker, "interval": interval, "range": range_, "candles": []}


@ttl_cache(prefix="ratios", ttl_seconds=1800)
def get_ratios(ticker: str) -> dict:
    ticker = ticker.strip().upper()
    default = _empty_ratios(ticker)

    def _to_percent(value):
        val = _safe_float_or_none(value)
        if val is None:
            return None
        if -1 <= val <= 1:
            val = val * 100
        return val

    finnhub_profile = None
    finnhub_metric = None

    if _finnhub_enabled():
        try:
            finnhub_profile = _finnhub_company_profile(ticker)
            finnhub_metric = _finnhub_basic_financials(ticker)
            if finnhub_metric:
                market_cap = None
                if finnhub_profile:
                    market_cap = _safe_float_or_none(finnhub_profile.get("market_cap"))
                    if market_cap is not None and market_cap < 1_000_000:
                        market_cap *= 1_000_000

                out = {
                    "symbol": ticker,
                    "market_cap": _round_or_none(market_cap, 4),
                    "pe_ttm": _round_or_none(finnhub_metric.get("peTTM"), 4),
                    "pb": _round_or_none(
                        finnhub_metric.get("pbQuarterly") or finnhub_metric.get("pbAnnual"),
                        4,
                    ),
                    "ps_ttm": _round_or_none(finnhub_metric.get("psTTM"), 4),
                    "dividend_yield": _round_or_none(
                        _to_percent(
                            finnhub_metric.get("dividendYieldIndicatedAnnual")
                            or finnhub_metric.get("currentDividendYieldTTM")
                        ),
                        4,
                    ),
                    "beta": _round_or_none(finnhub_metric.get("beta"), 4),
                    "profit_margin": _round_or_none(
                        _to_percent(
                            finnhub_metric.get("netMarginTTM")
                            or finnhub_metric.get("netProfitMarginAnnual")
                        ),
                        4,
                    ),
                    "operating_margin": _round_or_none(
                        _to_percent(
                            finnhub_metric.get("operatingMarginTTM")
                            or finnhub_metric.get("operatingMarginAnnual")
                        ),
                        4,
                    ),
                    "roe": _round_or_none(
                        _to_percent(
                            finnhub_metric.get("roeTTM")
                            or finnhub_metric.get("roeAnnual")
                        ),
                        4,
                    ),
                    "roa": _round_or_none(
                        _to_percent(
                            finnhub_metric.get("roaTTM")
                            or finnhub_metric.get("roaAnnual")
                        ),
                        4,
                    ),
                    "debt_to_equity": _round_or_none(
                        finnhub_metric.get("totalDebt/totalEquityQuarterly")
                        or finnhub_metric.get("totalDebt/totalEquityAnnual")
                        or finnhub_metric.get("totalDebtToEquityQuarterly")
                        or finnhub_metric.get("totalDebtToEquityAnnual"),
                        4,
                    ),
                    "current_ratio": _round_or_none(
                        finnhub_metric.get("currentRatioQuarterly")
                        or finnhub_metric.get("currentRatioAnnual"),
                        4,
                    ),
                    "quick_ratio": _round_or_none(
                        finnhub_metric.get("quickRatioQuarterly")
                        or finnhub_metric.get("quickRatioAnnual"),
                        4,
                    ),
                }
                if any(v is not None for k, v in out.items() if k != "symbol"):
                    return out
        except Exception:
            logger.exception("Finnhub ratios fetch failed for ticker=%s", ticker)

    t = yf.Ticker(ticker)

    info = {}
    fast = {}

    try:
        fast = t.fast_info or {}
    except Exception:
        logger.warning("fast_info fetch failed for %s", ticker)

    try:
        info = t.info or {}
    except Exception:
        logger.warning("info fetch failed for %s", ticker)

    market_cap = _safe_float_or_none(info.get("marketCap") or fast.get("market_cap"))
    pe_ttm = _safe_float_or_none(info.get("trailingPE") or info.get("forwardPE"))
    pb = _safe_float_or_none(info.get("priceToBook"))
    ps_ttm = _safe_float_or_none(info.get("priceToSalesTrailing12Months"))
    dividend_yield = _to_percent(info.get("dividendYield"))
    beta = _safe_float_or_none(info.get("beta"))

    profit_margin = _safe_float_or_none(info.get("profitMargins"))
    operating_margin = _safe_float_or_none(info.get("operatingMargins"))
    roe = _safe_float_or_none(info.get("returnOnEquity"))
    roa = _safe_float_or_none(info.get("returnOnAssets"))
    debt_to_equity = _safe_float_or_none(info.get("debtToEquity"))
    current_ratio = _safe_float_or_none(info.get("currentRatio"))
    quick_ratio = _safe_float_or_none(info.get("quickRatio"))

    equity = None
    total_assets = None

    try:
        bs = t.balance_sheet
        fin = t.financials

        if bs is not None and not bs.empty:
            col = bs.columns[0]

            current_assets = bs.loc["Current Assets", col] if "Current Assets" in bs.index else None
            current_liabilities = bs.loc["Current Liabilities", col] if "Current Liabilities" in bs.index else None
            inventory = bs.loc["Inventory", col] if "Inventory" in bs.index else None
            total_debt = bs.loc["Total Debt", col] if "Total Debt" in bs.index else None
            equity = bs.loc["Stockholders Equity", col] if "Stockholders Equity" in bs.index else None
            total_assets = bs.loc["Total Assets", col] if "Total Assets" in bs.index else None

            if current_ratio is None:
                current_ratio = _safe_div(current_assets, current_liabilities)

            if quick_ratio is None:
                if inventory is not None:
                    quick_ratio = _safe_div(
                        _safe_float_or_none(current_assets) - _safe_float(inventory, 0.0),
                        current_liabilities,
                    )
                else:
                    quick_ratio = _safe_div(current_assets, current_liabilities)

            if debt_to_equity is None:
                debt_to_equity = _safe_div(total_debt, equity)

        if fin is not None and not fin.empty:
            col = fin.columns[0]

            revenue = fin.loc["Total Revenue", col] if "Total Revenue" in fin.index else None
            net_income = fin.loc["Net Income", col] if "Net Income" in fin.index else None
            operating_income = fin.loc["Operating Income", col] if "Operating Income" in fin.index else None

            if profit_margin is None:
                profit_margin = _safe_div(net_income, revenue)

            if operating_margin is None:
                operating_margin = _safe_div(operating_income, revenue)

            if roe is None:
                roe = _safe_div(net_income, equity)

            if roa is None:
                roa = _safe_div(net_income, total_assets)

    except Exception:
        logger.exception("Failed to compute fallback ratios for ticker=%s", ticker)

    profit_margin = _to_percent(profit_margin)
    operating_margin = _to_percent(operating_margin)
    roe = _to_percent(roe)
    roa = _to_percent(roa)

    return {
        "symbol": ticker,
        "market_cap": _round_or_none(market_cap, 4),
        "pe_ttm": _round_or_none(pe_ttm, 4),
        "pb": _round_or_none(pb, 4),
        "ps_ttm": _round_or_none(ps_ttm, 4),
        "dividend_yield": _round_or_none(dividend_yield, 4),
        "beta": _round_or_none(beta, 4),
        "profit_margin": _round_or_none(profit_margin, 4),
        "operating_margin": _round_or_none(operating_margin, 4),
        "roe": _round_or_none(roe, 4),
        "roa": _round_or_none(roa, 4),
        "debt_to_equity": _round_or_none(debt_to_equity, 4),
        "current_ratio": _round_or_none(current_ratio, 4),
        "quick_ratio": _round_or_none(quick_ratio, 4),
    }


@ttl_cache(prefix="forecast_intraday", ttl_seconds=1200)
def get_intraday_forecast_arima(
    ticker: str,
    horizon_days: int = 3,
    interval: str = "15m",
    history_range: str | None = None,
) -> dict:
    ticker = ticker.strip().upper()
    interval = interval.strip().lower()

    if interval not in {"5m", "15m"}:
        return {
            "symbol": ticker,
            "horizon_days": horizon_days,
            "interval": interval,
            "last_close": None,
            "forecast_close": None,
            "points": [],
            "model": "ARIMA(5,1,0)",
            "error": "Only 5m and 15m intervals are supported",
        }

    if horizon_days <= 0:
        return {
            "symbol": ticker,
            "horizon_days": horizon_days,
            "interval": interval,
            "last_close": None,
            "forecast_close": None,
            "points": [],
            "model": "ARIMA(5,1,0)",
            "error": "horizon_days must be positive",
        }

    if history_range is None:
        history_range = "5d" if interval == "5m" else "1mo"

    try:
        df = _fetch_history_df(ticker, history_range, interval, for_label="intraday forecast")

        if df is None or df.empty or "Close" not in df.columns:
            return {
                "symbol": ticker,
                "horizon_days": horizon_days,
                "interval": interval,
                "last_close": None,
                "forecast_close": None,
                "points": [],
                "model": "ARIMA(5,1,0)",
                "error": "No intraday history data available",
            }

        close = df["Close"].dropna()
        freq = "5min" if interval == "5m" else "15min"
        close = _prepare_series_for_arima(close, freq)

        min_points = 60 if interval == "5m" else 40
        if len(close) < min_points:
            return {
                "symbol": ticker,
                "horizon_days": horizon_days,
                "interval": interval,
                "last_close": _round_or_none(close.iloc[-1], 2) if len(close) else None,
                "forecast_close": None,
                "points": [],
                "model": "ARIMA(5,1,0)",
                "error": f"Not enough intraday data for forecasting (need ~{min_points}+ points)",
            }

        std_val = _safe_float_or_none(close.std())
        if std_val is None or std_val == 0.0:
            return {
                "symbol": ticker,
                "horizon_days": horizon_days,
                "interval": interval,
                "last_close": _round_or_none(close.iloc[-1], 2),
                "forecast_close": None,
                "points": [],
                "model": "ARIMA(5,1,0)",
                "error": "Not enough price variation for forecasting",
            }

        bars_per_day = 75 if interval == "5m" else 25
        horizon_steps = horizon_days * bars_per_day
        last_close = _safe_float_or_none(close.iloc[-1])

        model = ARIMA(close, order=(5, 1, 0))
        fit = model.fit()

        fc = fit.forecast(steps=horizon_steps)
        fc = np.array(fc, dtype=float)

        last_ts = pd.Timestamp(close.index[-1])
        step_minutes = 5 if interval == "5m" else 15
        step = pd.Timedelta(minutes=step_minutes)

        points = []
        current_ts = last_ts

        while len(points) < horizon_steps:
            current_ts = current_ts + step

            if current_ts.weekday() >= 5:
                continue

            session_start = dt.time(9, 15)
            session_end = dt.time(15, 30)

            if current_ts.time() < session_start:
                current_ts = current_ts.replace(hour=9, minute=15, second=0, microsecond=0)

            if current_ts.time() > session_end:
                next_day = current_ts + pd.Timedelta(days=1)
                current_ts = next_day.replace(hour=9, minute=15, second=0, microsecond=0)
                continue

            idx = len(points)
            points.append(
                {
                    "t": _iso(current_ts),
                    "y": _round_or_none(fc[idx], 2),
                }
            )

        return {
            "symbol": ticker,
            "horizon_days": horizon_days,
            "interval": interval,
            "last_close": _round_or_none(last_close, 2),
            "forecast_close": _round_or_none(fc[-1], 2) if len(fc) else None,
            "points": points,
            "model": "ARIMA(5,1,0)",
            "error": None,
        }

    except Exception as e:
        logger.exception(
            "Failed intraday forecast for ticker=%s interval=%s horizon_days=%s",
            ticker,
            interval,
            horizon_days,
        )
        return {
            "symbol": ticker,
            "horizon_days": horizon_days,
            "interval": interval,
            "last_close": None,
            "forecast_close": None,
            "points": [],
            "model": "ARIMA(5,1,0)",
            "error": str(e),
        }


@ttl_cache(prefix="forecast_long", ttl_seconds=1800)
def get_long_range_forecast_arima(
    ticker: str,
    horizon_days: int = 30,
    interval: str = "1d",
    history_range: str = "1y",
) -> dict:
    ticker = ticker.strip().upper()

    if horizon_days <= 0:
        return {
            "symbol": ticker,
            "horizon_days": horizon_days,
            "interval": interval,
            "last_close": None,
            "forecast_close": None,
            "points": [],
            "model": "ARIMA(5,1,0)",
            "error": "horizon_days must be positive",
        }

    try:
        df = _fetch_history_df(ticker, history_range, interval, for_label="long-range forecast")

        if df is None or df.empty or "Close" not in df.columns:
            return {
                "symbol": ticker,
                "horizon_days": horizon_days,
                "interval": interval,
                "last_close": None,
                "forecast_close": None,
                "points": [],
                "model": "ARIMA(5,1,0)",
                "error": "No history data available",
            }

        close = df["Close"].dropna()
        close = _prepare_series_for_arima(close, "B")

        if len(close) < 60:
            return {
                "symbol": ticker,
                "horizon_days": horizon_days,
                "interval": interval,
                "last_close": _round_or_none(close.iloc[-1], 2) if len(close) else None,
                "forecast_close": None,
                "points": [],
                "model": "ARIMA(5,1,0)",
                "error": "Not enough data for forecasting (need ~60+ daily points)",
            }

        std_val = _safe_float_or_none(close.std())
        if std_val is None or std_val == 0.0:
            return {
                "symbol": ticker,
                "horizon_days": horizon_days,
                "interval": interval,
                "last_close": _round_or_none(close.iloc[-1], 2),
                "forecast_close": None,
                "points": [],
                "model": "ARIMA(5,1,0)",
                "error": "Not enough price variation for forecasting",
            }

        last_close = _safe_float_or_none(close.iloc[-1])

        model = ARIMA(close, order=(5, 1, 0))
        fit = model.fit()

        fc = fit.forecast(steps=horizon_days)
        fc = np.array(fc, dtype=float)

        last_date = close.index[-1]
        start = pd.Timestamp(last_date).normalize() + pd.Timedelta(days=1)
        future_dates = pd.bdate_range(start=start, periods=horizon_days)

        points = [{"t": _iso(ts), "y": _round_or_none(val, 2)} for ts, val in zip(future_dates, fc)]

        return {
            "symbol": ticker,
            "horizon_days": horizon_days,
            "interval": interval,
            "last_close": _round_or_none(last_close, 2),
            "forecast_close": _round_or_none(fc[-1], 2) if len(fc) else None,
            "points": points,
            "model": "ARIMA(5,1,0)",
            "error": None,
        }

    except Exception as e:
        logger.exception(
            "Failed long-range forecast for ticker=%s interval=%s horizon_days=%s",
            ticker,
            interval,
            horizon_days,
        )
        return {
            "symbol": ticker,
            "horizon_days": horizon_days,
            "interval": interval,
            "last_close": None,
            "forecast_close": None,
            "points": [],
            "model": "ARIMA(5,1,0)",
            "error": str(e),
        }


@ttl_cache(prefix="correlation", ttl_seconds=600)
def get_correlation_matrix(tickers: list[str], range_: str = "6mo") -> dict:
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    tickers = list(dict.fromkeys(tickers))

    if len(tickers) < 2:
        return {"tickers": tickers, "range": range_, "matrix": []}

    try:
        closes = {}
        for tk in tickers:
            try:
                df = _fetch_history_df(tk, range_, "1d", for_label="correlation")
                if df is None or df.empty or "Close" not in df.columns:
                    closes[tk] = None
                else:
                    s = df["Close"].dropna()
                    closes[tk] = s if len(s) else None
            except Exception:
                logger.warning("Failed correlation history fetch for ticker=%s", tk)
                closes[tk] = None

        series_list = []
        valid_tickers = []
        for tk in tickers:
            s = closes.get(tk)
            if s is not None:
                series_list.append(s.rename(tk))
                valid_tickers.append(tk)

        if len(valid_tickers) < 2:
            fallback = [[0.0 for _ in tickers] for _ in tickers]
            for i in range(len(tickers)):
                fallback[i][i] = 1.0
            return {"tickers": tickers, "range": range_, "matrix": fallback}

        prices_df = pd.concat(series_list, axis=1)
        prices_df = prices_df.ffill().dropna()

        if prices_df.empty or len(prices_df) < 5:
            fallback = [[0.0 for _ in tickers] for _ in tickers]
            for i in range(len(tickers)):
                fallback[i][i] = 1.0
            return {"tickers": tickers, "range": range_, "matrix": fallback}

        rets = prices_df.pct_change().dropna()
        corr = rets.corr().fillna(0.0)

        matrix = []
        for i, tk_i in enumerate(tickers):
            row = []
            for j, tk_j in enumerate(tickers):
                if tk_i in corr.index and tk_j in corr.columns:
                    row.append(round(float(corr.loc[tk_i, tk_j]), 2))
                else:
                    row.append(1.0 if i == j else 0.0)
            matrix.append(row)

        return {"tickers": tickers, "range": range_, "matrix": matrix}

    except Exception:
        logger.exception("Failed correlation matrix for tickers=%s range=%s", tickers, range_)
        fallback = [[0.0 for _ in tickers] for _ in tickers]
        for i in range(len(tickers)):
            fallback[i][i] = 1.0
        return {"tickers": tickers, "range": range_, "matrix": fallback}


def _pick_row(df: pd.DataFrame, candidates: list[str]) -> pd.Series | None:
    if df is None or df.empty:
        return None
    for name in candidates:
        if name in df.index:
            return df.loc[name]
    return None


@ttl_cache(prefix="revenue_expenses", ttl_seconds=3600)
def get_revenue_expenses(ticker: str, frequency: str = "quarterly") -> dict:
    ticker = ticker.strip().upper()

    try:
        t = yf.Ticker(ticker)
        info = {}
        try:
            info = t.info or {}
        except Exception:
            logger.warning("Failed info fetch for revenue/expenses ticker=%s", ticker)

        currency = info.get("currency") or _default_market_currency(ticker)[1]
        fin = t.quarterly_financials if frequency == "quarterly" else t.financials

        if fin is None or fin.empty:
            return {
                "symbol": ticker,
                "frequency": frequency,
                "points": [],
                "currency": currency,
                "error": "No income statement data available",
            }

        revenue_row = _pick_row(fin, ["Total Revenue", "TotalRevenue", "Revenue"])
        expenses_row = _pick_row(
            fin,
            ["Total Operating Expenses", "TotalOperatingExpenses", "Total Expenses", "TotalExpenses"],
        )

        points = []
        for col in fin.columns:
            period = col.date().isoformat() if hasattr(col, "date") else str(col)

            revenue = None
            expenses = None

            if revenue_row is not None and col in revenue_row.index:
                revenue = _safe_float_or_none(revenue_row[col])

            if expenses_row is not None and col in expenses_row.index:
                expenses = _safe_float_or_none(expenses_row[col])

            points.append({"period": period, "revenue": revenue, "expenses": expenses})

        points.sort(key=lambda x: x["period"])

        return {
            "symbol": ticker,
            "frequency": frequency,
            "points": points,
            "currency": currency,
            "error": None,
        }

    except Exception as e:
        logger.exception("Failed revenue/expenses fetch for ticker=%s frequency=%s", ticker, frequency)
        return {
            "symbol": ticker,
            "frequency": frequency,
            "points": [],
            "currency": _default_market_currency(ticker)[1],
            "error": str(e),
        }


_quote_cache = TTLCache(maxsize=5000, ttl=300)
_profile_cache = TTLCache(maxsize=2000, ttl=3600)


def _now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def get_quote(ticker: str) -> dict:
    ticker = ticker.strip().upper()

    cached = _quote_cache.get(ticker)
    if cached:
        return cached

    market_default, currency_default = _default_market_currency(ticker)

    try:
        profile = None

        price = None
        prev_close = None
        change = None
        change_pct = None
        currency = currency_default
        name = ticker
        exchange = None
        market = market_default
        market_state = None

        t = None
        fast = {}

        try:
            t = yf.Ticker(ticker)
        except Exception:
            logger.info("Ticker init failed for ticker=%s", ticker)

        if t is not None:
            try:
                fast = t.fast_info or {}
            except Exception:
                logger.info("fast_info unavailable for ticker=%s", ticker)

            fast_price = _extract_fast_value(
                fast,
                "lastPrice",
                "last_price",
                "regularMarketPrice",
                "previous_close",
                "previousClose",
            )
            fast_prev_close = _extract_fast_value(fast, "previousClose", "previous_close")

            if _is_valid_price(fast_price):
                price = _safe_float_or_none(fast_price)
                prev_close = _safe_float_or_none(fast_prev_close)

            currency = (
                _safe_nonempty_str(
                    _extract_fast_value(fast, "currency"),
                    currency,
                )
                or currency_default
            )
            exchange = _safe_nonempty_str(
                _extract_fast_value(fast, "exchange"),
                exchange,
            )

        if not _is_valid_price(price):
            try:
                hist = _fetch_history_df(ticker, "5d", "1d", for_label="quote daily fallback")

                if hist is None or hist.empty or "Close" not in hist.columns:
                    hist = _fetch_history_df(ticker, "1mo", "1d", for_label="quote extended fallback")

                if hist is not None and not hist.empty and "Close" in hist.columns:
                    closes = hist["Close"].dropna()

                    if len(closes) >= 2:
                        price = _safe_float_or_none(closes.iloc[-1])
                        prev_close = _safe_float_or_none(closes.iloc[-2])
                    elif len(closes) == 1:
                        price = _safe_float_or_none(closes.iloc[-1])
                        prev_close = _safe_float_or_none(closes.iloc[-1])
            except Exception:
                logger.exception("History-based quote fetch failed for ticker=%s", ticker)

        if not _is_valid_price(price) and _finnhub_enabled():
            try:
                finnhub_q = _finnhub_quote(ticker)
                profile = _finnhub_company_profile(ticker)

                if finnhub_q and _is_valid_price(finnhub_q.get("price")):
                    price = _safe_float_or_none(finnhub_q.get("price"))
                    prev_close = _safe_float_or_none(finnhub_q.get("prevClose"))
                    change = _safe_float_or_none(finnhub_q.get("change"))
                    change_pct = _safe_float_or_none(finnhub_q.get("changePercent"))

                if profile:
                    name = profile.get("name") or name
                    exchange = profile.get("exchange") or exchange
                    currency = profile.get("currency") or currency
                    market = _infer_market_from_exchange(exchange, ticker)
                    market_state = "REGULAR"
            except Exception:
                logger.exception("Finnhub quote/profile fetch failed for ticker=%s", ticker)

        if t is not None:
            try:
                info = t.info or {}

                if not _is_valid_price(price):
                    info_price = info.get("regularMarketPrice") or info.get("currentPrice")
                    if _is_valid_price(info_price):
                        price = _safe_float_or_none(info_price)

                if prev_close is None:
                    prev_close = _safe_float_or_none(
                        info.get("regularMarketPreviousClose") or info.get("previousClose")
                    )

                name = (
                    info.get("shortName")
                    or info.get("longName")
                    or info.get("displayName")
                    or info.get("name")
                    or name
                )
                exchange = (
                    exchange
                    or info.get("exchange")
                    or info.get("fullExchangeName")
                    or info.get("quoteExchange")
                )
                market = (
                    info.get("market")
                    or info.get("marketType")
                    or info.get("exchangeTimezoneName")
                    or market
                )
                currency = info.get("currency") or currency
                market_state = info.get("marketState") or market_state
            except Exception:
                logger.info("info unavailable for ticker=%s", ticker)

        try:
            if profile is None and _finnhub_enabled():
                profile = _finnhub_company_profile(ticker)
            if profile:
                name = profile.get("name") or name
                exchange = profile.get("exchange") or exchange
                currency = profile.get("currency") or currency
                market = _infer_market_from_exchange(exchange, ticker)
        except Exception:
            logger.info("Finnhub profile unavailable for ticker=%s", ticker)

        if change is None and _is_valid_price(price) and prev_close not in (None, 0):
            try:
                change = float(price) - float(prev_close)
                change_pct = (change / float(prev_close)) * 100
            except Exception:
                logger.warning("Failed change calculation for ticker=%s", ticker)

        out = {
            "symbol": ticker,
            "name": name or ticker,
            "exchange": exchange,
            "market": market or market_default,
            "currency": currency or currency_default,
            "price": _safe_float_or_none(price),
            "change": _safe_float_or_none(change),
            "changePercent": _safe_float_or_none(change_pct),
            "marketState": market_state,
            "asOf": _now_iso(),
        }

        _quote_cache[ticker] = out
        return out

    except Exception as e:
        logger.exception("Unhandled get_quote failure for ticker=%s", ticker)
        fallback = {
            "symbol": ticker,
            "name": ticker,
            "exchange": None,
            "market": market_default,
            "currency": currency_default,
            "price": None,
            "change": None,
            "changePercent": None,
            "marketState": "CLOSED",
            "asOf": _now_iso(),
            "error": str(e),
        }
        _quote_cache[ticker] = fallback
        return fallback


def get_company_profile(symbol: str) -> dict:
    symbol = (symbol or "").upper().strip()
    if not symbol:
        return {}

    cached = _profile_cache.get(symbol)
    if cached:
        return cached

    fallback = _empty_profile(symbol)

    if _finnhub_enabled():
        try:
            profile = _finnhub_company_profile(symbol)
            if profile:
                out = {
                    "symbol": symbol,
                    "sector": "",
                    "industry": profile.get("industry") or "",
                    "market_cap": _safe_float_or_none(profile.get("market_cap")),
                    "currency": profile.get("currency") or fallback["currency"],
                    "summary": "",
                }
                if out["market_cap"] is not None and out["market_cap"] < 1_000_000:
                    out["market_cap"] *= 1_000_000
                _profile_cache[symbol] = out
                return out
        except Exception:
            logger.exception("Finnhub company profile fetch failed for symbol=%s", symbol)

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}

        out = {
            "symbol": symbol,
            "sector": info.get("sector") or "",
            "industry": info.get("industry") or "",
            "market_cap": _safe_float_or_none(info.get("marketCap")),
            "currency": info.get("currency") or fallback["currency"],
            "summary": info.get("longBusinessSummary") or "",
        }
        _profile_cache[symbol] = out
        return out

    except Exception:
        logger.exception("Failed company profile fetch for symbol=%s", symbol)
        _profile_cache[symbol] = fallback
        return fallback


def get_quotes_batch(tickers: list[str]) -> list[dict]:
    return [get_quote(tk) for tk in tickers]