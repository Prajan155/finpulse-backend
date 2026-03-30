from __future__ import annotations

import datetime as dt
import logging

from cachetools import TTLCache
import yfinance as yf
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA

from app.core.cache import ttl_cache


logger = logging.getLogger(__name__)


def _iso(dt_like) -> str:
    """Convert pandas/py datetime to ISO string safely."""
    if hasattr(dt_like, "to_pydatetime"):
        dt_like = dt_like.to_pydatetime()
    if isinstance(dt_like, dt.datetime) and dt_like.tzinfo is None:
        dt_like = dt_like.replace(tzinfo=dt.timezone.utc)
    return dt_like.isoformat()


def _safe_float(value, default=0.0):
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


def _fetch_history_df(
    ticker: str,
    period: str,
    interval: str,
    *,
    for_label: str = "history",
) -> pd.DataFrame | None:
    t = yf.Ticker(ticker)
    df = None

    try:
        df = t.history(
            period=period,
            interval=interval,
            auto_adjust=False,
            actions=False,
        )
    except Exception:
        logger.exception(
            "Primary %s fetch failed for ticker=%s interval=%s period=%s",
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
                "Fallback %s download failed for ticker=%s interval=%s period=%s",
                for_label,
                ticker,
                interval,
                period,
            )
            return None

    df = _normalize_download_df(df)
    return df


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


@ttl_cache(prefix="history", ttl_seconds=300)
def get_history(ticker: str, interval: str = "1d", range_: str = "6mo") -> dict:
    ticker = ticker.strip().upper()

    try:
        df = _fetch_history_df(ticker, range_, interval, for_label="history")

        if df is None or df.empty:
            return {"symbol": ticker, "interval": interval, "range": range_, "candles": []}

        if "Close" not in df.columns:
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


@ttl_cache(prefix="ratios", ttl_seconds=300)
def get_ratios(ticker: str) -> dict:
    ticker = ticker.strip().upper()
    t = yf.Ticker(ticker)

    info = {}
    fast = {}

    try:
        info = t.info or {}
    except Exception:
        logger.warning("info fetch failed for %s", ticker)

    try:
        fast = t.fast_info or {}
    except Exception:
        logger.warning("fast_info fetch failed for %s", ticker)

    def _to_percent(value):
        val = _safe_float_or_none(value)
        if val is None:
            return None

        if -1 <= val <= 1:
            val = val * 100

        if val < 0:
            return None

        return val

    market_cap = _safe_float_or_none(
        info.get("marketCap")
        or fast.get("market_cap")
    )
    pe_ttm = _safe_float_or_none(
        info.get("trailingPE")
        or info.get("forwardPE")
    )
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


@ttl_cache(prefix="forecast_intraday", ttl_seconds=900)
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

            weekday = current_ts.weekday()
            if weekday >= 5:
                continue

            session_start = dt.time(9, 15)
            session_end = dt.time(15, 30)
            current_time = current_ts.time()

            if current_time < session_start:
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


@ttl_cache(prefix="forecast_long", ttl_seconds=900)
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


@ttl_cache(prefix="correlation", ttl_seconds=300)
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
    """
    yfinance financials are usually: rows=metrics, cols=periods
    This finds the first matching row name.
    """
    if df is None or df.empty:
        return None
    for name in candidates:
        if name in df.index:
            return df.loc[name]
    return None


@ttl_cache(prefix="revenue_expenses", ttl_seconds=1800)
def get_revenue_expenses(ticker: str, frequency: str = "quarterly") -> dict:
    """
    Returns revenue vs expenses points for charting.
    frequency: "quarterly" (default) or "annual"
    """
    ticker = ticker.strip().upper()

    try:
        t = yf.Ticker(ticker)
        info = {}
        try:
            info = t.info or {}
        except Exception:
            logger.warning("Failed info fetch for revenue/expenses ticker=%s", ticker)

        currency = info.get("currency") or ""

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
            "currency": "",
            "error": str(e),
        }


_quote_cache = TTLCache(maxsize=5000, ttl=60)


def _now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def get_quote(ticker: str) -> dict:
    ticker = ticker.strip().upper()

    t = yf.Ticker(ticker)

    price = None
    prev_close = None
    change = None
    change_pct = None

    currency = None
    name = ticker
    exchange = None
    market = None
    market_state = None

    try:
        fast = t.fast_info or {}
        price = fast.get("lastPrice") or fast.get("last_price") or fast.get("regularMarketPrice")
        prev_close = fast.get("previousClose") or fast.get("previous_close")
        currency = fast.get("currency")
        exchange = fast.get("exchange")
    except Exception:
        logger.info("fast_info unavailable for ticker=%s", ticker)

    try:
        info = t.info or {}
        name = (
            info.get("shortName")
            or info.get("longName")
            or info.get("displayName")
            or ticker
        )
        exchange = exchange or info.get("exchange") or info.get("fullExchangeName")
        market = info.get("market") or info.get("marketType") or info.get("exchangeTimezoneName")
        currency = currency or info.get("currency")
        market_state = info.get("marketState")

        if price is None:
            price = info.get("regularMarketPrice") or info.get("currentPrice")
        if prev_close is None:
            prev_close = info.get("regularMarketPreviousClose") or info.get("previousClose")
    except Exception:
        logger.info("info unavailable for ticker=%s", ticker)

    try:
        df = yf.download(
            ticker,
            period="5d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )

        if df is not None and not df.empty and "Close" in df.columns:
            closes = df["Close"].dropna()

            if len(closes) >= 2:
                price = float(closes.iloc[-1])
                prev_close = float(closes.iloc[-2])
                change = price - prev_close
                change_pct = (change / prev_close) * 100 if prev_close else None
            elif len(closes) == 1:
                price = float(closes.iloc[-1])
                if prev_close is None:
                    prev_close = price
    except Exception:
        logger.exception("History fallback failed for ticker=%s", ticker)

    if change is None and price is not None and prev_close not in (None, 0):
        try:
            change = float(price) - float(prev_close)
            change_pct = (change / float(prev_close)) * 100
        except Exception:
            logger.warning("Failed change calculation for ticker=%s", ticker)

    return {
        "symbol": ticker,
        "name": name or ticker,
        "exchange": exchange,
        "market": market,
        "currency": currency or ("INR" if ticker.endswith(".NS") else "USD"),
        "price": _safe_float_or_none(price),
        "change": _safe_float_or_none(change),
        "changePercent": _safe_float_or_none(change_pct),
        "marketState": market_state,
        "asOf": _now_iso(),
    }


def get_company_profile(symbol: str) -> dict:
    symbol = (symbol or "").upper().strip()
    if not symbol:
        return {}

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}

        return {
            "symbol": symbol,
            "sector": info.get("sector") or "",
            "industry": info.get("industry") or "",
            "market_cap": _safe_float_or_none(info.get("marketCap")),
            "currency": info.get("currency") or "",
            "summary": info.get("longBusinessSummary") or "",
        }
    except Exception:
        logger.exception("Failed company profile fetch for symbol=%s", symbol)
        return {
            "symbol": symbol,
            "sector": "",
            "industry": "",
            "market_cap": None,
            "currency": "",
            "summary": "",
        }


def get_quotes_batch(tickers: list[str]) -> list[dict]:
    return [get_quote(tk) for tk in tickers]