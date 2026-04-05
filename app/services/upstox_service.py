import os
import time
import logging
from typing import Any, Dict, Optional

import requests
import yfinance as yf

logger = logging.getLogger(__name__)

FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _is_indian_symbol(symbol: str) -> bool:
    s = (symbol or "").upper().strip()
    return (
        s.endswith(".NS")
        or s.endswith(".BO")
        or s.startswith("NSE:")
        or s.startswith("BSE:")
    )


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _to_yahoo_symbol(symbol: str) -> str:
    s = _normalize_symbol(symbol)

    if s.startswith("NSE:"):
        return s.replace("NSE:", "") + ".NS"
    if s.startswith("BSE:"):
        return s.replace("BSE:", "") + ".BO"

    return s


def _to_finnhub_symbol(symbol: str) -> str:
    s = _normalize_symbol(symbol)

    if s.endswith(".NS"):
        return f"NSE:{s[:-3]}"
    if s.endswith(".BO"):
        return f"BSE:{s[:-3]}"

    return s


def _empty_quote(symbol: str, source: str = "none") -> Dict[str, Any]:
    clean_symbol = _normalize_symbol(symbol)
    now_ts = int(time.time())

    return {
        "symbol": clean_symbol,
        "name": clean_symbol,
        "price": None,
        "current_price": None,
        "change": None,
        "change_percent": None,
        "changePercent": None,
        "previous_close": None,
        "previousClose": None,
        "open": None,
        "day_high": None,
        "dayHigh": None,
        "high": None,
        "day_low": None,
        "dayLow": None,
        "low": None,
        "volume": None,
        "currency": None,
        "exchange": None,
        "market_state": "unknown",
        "marketState": "unknown",
        "timestamp": now_ts,
        "source": source,
        "market": "India" if _is_indian_symbol(clean_symbol) else "US",
        "asOf": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts)),
    }


def _finalize_quote(
    symbol: str,
    *,
    price: Optional[float],
    previous_close: Optional[float],
    open_price: Optional[float],
    day_high: Optional[float],
    day_low: Optional[float],
    volume: Optional[int],
    currency: Optional[str],
    exchange: Optional[str],
    market_state: Optional[str],
    name: Optional[str],
    source: str,
) -> Dict[str, Any]:
    clean_symbol = _normalize_symbol(symbol)
    now_ts = int(time.time())

    change = None
    change_percent = None

    if price is not None and previous_close not in (None, 0):
        change = round(price - previous_close, 4)
        change_percent = round((change / previous_close) * 100, 4)

    market = "India" if _is_indian_symbol(clean_symbol) else "US"

    return {
        "symbol": clean_symbol,
        "name": name or clean_symbol,
        "price": price,
        "current_price": price,
        "change": change,
        "change_percent": change_percent,
        "changePercent": change_percent,
        "previous_close": previous_close,
        "previousClose": previous_close,
        "open": open_price,
        "day_high": day_high,
        "dayHigh": day_high,
        "high": day_high,
        "day_low": day_low,
        "dayLow": day_low,
        "low": day_low,
        "volume": volume,
        "currency": currency,
        "exchange": exchange,
        "market_state": market_state or "unknown",
        "marketState": (market_state or "unknown").upper(),
        "timestamp": now_ts,
        "source": source,
        "market": market,
        "asOf": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts)),
    }


def _finnhub_request(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not FINNHUB_API_KEY:
        raise RuntimeError("FINNHUB_API_KEY is missing")

    req_params = dict(params or {})
    req_params["token"] = FINNHUB_API_KEY

    url = f"{FINNHUB_BASE_URL}{path}"
    resp = requests.get(url, params=req_params, timeout=12)
    resp.raise_for_status()
    return resp.json()


def _get_finnhub_quote_us(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        finnhub_symbol = _to_finnhub_symbol(symbol)
        data = _finnhub_request("/quote", {"symbol": finnhub_symbol})

        price = _safe_float(data.get("c"))
        previous_close = _safe_float(data.get("pc"))
        open_price = _safe_float(data.get("o"))
        day_high = _safe_float(data.get("h"))
        day_low = _safe_float(data.get("l"))

        if price is None:
            return None

        return _finalize_quote(
            symbol=symbol,
            price=price,
            previous_close=previous_close,
            open_price=open_price,
            day_high=day_high,
            day_low=day_low,
            volume=None,
            currency="USD",
            exchange="US",
            market_state="LIVE",
            name=_normalize_symbol(symbol),
            source="finnhub",
        )
    except Exception:
        logger.exception("[QUOTE][US][FINNHUB] failed for %s", symbol)
        return None


def _get_yahoo_quote(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        yahoo_symbol = _to_yahoo_symbol(symbol)
        ticker = yf.Ticker(yahoo_symbol)

        # 🔥 FORCE history-based pricing (most reliable on Render)
        hist = ticker.history(period="5d", interval="1d", auto_adjust=False)

        if hist is None or hist.empty:
            logger.error("[YAHOO] history empty for %s", yahoo_symbol)
            return None

        last = hist.iloc[-1]

        price = _safe_float(last.get("Close"))
        open_price = _safe_float(last.get("Open"))
        day_high = _safe_float(last.get("High"))
        day_low = _safe_float(last.get("Low"))
        volume = _safe_int(last.get("Volume"))

        previous_close = None
        if len(hist) >= 2:
            previous_close = _safe_float(hist.iloc[-2].get("Close"))

        return _finalize_quote(
            symbol=symbol,
            price=price,
            previous_close=previous_close,
            open_price=open_price,
            day_high=day_high,
            day_low=day_low,
            volume=volume,
            currency="INR" if _is_indian_symbol(symbol) else "USD",
            exchange="NSE" if _is_indian_symbol(symbol) else "US",
            market_state="LIVE",
            name=_normalize_symbol(symbol),
            source="yahoo_history",
        )

    except Exception:
        logger.exception("[QUOTE][YAHOO] failed for %s", symbol)
        return None


def get_quote(symbol: str) -> Dict[str, Any]:
    clean_symbol = _normalize_symbol(symbol)

    if not clean_symbol:
        return _empty_quote(symbol, source="invalid")

    if _is_indian_symbol(clean_symbol):
        yahoo_quote = _get_yahoo_quote(clean_symbol)
        if yahoo_quote:
            logger.info("[QUOTE][INDIA][YAHOO] ticker=%s", clean_symbol)
            return yahoo_quote

        logger.error("[QUOTE][INDIA] empty quote for %s", clean_symbol)
        return _empty_quote(clean_symbol, source="yahoo_failed")

    finnhub_quote = _get_finnhub_quote_us(clean_symbol)
    if finnhub_quote:
        logger.info("[QUOTE][US][FINNHUB] ticker=%s", clean_symbol)
        return finnhub_quote

    yahoo_fallback = _get_yahoo_quote(clean_symbol)
    if yahoo_fallback:
        logger.info("[QUOTE][US][YAHOO_FALLBACK] ticker=%s", clean_symbol)
        return yahoo_fallback

    logger.error("[QUOTE][US] empty quote for %s", clean_symbol)
    return _empty_quote(clean_symbol, source="all_failed")


def get_upstox_quote(symbol: str):
    return get_quote(symbol)