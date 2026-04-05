import logging
import time
from typing import Any, Dict, Optional

import requests
import yfinance as yf

from app.core.config import settings

logger = logging.getLogger(__name__)

FINNHUB_API_KEY = (settings.finnhub_api_key or "").strip()
TWELVE_API_KEY = (getattr(settings, "twelve_data_api_key", "") or "").strip()

FINNHUB_BASE = "https://finnhub.io/api/v1"
TWELVE_BASE = "https://api.twelvedata.com"


def _safe_float(value):
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def _utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _is_indian(symbol: str) -> bool:
    s = (symbol or "").upper()
    return (
        s.endswith(".NS")
        or s.endswith(".BO")
        or s.startswith("NSE:")
        or s.startswith("BSE:")
    )


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").upper().strip()


def _build_quote(
    symbol: str,
    name: Optional[str] = None,
    price: Optional[float] = None,
    previous_close: Optional[float] = None,
    change: Optional[float] = None,
    change_percent: Optional[float] = None,
    currency: Optional[str] = None,
    exchange: Optional[str] = None,
    market: Optional[str] = None,
    source: Optional[str] = None,
    market_state: Optional[str] = "LIVE",
) -> Dict[str, Any]:
    symbol = _normalize_symbol(symbol)

    if change is None and price is not None and previous_close is not None:
        change = price - previous_close

    if change_percent is None and change is not None and previous_close not in (None, 0):
        change_percent = (change / previous_close) * 100

    is_india = _is_indian(symbol)

    return {
        "symbol": symbol,
        "name": name or symbol,
        "price": _safe_float(price),
        "change": _safe_float(change),
        "changePercent": _safe_float(change_percent),
        "marketState": market_state or "LIVE",
        "currency": currency or ("INR" if is_india else "USD"),
        "exchange": exchange or ("NSE" if is_india else "US"),
        "market": market or ("India" if is_india else "US"),
        "source": source or "unknown",
        "asOf": _utc_now(),
    }


def _empty(
    symbol: str,
    source: Optional[str] = None,
    currency: Optional[str] = None,
    exchange: Optional[str] = None,
    market: Optional[str] = None,
) -> Dict[str, Any]:
    symbol = _normalize_symbol(symbol)
    is_india = _is_indian(symbol)

    return {
        "symbol": symbol,
        "name": symbol,
        "price": None,
        "change": None,
        "changePercent": None,
        "marketState": "CLOSED",
        "currency": currency or ("INR" if is_india else "USD"),
        "exchange": exchange or ("NSE" if is_india else "US"),
        "market": market or ("India" if is_india else "US"),
        "source": source or "unknown",
        "asOf": _utc_now(),
    }


# =========================
# 🇮🇳 TWELVE DATA (PRIMARY FOR INDIA)
# =========================
def _get_twelve_quote(symbol: str) -> Optional[Dict[str, Any]]:
    if not TWELVE_API_KEY:
        logger.error("[TWELVE] API key missing")
        return None

    raw_symbol = _normalize_symbol(symbol)
    base = raw_symbol

    if base.startswith("NSE:") or base.startswith("BSE:"):
        base = base.split(":", 1)[1]

    candidates = []

    if base.endswith(".NS"):
        core = base[:-3]
        candidates = [
            {"symbol": f"{core}.NSE"},
            {"symbol": core, "exchange": "NSE"},
            {"symbol": base},
        ]
    elif base.endswith(".BO"):
        core = base[:-3]
        candidates = [
            {"symbol": f"{core}.BSE"},
            {"symbol": core, "exchange": "BSE"},
            {"symbol": base},
        ]
    else:
        candidates = [
            {"symbol": f"{base}.NSE"},
            {"symbol": base, "exchange": "NSE"},
            {"symbol": base},
        ]

    for params in candidates:
        try:
            req = dict(params)
            req["apikey"] = TWELVE_API_KEY

            logger.info("[TWELVE] trying params=%s", req)

            response = requests.get(f"{TWELVE_BASE}/quote", params=req, timeout=10)
            body_preview = response.text[:500]
            logger.info("[TWELVE] status=%s body=%s", response.status_code, body_preview)

            response.raise_for_status()
            data = response.json()

            if not isinstance(data, dict):
                continue

            if data.get("status") == "error" or data.get("code"):
                logger.error("[TWELVE] api error for %s -> %s", raw_symbol, data)
                continue

            price = _safe_float(data.get("price"))
            prev_close = _safe_float(
                data.get("previous_close")
                or data.get("previousClose")
                or data.get("prev_close")
            )
            percent_change = _safe_float(
                data.get("percent_change")
                or data.get("percentChange")
                or data.get("change_percent")
            )
            change = _safe_float(data.get("change"))

            if price is None:
                logger.error("[TWELVE] no price for %s -> %s", raw_symbol, data)
                continue

            exchange = data.get("exchange")
            if not exchange:
                if ".BO" in raw_symbol or raw_symbol.startswith("BSE:"):
                    exchange = "BSE"
                else:
                    exchange = "NSE"

            return _build_quote(
                symbol=raw_symbol,
                name=data.get("name") or raw_symbol,
                price=price,
                previous_close=prev_close,
                change=change,
                change_percent=percent_change,
                currency=data.get("currency") or "INR",
                exchange=exchange,
                market="India",
                source="twelve_data",
                market_state="LIVE",
            )

        except Exception:
            logger.exception(
                "[TWELVE] failed attempt for %s with params=%s", raw_symbol, params
            )

    return None


# =========================
# 🇺🇸 FINNHUB (PRIMARY FOR US)
# =========================
def _get_finnhub(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        symbol = _normalize_symbol(symbol)

        if not FINNHUB_API_KEY:
            logger.error("[FINNHUB] API key missing")
            return None

        response = requests.get(
            f"{FINNHUB_BASE}/quote",
            params={"symbol": symbol, "token": FINNHUB_API_KEY},
            timeout=8,
        )
        response.raise_for_status()
        data = response.json()

        price = _safe_float(data.get("c"))
        prev_close = _safe_float(data.get("pc"))
        change = _safe_float(data.get("d"))
        change_percent = _safe_float(data.get("dp"))

        if price is None:
            logger.error("[FINNHUB] no price for %s -> %s", symbol, data)
            return None

        return _build_quote(
            symbol=symbol,
            name=symbol,
            price=price,
            previous_close=prev_close,
            change=change,
            change_percent=change_percent,
            currency="USD",
            exchange="US",
            market="US",
            source="finnhub",
            market_state="LIVE",
        )

    except Exception:
        logger.exception("[FINNHUB] failed for %s", symbol)
        return None


# =========================
# 🔁 YAHOO (LAST FALLBACK)
# =========================
def _get_yahoo(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        symbol = _normalize_symbol(symbol)
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="5d", auto_adjust=False)

        if hist.empty:
            logger.error("[YAHOO] empty history for %s", symbol)
            return None

        last = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) > 1 else None

        price = _safe_float(last.get("Close"))
        prev_close = _safe_float(prev.get("Close")) if prev is not None else None

        if price is None:
            logger.error("[YAHOO] no close price for %s", symbol)
            return None

        info = {}
        try:
            info = ticker.fast_info or {}
        except Exception:
            info = {}

        is_india = _is_indian(symbol)

        return _build_quote(
            symbol=symbol,
            name=symbol,
            price=price,
            previous_close=prev_close,
            currency="INR" if is_india else "USD",
            exchange="NSE" if is_india else "US",
            market="India" if is_india else "US",
            source="yahoo",
            market_state="LIVE",
        )

    except Exception:
        logger.exception("[YAHOO] failed for %s", symbol)
        return None


# =========================
# 🚀 FINAL ROUTER
# =========================
def get_quote(symbol: str) -> Dict[str, Any]:
    symbol = _normalize_symbol(symbol)

    if _is_indian(symbol):
        logger.info(
            "[QUOTE][INDIA] symbol=%s twelve_key_present=%s",
            symbol,
            bool(TWELVE_API_KEY),
        )

        quote = _get_twelve_quote(symbol)
        if quote:
            logger.info("[QUOTE][INDIA] success via Twelve Data for %s", symbol)
            return quote

        logger.error("[QUOTE][INDIA] Twelve Data failed for %s", symbol)

        quote = _get_yahoo(symbol)
        if quote:
            logger.info("[QUOTE][INDIA] fallback success via Yahoo for %s", symbol)
            return quote

        logger.error("[QUOTE][INDIA] all providers failed for %s", symbol)
        return _empty(
            symbol,
            source="india_all_failed",
            currency="INR",
            exchange="NSE",
            market="India",
        )

    quote = _get_finnhub(symbol)
    if quote:
        logger.info("[QUOTE][US] success via Finnhub for %s", symbol)
        return quote

    logger.error("[QUOTE][US] Finnhub failed for %s", symbol)

    quote = _get_yahoo(symbol)
    if quote:
        logger.info("[QUOTE][US] fallback success via Yahoo for %s", symbol)
        return quote

    logger.error("[QUOTE][US] all providers failed for %s", symbol)
    return _empty(
        symbol,
        source="us_all_failed",
        currency="USD",
        exchange="US",
        market="US",
    )


# backward compatibility
def get_upstox_quote(symbol: str) -> Dict[str, Any]:
    return get_quote(symbol)