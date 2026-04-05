import time
import logging
from typing import Any, Dict, Optional
from app.core.config import settings
import requests
import yfinance as yf

logger = logging.getLogger(__name__)

FINNHUB_API_KEY = (settings.finnhub_api_key or "").strip()
TWELVE_API_KEY = (getattr(settings, "twelve_data_api_key", "") or "").strip()

FINNHUB_BASE = "https://finnhub.io/api/v1"
TWELVE_BASE = "https://api.twelvedata.com"


def _safe_float(v):
    try:
        return float(v) if v is not None else None
    except:
        return None


def _is_indian(symbol: str):
    s = symbol.upper()
    return s.endswith(".NS") or s.endswith(".BO") or "NSE" in s


def _to_twelve_symbol(symbol: str):
    s = symbol.upper()
    if s.endswith(".NS"):
        return s.replace(".NS", "")
    return s


def _empty(symbol):
    return {
        "symbol": symbol,
        "name": symbol,
        "price": None,
        "change": None,
        "changePercent": None,
        "marketState": "unknown",
        "currency": None,
        "exchange": None,
        "market": "India" if _is_indian(symbol) else "US",
        "asOf": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# =========================
# 🇮🇳 TWELVE DATA (PRIMARY)
# =========================
def _get_twelve_quote(symbol: str) -> Optional[Dict[str, Any]]:
    if not TWELVE_API_KEY:
        logger.error("[TWELVE] API key missing")
        return None

    raw_symbol = (symbol or "").upper().strip()

    candidates = []
    base = raw_symbol

    if base.startswith("NSE:") or base.startswith("BSE:"):
        base = base.split(":", 1)[1]

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

            r = requests.get(f"{TWELVE_BASE}/quote", params=req, timeout=10)
            body_preview = r.text[:500]
            logger.info("[TWELVE] status=%s body=%s", r.status_code, body_preview)

            r.raise_for_status()

            data = r.json()
            if not isinstance(data, dict):
                continue

            if data.get("status") == "error" or data.get("code"):
                logger.error("[TWELVE] api error for %s -> %s", symbol, data)
                continue

            price = _safe_float(data.get("price"))
            prev_close = _safe_float(
                data.get("previous_close")
                or data.get("previousClose")
                or data.get("prev_close")
            )

            if price is None:
                logger.error("[TWELVE] no price for %s -> %s", symbol, data)
                continue

            exchange = "NSE"
            if ".BO" in raw_symbol or raw_symbol.startswith("BSE:"):
                exchange = "BSE"

            return _build_quote(
                symbol=raw_symbol,
                name=data.get("name") or raw_symbol,
                price=price,
                previous_close=prev_close,
                currency=data.get("currency") or "INR",
                exchange=data.get("exchange") or exchange,
                market="India",
                source="twelve_data",
            )

        except Exception:
            logger.exception("[TWELVE] failed attempt for %s with params=%s", symbol, params)

    return None

# =========================
# 🇺🇸 FINNHUB (PRIMARY)
# =========================
def _get_finnhub(symbol: str):
    try:
        if not FINNHUB_API_KEY:
            return None

        url = f"{FINNHUB_BASE}/quote"
        params = {
            "symbol": symbol,
            "token": FINNHUB_API_KEY,
        }

        r = requests.get(url, params=params, timeout=8)
        data = r.json()

        price = _safe_float(data.get("c"))
        prev = _safe_float(data.get("pc"))

        if not price:
            return None

        change = price - prev if prev else None
        change_pct = (change / prev * 100) if prev else None

        return {
            "symbol": symbol,
            "name": symbol,
            "price": price,
            "change": change,
            "changePercent": change_pct,
            "marketState": "LIVE",
            "currency": "USD",
            "exchange": "US",
            "market": "US",
            "asOf": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    except Exception:
        logger.exception("[FINNHUB] failed")
        return None


# =========================
# 🔁 YAHOO (LAST FALLBACK)
# =========================
def _get_yahoo(symbol: str):
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="5d")

        if hist.empty:
            return None

        last = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) > 1 else None

        price = _safe_float(last["Close"])
        prev_close = _safe_float(prev["Close"]) if prev is not None else None

        change = price - prev_close if prev_close else None
        change_pct = (change / prev_close * 100) if prev_close else None

        return {
            "symbol": symbol,
            "name": symbol,
            "price": price,
            "change": change,
            "changePercent": change_pct,
            "marketState": "LIVE",
            "currency": "INR" if _is_indian(symbol) else "USD",
            "exchange": "NSE" if _is_indian(symbol) else "US",
            "market": "India" if _is_indian(symbol) else "US",
            "asOf": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    except:
        return None


# =========================
# 🚀 FINAL ROUTER
# =========================
def get_quote(symbol: str):
    symbol = symbol.upper().strip()

    if _is_indian(symbol):
        logger.info("[QUOTE][INDIA] symbol=%s twelve_key_present=%s", symbol, bool(TWELVE_API_KEY))

        q = _get_twelve_quote(symbol)
        if q:
            logger.info("[QUOTE][INDIA] success via Twelve Data for %s", symbol)
            return q

        logger.error("[QUOTE][INDIA] Twelve Data failed for %s", symbol)
        return _empty(symbol, source="india_twelve_failed")
    else:
        q = _get_finnhub(symbol)
        if q:
            return q

        q = _get_yahoo(symbol)
        if q:
            return q

        return _empty(symbol)


# backward compatibility
def get_upstox_quote(symbol: str):
    return get_quote(symbol)