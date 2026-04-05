import os
import time
import logging
from typing import Any, Dict, Optional

import requests
import yfinance as yf

logger = logging.getLogger(__name__)

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
TWELVE_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

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
def _get_twelve_quote(symbol: str):
    if not TWELVE_API_KEY:
        return None

    try:
        sym = _to_twelve_symbol(symbol)

        url = f"{TWELVE_BASE}/quote"
        params = {
            "symbol": sym,
            "exchange": "NSE",
            "apikey": TWELVE_API_KEY,
        }

        r = requests.get(url, params=params, timeout=8)
        data = r.json()

        if "price" not in data:
            return None

        price = _safe_float(data.get("price"))
        prev_close = _safe_float(data.get("previous_close"))

        change = None
        change_pct = None

        if price and prev_close:
            change = price - prev_close
            change_pct = (change / prev_close) * 100

        return {
            "symbol": symbol,
            "name": data.get("name") or symbol,
            "price": price,
            "change": change,
            "changePercent": change_pct,
            "marketState": "LIVE",
            "currency": "INR",
            "exchange": "NSE",
            "market": "India",
            "asOf": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    except Exception:
        logger.exception("[TWELVE] failed for %s", symbol)
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
        q = _get_twelve_quote(symbol)
        if q:
            return q

        q = _get_yahoo(symbol)
        if q:
            return q

        return _empty(symbol)

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