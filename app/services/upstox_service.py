import gzip
import json
import logging
import time
from typing import Any, Dict, Iterable, Optional

import requests
import yfinance as yf
from cachetools import TTLCache

from app.core.config import settings

logger = logging.getLogger(__name__)

FINNHUB_API_KEY = (settings.finnhub_api_key or "").strip()
UPSTOX_TOKEN = (settings.upstox_analytics_token or "").strip()

FINNHUB_BASE = "https://finnhub.io/api/v1"
UPSTOX_BASE = "https://api.upstox.com/v2"
UPSTOX_WS_AUTHORIZE_V3 = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

# Upstox instrument master URLs
# NSE/BSE instrument JSON links are documented by Upstox under Instruments.
UPSTOX_NSE_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
UPSTOX_BSE_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"

HTTP_TIMEOUT = 12

_http = requests.Session()

# Caches
# instrument master: 6h
# symbol resolution: 24h
# quote cache: 10s
_instrument_master_cache = TTLCache(maxsize=4, ttl=6 * 60 * 60)
_symbol_resolution_cache = TTLCache(maxsize=20000, ttl=24 * 60 * 60)
_quote_cache = TTLCache(maxsize=10000, ttl=10)


def _safe_float(value):
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def _utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _is_indian(symbol: str) -> bool:
    s = (symbol or "").upper().strip()
    return (
        s.endswith(".NS")
        or s.endswith(".BO")
        or s.startswith("NSE:")
        or s.startswith("BSE:")
    )


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").upper().strip()


def _default_exchange_market_currency(symbol: str) -> tuple[str, str, str]:
    symbol = _normalize_symbol(symbol)
    if symbol.endswith(".BO") or symbol.startswith("BSE:"):
        return "BSE", "India", "INR"
    if _is_indian(symbol):
        return "NSE", "India", "INR"
    return "US", "US", "USD"


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

    default_exchange, default_market, default_currency = _default_exchange_market_currency(symbol)

    return {
        "symbol": symbol,
        "name": name or symbol,
        "price": _safe_float(price),
        "change": _safe_float(change),
        "changePercent": _safe_float(change_percent),
        "marketState": market_state or "LIVE",
        "currency": currency or default_currency,
        "exchange": exchange or default_exchange,
        "market": market or default_market,
        "source": source or "unknown",
        "asOf": _utc_now(),
    }


def _empty(
    symbol: str,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    exchange, market, currency = _default_exchange_market_currency(symbol)
    return {
        "symbol": _normalize_symbol(symbol),
        "name": _normalize_symbol(symbol),
        "price": None,
        "change": None,
        "changePercent": None,
        "marketState": "CLOSED",
        "currency": currency,
        "exchange": exchange,
        "market": market,
        "source": source or "unknown",
        "asOf": _utc_now(),
    }


def _upstox_headers() -> dict:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {UPSTOX_TOKEN}",
    }


def _gunzip_json_bytes(payload: bytes):
    if not payload:
        return None

    try:
        if payload[:2] == b"\x1f\x8b":
            payload = gzip.decompress(payload)
    except Exception:
        logger.exception("[UPSTOX] failed to gunzip instrument payload")
        return None

    try:
        return json.loads(payload.decode("utf-8"))
    except Exception:
        logger.exception("[UPSTOX] failed to decode instrument JSON")
        return None


def _load_exchange_instruments(exchange_code: str) -> list[dict]:
    exchange_code = (exchange_code or "").upper().strip()
    cache_key = f"exchange:{exchange_code}"

    cached = _instrument_master_cache.get(cache_key)
    if cached is not None:
        return cached

    if exchange_code == "BSE":
        url = UPSTOX_BSE_INSTRUMENTS_URL
    else:
        url = UPSTOX_NSE_INSTRUMENTS_URL

    try:
        resp = _http.get(url, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = _gunzip_json_bytes(resp.content)

        if not isinstance(data, list):
            logger.error("[UPSTOX] invalid instrument file for %s", exchange_code)
            _instrument_master_cache[cache_key] = []
            return []

        _instrument_master_cache[cache_key] = data
        logger.info("[UPSTOX] loaded %s instruments: %s rows", exchange_code, len(data))
        return data

    except Exception:
        logger.exception("[UPSTOX] failed loading %s instrument master", exchange_code)
        _instrument_master_cache[cache_key] = []
        return []


def _clean_symbol_base(symbol: str) -> tuple[str, str]:
    s = _normalize_symbol(symbol)
    exchange = "NSE"

    if s.startswith("NSE:"):
        s = s.split(":", 1)[1]
        exchange = "NSE"
    elif s.startswith("BSE:"):
        s = s.split(":", 1)[1]
        exchange = "BSE"

    if s.endswith(".NS"):
        s = s[:-3]
        exchange = "NSE"
    elif s.endswith(".BO"):
        s = s[:-3]
        exchange = "BSE"

    return s.upper().strip(), exchange


def _resolve_instrument(symbol: str) -> Optional[dict]:
    symbol = _normalize_symbol(symbol)

    cached = _symbol_resolution_cache.get(symbol)
    if cached is not None:
        return cached

    base, exchange = _clean_symbol_base(symbol)
    instruments = _load_exchange_instruments(exchange)

    if not instruments:
        _symbol_resolution_cache[symbol] = None
        return None

    exact_match = None
    fallback_match = None

    for item in instruments:
        try:
            segment = (item.get("segment") or "").upper().strip()
            instrument_type = (item.get("instrument_type") or "").upper().strip()
            trading_symbol = (item.get("trading_symbol") or "").upper().strip()
            short_name = (item.get("short_name") or "").upper().strip()
            name = (item.get("name") or "").upper().strip()
            security_type = (item.get("security_type") or "").upper().strip()

            if exchange == "NSE" and segment != "NSE_EQ":
                continue
            if exchange == "BSE" and segment != "BSE_EQ":
                continue
            if instrument_type != "EQ":
                continue
            if security_type and security_type not in {"NORMAL", "SM", "BE", "BZ", "ST"}:
                # do not overfilter if missing, but skip obviously non-standard rows when present
                pass

            if trading_symbol == base:
                exact_match = item
                break

            if short_name == base or name == base:
                fallback_match = fallback_match or item

        except Exception:
            continue

    resolved = exact_match or fallback_match
    _symbol_resolution_cache[symbol] = resolved
    return resolved


def resolve_instrument_key(symbol: str) -> Optional[str]:
    item = _resolve_instrument(symbol)
    if not item:
        return None
    return item.get("instrument_key")


def refresh_instrument_master() -> dict:
    nse = _load_exchange_instruments("NSE")
    bse = _load_exchange_instruments("BSE")
    return {
        "ok": True,
        "nse_count": len(nse),
        "bse_count": len(bse),
        "asOf": _utc_now(),
    }


def _parse_upstox_quote_entry(symbol: str, entry: dict, resolved: Optional[dict]) -> Dict[str, Any]:
    ohlc = entry.get("ohlc") or {}
    price = _safe_float(entry.get("last_price"))
    previous_close = _safe_float(ohlc.get("close"))
    change = _safe_float(entry.get("net_change"))

    if change is None and price is not None and previous_close is not None:
        change = price - previous_close

    change_percent = None
    if change is not None and previous_close not in (None, 0):
        change_percent = (change / previous_close) * 100

    exchange, market, currency = _default_exchange_market_currency(symbol)
    if resolved:
        exchange = (resolved.get("exchange") or exchange).upper()

    name = None
    if resolved:
        name = (
            resolved.get("short_name")
            or resolved.get("trading_symbol")
            or resolved.get("name")
        )

    market_state = "LIVE" if price is not None else "CLOSED"

    return _build_quote(
        symbol=symbol,
        name=name or symbol,
        price=price,
        previous_close=previous_close,
        change=change,
        change_percent=change_percent,
        currency=currency,
        exchange=exchange,
        market=market,
        source="upstox_rest",
        market_state=market_state,
    )


def _fetch_upstox_quotes_by_instrument_keys(instrument_keys: list[str]) -> dict:
    if not UPSTOX_TOKEN:
        logger.error("[UPSTOX] access token missing")
        return {}

    if not instrument_keys:
        return {}

    unique_keys = []
    seen = set()
    for key in instrument_keys:
        if key and key not in seen:
            unique_keys.append(key)
            seen.add(key)

    out = {}

    for i in range(0, len(unique_keys), 500):
        batch = unique_keys[i:i + 500]
        try:
            resp = _http.get(
                f"{UPSTOX_BASE}/market-quote/quotes",
                headers=_upstox_headers(),
                params={"instrument_key": ",".join(batch)},
                timeout=HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, dict):
                continue
            payload = data.get("data") or {}
            if isinstance(payload, dict):
                out.update(payload)

        except Exception:
            logger.exception("[UPSTOX] quote batch fetch failed for %s keys", len(batch))

    return out


def _get_upstox_quote(symbol: str) -> Optional[Dict[str, Any]]:
    symbol = _normalize_symbol(symbol)
    resolved = _resolve_instrument(symbol)
    if not resolved:
        logger.error("[UPSTOX] instrument not found for %s", symbol)
        return None

    instrument_key = resolved.get("instrument_key")
    if not instrument_key:
        logger.error("[UPSTOX] instrument key missing for %s", symbol)
        return None

    payload = _fetch_upstox_quotes_by_instrument_keys([instrument_key])
    if not payload:
        return None

    entry = payload.get(instrument_key)
    if not entry and isinstance(payload, dict):
        # defensive fallback if key format differs
        try:
            entry = next(iter(payload.values()))
        except Exception:
            entry = None

    if not isinstance(entry, dict):
        logger.error("[UPSTOX] malformed quote entry for %s", symbol)
        return None

    return _parse_upstox_quote_entry(symbol, entry, resolved)


def get_quotes_batch(symbols: list[str]) -> list[Dict[str, Any]]:
    normalized = [_normalize_symbol(s) for s in symbols if s and str(s).strip()]
    normalized = list(dict.fromkeys(normalized))
    if not normalized:
        return []

    results_by_symbol: dict[str, Dict[str, Any]] = {}
    upstox_keys: list[str] = []
    upstox_key_to_symbol: dict[str, str] = {}
    resolved_by_symbol: dict[str, Optional[dict]] = {}

    # First, honor cache
    uncached_symbols = []
    for symbol in normalized:
        cached = _quote_cache.get(symbol)
        if cached is not None:
            results_by_symbol[symbol] = cached
        else:
            uncached_symbols.append(symbol)

    # Resolve Indian symbols for Upstox bulk fetch
    for symbol in uncached_symbols:
        if _is_indian(symbol):
            resolved = _resolve_instrument(symbol)
            resolved_by_symbol[symbol] = resolved
            if resolved and resolved.get("instrument_key"):
                key = resolved["instrument_key"]
                upstox_keys.append(key)
                upstox_key_to_symbol[key] = symbol
            else:
                results_by_symbol[symbol] = _empty(symbol, source="upstox_instrument_not_found")
        else:
            quote = _get_finnhub(symbol) or _get_yahoo(symbol) or _empty(symbol, source="us_all_failed")
            results_by_symbol[symbol] = quote
            _quote_cache[symbol] = quote

    # Fetch Indian quotes in bulk
    if upstox_keys:
        raw_quotes = _fetch_upstox_quotes_by_instrument_keys(upstox_keys)

        for key, symbol in upstox_key_to_symbol.items():
            entry = raw_quotes.get(key)
            resolved = resolved_by_symbol.get(symbol)

            if isinstance(entry, dict):
                quote = _parse_upstox_quote_entry(symbol, entry, resolved)
            else:
                quote = _get_yahoo(symbol) or _empty(symbol, source="india_all_failed")

            results_by_symbol[symbol] = quote
            _quote_cache[symbol] = quote

    # Preserve input order
    return [results_by_symbol.get(symbol, _empty(symbol, source="batch_missing")) for symbol in normalized]


# =========================
# 🇺🇸 FINNHUB (PRIMARY FOR US)
# =========================
def _get_finnhub(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        symbol = _normalize_symbol(symbol)

        if not FINNHUB_API_KEY:
            logger.error("[FINNHUB] API key missing")
            return None

        response = _http.get(
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

        # more reliable fallback than 5d daily for some Indian symbols
        hist = ticker.history(period="1mo", interval="1d", auto_adjust=False)

        if hist is None or hist.empty:
            logger.error("[YAHOO] empty history for %s", symbol)
            return None

        last = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) > 1 else None

        price = _safe_float(last.get("Close"))
        prev_close = _safe_float(prev.get("Close")) if prev is not None else None

        if price is None:
            logger.error("[YAHOO] no close price for %s", symbol)
            return None

        exchange, market, currency = _default_exchange_market_currency(symbol)

        return _build_quote(
            symbol=symbol,
            name=symbol,
            price=price,
            previous_close=prev_close,
            currency=currency,
            exchange=exchange,
            market=market,
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

    cached = _quote_cache.get(symbol)
    if cached is not None:
        return cached

    if _is_indian(symbol):
        quote = _get_upstox_quote(symbol)
        if quote:
            _quote_cache[symbol] = quote
            return quote

        logger.error("[QUOTE][INDIA] Upstox failed for %s", symbol)

        quote = _get_yahoo(symbol)
        if quote:
            _quote_cache[symbol] = quote
            return quote

        quote = _empty(symbol, source="india_all_failed")
        _quote_cache[symbol] = quote
        return quote

    quote = _get_finnhub(symbol)
    if quote:
        _quote_cache[symbol] = quote
        return quote

    logger.error("[QUOTE][US] Finnhub failed for %s", symbol)

    quote = _get_yahoo(symbol)
    if quote:
        _quote_cache[symbol] = quote
        return quote

    quote = _empty(symbol, source="us_all_failed")
    _quote_cache[symbol] = quote
    return quote


# backward compatibility
def get_upstox_quote(symbol: str) -> Dict[str, Any]:
    return get_quote(symbol)


def get_market_data_feed_authorize_url_v3() -> Optional[str]:
    """
    Helper for phase-2 websocket streaming.
    Upstox V3 returns a one-time authorized_redirect_uri for the market feed.
    """
    if not UPSTOX_TOKEN:
        logger.error("[UPSTOX][WS] access token missing")
        return None

    try:
        resp = _http.get(
            UPSTOX_WS_AUTHORIZE_V3,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {UPSTOX_TOKEN}",
            },
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        return ((data or {}).get("data") or {}).get("authorized_redirect_uri")
    except Exception:
        logger.exception("[UPSTOX][WS] authorize v3 failed")
        return None


def build_market_data_subscription(instrument_keys: Iterable[str], mode: str = "ltpc") -> dict:
    return {
        "guid": f"finpulse-{int(time.time() * 1000)}",
        "method": "sub",
        "data": {
            "mode": mode,
            "instrumentKeys": [k for k in instrument_keys if k],
        },
    }