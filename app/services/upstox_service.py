import logging
import requests

from app.core.config import settings

logger = logging.getLogger(__name__)

_SEARCH_CACHE = {}


def _headers():
    return {
        "Authorization": f"Bearer {(settings.upstox_analytics_token or '').strip()}",
        "Accept": "application/json",
    }


def _pick_first_number(*values):
    for value in values:
        try:
            if value is None or value == "":
                continue
            return float(value)
        except Exception:
            continue
    return None


def _normalize_symbol(symbol: str) -> str:
    symbol = (symbol or "").upper().strip()
    if symbol.endswith(".NS") or symbol.endswith(".BO"):
        return symbol[:-3]
    return symbol


def _search_upstox_instrument(symbol: str) -> str | None:
    normalized = _normalize_symbol(symbol)

    if normalized in _SEARCH_CACHE:
        return _SEARCH_CACHE[normalized]

    try:
        url = "https://api.upstox.com/v1/instruments/search"
        params = {
            "query": normalized,
            "exchange": "NSE",
            "segment": "NSE_EQ",
            "instrument_type": "EQ",
        }

        resp = requests.get(url, headers=_headers(), params=params, timeout=12)

        print(f"[UPSTOX SEARCH] status={resp.status_code}")
        print(f"[UPSTOX SEARCH] raw={resp.text[:1200]}")

        resp.raise_for_status()
        payload = resp.json() or {}

        results = payload.get("data") or []
        if not isinstance(results, list):
            results = []

        chosen_key = None
        for item in results:
            trading_symbol = str(item.get("trading_symbol") or "").upper().strip()
            segment = str(item.get("segment") or "").upper().strip()
            instrument_type = str(item.get("instrument_type") or "").upper().strip()
            instrument_key = item.get("instrument_key")

            if (
                trading_symbol == normalized
                and segment == "NSE_EQ"
                and instrument_type == "EQ"
                and instrument_key
            ):
                chosen_key = instrument_key
                break

        if not chosen_key and results:
            for item in results:
                instrument_key = item.get("instrument_key")
                if instrument_key:
                    chosen_key = instrument_key
                    break

        _SEARCH_CACHE[normalized] = chosen_key
        print(f"[UPSTOX SEARCH] resolved {normalized} -> {chosen_key}")
        return chosen_key

    except Exception as e:
        logger.exception("Upstox instrument search failed for %s: %s", symbol, e)
        _SEARCH_CACHE[normalized] = None
        return None


def get_upstox_quote(symbol: str):
    instrument = _search_upstox_instrument(symbol)

    if not instrument:
        print(f"[UPSTOX] instrument not found for {symbol}")
        return None

    try:
        url = "https://api.upstox.com/v3/market-quote/ltp"
        params = {"instrument_key": instrument}

        print(f"[UPSTOX] symbol={symbol}")
        print(f"[UPSTOX] instrument={instrument}")
        print(f"[UPSTOX] token_present={bool((settings.upstox_analytics_token or '').strip())}")

        resp = requests.get(
            url,
            headers=_headers(),
            params=params,
            timeout=12,
        )

        print(f"[UPSTOX] status={resp.status_code}")
        print(f"[UPSTOX] raw={resp.text[:1200]}")

        resp.raise_for_status()
        payload = resp.json() or {}

        quote = (payload.get("data") or {}).get(instrument)
        print(f"[UPSTOX DEBUG] full quote = {quote}")

        if not quote:
            return None

        price = _pick_first_number(
            quote.get("last_price"),
            quote.get("ltp"),
            quote.get("close"),
            quote.get("price"),
        )

        prev_close = _pick_first_number(
            quote.get("cp"),
            quote.get("prev_close"),
            quote.get("previous_close"),
            quote.get("close"),
        )

        change = None
        change_percent = None

        if price is not None and prev_close not in (None, 0):
            change = price - prev_close
            change_percent = (change / prev_close) * 100

        result = {
            "price": price,
            "change": change,
            "changePercent": change_percent,
            "prevClose": prev_close,
        }

        print(f"[UPSTOX] result={result}")
        return result

    except Exception as e:
        logger.exception("Upstox quote failed for %s: %s", symbol, e)
        return None