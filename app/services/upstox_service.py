import logging
import requests

from app.core.config import settings

logger = logging.getLogger(__name__)

# quick manual map for the symbols you need right now
_MANUAL_INSTRUMENT_MAP = {
    "RELIANCE": "NSE_EQ|INE002A01018",
}

def _headers():
    return {
        "Authorization": f"Bearer {(settings.upstox_analytics_token or '').strip()}",
        "Accept": "application/json",
    }

def _resolve_instrument(symbol: str):
    symbol = (symbol or "").upper().strip()

    if symbol.endswith(".NS"):
        symbol = symbol[:-3]
    elif symbol.endswith(".BO"):
        symbol = symbol[:-3]

    key = _MANUAL_INSTRUMENT_MAP.get(symbol)
    print(f"[UPSTOX] resolved {symbol} -> {key}")
    return key

def _pick_first_number(*values):
    for value in values:
        try:
            if value is None or value == "":
                continue
            return float(value)
        except Exception:
            continue
    return None

def get_upstox_quote(symbol: str):
    instrument = _resolve_instrument(symbol)

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