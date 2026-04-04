import logging
import requests

from app.core.config import settings

logger = logging.getLogger(__name__)

# 🔥 CACHE FOR SYMBOL → INSTRUMENT KEY
_INSTRUMENT_MAP = {}


def _headers():
    return {
        "Authorization": f"Bearer {(settings.upstox_analytics_token or '').strip()}",
        "Accept": "application/json",
    }


# 🔥 DOWNLOAD INSTRUMENT MASTER
def _load_instruments():
    global _INSTRUMENT_MAP

    if _INSTRUMENT_MAP:
        return

    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json"
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()

        data = resp.json()

        for item in data:
            symbol = item.get("trading_symbol")
            key = item.get("instrument_key")

            if symbol and key:
                _INSTRUMENT_MAP[symbol.upper()] = key

        print(f"[UPSTOX] Loaded {len(_INSTRUMENT_MAP)} instruments")

    except Exception as e:
        logger.exception("Failed to load Upstox instruments: %s", e)


def _resolve_instrument(symbol: str):
    symbol = symbol.upper().strip()

    if symbol.endswith(".NS"):
        symbol = symbol[:-3]

    _load_instruments()

    key = _INSTRUMENT_MAP.get(symbol)

    print(f"[UPSTOX] resolved {symbol} → {key}")

    return key


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

        resp = requests.get(
            url,
            headers=_headers(),
            params=params,
            timeout=12,
        )

        print(f"[UPSTOX] status={resp.status_code}")
        print(f"[UPSTOX] raw={resp.text[:1000]}")

        resp.raise_for_status()
        payload = resp.json() or {}

        quote = (payload.get("data") or {}).get(instrument)

        print(f"[UPSTOX DEBUG] quote={quote}")

        if not quote:
            return None

        price = quote.get("last_price")
        prev_close = quote.get("cp")

        change = None
        change_percent = None

        if price and prev_close:
            change = price - prev_close
            change_percent = (change / prev_close) * 100

        return {
            "price": price,
            "change": change,
            "changePercent": change_percent,
            "prevClose": prev_close,
        }

    except Exception as e:
        logger.exception("Upstox quote failed for %s: %s", symbol, e)
        return None