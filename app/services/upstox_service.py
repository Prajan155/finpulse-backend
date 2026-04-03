import requests
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

def _headers():
    return {
        "Authorization": f"Bearer {(settings.upstox_analytics_token or '').strip()}",
        "Accept": "application/json",
    }

def _to_upstox_symbol(symbol: str):
    symbol = symbol.upper().strip()

    if symbol.endswith(".NS"):
        return f"NSE_EQ|{symbol.replace('.NS', '')}"

    if symbol.endswith(".BO"):
        return f"BSE_EQ|{symbol.replace('.BO', '')}"

    return None


def get_upstox_quote(symbol: str):
    instrument = _to_upstox_symbol(symbol)
    if not instrument:
        return None

    try:
        url = "https://api.upstox.com/v3/market-quote/ohlc"

        params = {
            "instrument_key": instrument,
            "interval": "1day"
        }

        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)

        print("UPSTOX STATUS:", resp.status_code)
        print("UPSTOX RAW:", resp.text[:500])

        resp.raise_for_status()
        data = resp.json()

        quote = data.get("data", {}).get(instrument)

        if not quote:
            print("UPSTOX: no quote found")
            return None

        live = quote.get("live_ohlc", {})
        prev = quote.get("prev_ohlc", {})

        price = live.get("close")
        prev_close = prev.get("close")

        change = None
        change_percent = None

        if price is not None and prev_close not in (None, 0):
            change = price - prev_close
            change_percent = (change / prev_close) * 100

        return {
            "price": price,
            "change": change,
            "changePercent": change_percent,
            "prevClose": prev_close,
        }

    except Exception as e:
        logger.exception(f"Upstox quote failed for {symbol}: {e}")
        return None