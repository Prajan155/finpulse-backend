import requests
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

HEADERS = {
    "Authorization": f"Bearer {settings.upstox_analytics_token}",
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
        url = f"{settings.upstox_base_url}/market-quote/ltp"
        params = {"instrument_key": instrument}

        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        quote = data.get("data", {}).get(instrument)

        if not quote:
            return None

        ltp = quote.get("last_price")
        prev_close = quote.get("ohlc", {}).get("close")

        change = None
        change_percent = None

        if ltp and prev_close:
            change = ltp - prev_close
            change_percent = (change / prev_close) * 100

        return {
            "price": ltp,
            "change": change,
            "changePercent": change_percent,
            "prevClose": prev_close,
        }

    except Exception as e:
        logger.exception(f"Upstox quote failed for {symbol}: {e}")
        return None