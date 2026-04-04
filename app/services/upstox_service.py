import logging
import requests

from app.core.config import settings

logger = logging.getLogger(__name__)


def _headers():
    return {
        "Authorization": f"Bearer {(settings.upstox_analytics_token or '').strip()}",
        "Accept": "application/json",
    }


def _to_upstox_symbol(symbol: str):
    symbol = (symbol or "").upper().strip()

    if symbol.endswith(".NS"):
        return f"NSE_EQ|{symbol[:-3]}"

    if symbol.endswith(".BO"):
        return f"BSE_EQ|{symbol[:-3]}"

    return None


def get_upstox_quote(symbol: str):
    instrument = _to_upstox_symbol(symbol)
    if not instrument:
        print(f"[UPSTOX] invalid symbol mapping for {symbol}")
        return None

    try:
        url = "https://api.upstox.com/v3/market-quote/ltp"
        params = {
            "instrument_key": instrument,
        }

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
        print(f"[UPSTOX] raw={resp.text[:1000]}")

        resp.raise_for_status()
        payload = resp.json() or {}

        quote = (payload.get("data") or {}).get(instrument)
        print(f"[UPSTOX] parsed_quote={quote}")

        if not quote:
            return None

        price = (
            quote.get("last_price")
            or quote.get("ltp")
            or quote.get("close")
        )

        prev_close = (
            quote.get("cp")
            or quote.get("prev_close")
            or quote.get("previous_close")
            or quote.get("close")
        )

        change = None
        change_percent = None

        if price is not None and prev_close not in (None, 0):
            change = float(price) - float(prev_close)
            change_percent = (change / float(prev_close)) * 100

        result = {
            "price": float(price) if price is not None else None,
            "change": float(change) if change is not None else None,
            "changePercent": float(change_percent) if change_percent is not None else None,
            "prevClose": float(prev_close) if prev_close is not None else None,
        }

        print(f"[UPSTOX] result={result}")
        return result

    except Exception as e:
        logger.exception("Upstox quote failed for %s: %s", symbol, e)
        return None