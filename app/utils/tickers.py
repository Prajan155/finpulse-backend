def normalize_ticker(ticker: str) -> str:
    return ticker.strip().upper()

def apply_market_suffix(symbol: str, market: str) -> str:
    """
    market: US | NSE | BSE
    """
    symbol = normalize_ticker(symbol)
    market = market.strip().upper()

    if market == "US":
        return symbol
    if market == "NSE":
        return symbol if symbol.endswith(".NS") else f"{symbol}.NS"
    if market == "BSE":
        return symbol if symbol.endswith(".BO") else f"{symbol}.BO"
    return symbol