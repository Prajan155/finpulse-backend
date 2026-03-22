from sqlalchemy.orm import Session

from app.models.watchlist import Watchlist


def get_watchlist(db: Session, user_id: int):
    items = (
        db.query(Watchlist)
        .filter(Watchlist.user_id == user_id)
        .order_by(Watchlist.created_at.desc())
        .all()
    )

    return [
        {
            "symbol": item.symbol,
            "added_at": item.created_at,
        }
        for item in items
    ]


def add_to_watchlist(db: Session, symbol: str, user_id: int):
    symbol = symbol.upper().strip()

    existing = (
        db.query(Watchlist)
        .filter(
            Watchlist.user_id == user_id,
            Watchlist.symbol == symbol,
        )
        .first()
    )
    if existing:
        return {"message": "Already in watchlist", "symbol": existing.symbol}

    item = Watchlist(
        user_id=user_id,
        symbol=symbol,
    )
    db.add(item)
    db.commit()
    db.refresh(item)

    return {
        "message": "Added",
        "symbol": item.symbol,
    }


def remove_from_watchlist(db: Session, symbol: str, user_id: int):
    symbol = symbol.upper().strip()

    item = (
        db.query(Watchlist)
        .filter(
            Watchlist.user_id == user_id,
            Watchlist.symbol == symbol,
        )
        .first()
    )

    if not item:
        return {"message": "Not found", "symbol": symbol}

    db.delete(item)
    db.commit()

    return {"message": "Removed", "symbol": symbol}