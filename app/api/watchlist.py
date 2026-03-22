from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.auth import get_current_user
from app.core.dependencies import get_db
from app.models.user import User
from app.services.watchlist_service import (
    add_to_watchlist,
    get_watchlist,
    remove_from_watchlist,
)

router = APIRouter(prefix="/watchlist", tags=["watchlist"])


@router.get("/")
def fetch_watchlist(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return get_watchlist(db, user_id=current_user.id)


@router.post("/")
def add_symbol(
    symbol: str = Query(..., description="Stock symbol to add"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not symbol.strip():
        raise HTTPException(status_code=400, detail="Symbol cannot be empty")

    return add_to_watchlist(db, symbol, user_id=current_user.id)


@router.delete("/{symbol}")
def delete_symbol(
    symbol: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return remove_from_watchlist(db, symbol, user_id=current_user.id)