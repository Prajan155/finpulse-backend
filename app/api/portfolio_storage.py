from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.auth import get_current_user
from app.core.dependencies import get_db
from app.models.user import User
from app.models.portfolio import PortfolioHolding
from app.schemas.portfolio_storage import (
    PortfolioHoldingCreate,
    PortfolioHoldingUpdate,
    PortfolioHoldingOut,
)

router = APIRouter(prefix="/portfolio", tags=["portfolio-storage"])


@router.get("/holdings", response_model=list[PortfolioHoldingOut])
def get_holdings(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    holdings = (
        db.query(PortfolioHolding)
        .filter(PortfolioHolding.user_id == current_user.id)
        .order_by(PortfolioHolding.id.desc())
        .all()
    )
    return holdings


@router.post("/holdings", response_model=PortfolioHoldingOut)
def add_holding(
    payload: PortfolioHoldingCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    symbol = payload.symbol.upper().strip()

    existing = (
        db.query(PortfolioHolding)
        .filter(
            PortfolioHolding.user_id == current_user.id,
            PortfolioHolding.symbol == symbol,
        )
        .first()
    )

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Holding already exists for this user",
        )

    holding = PortfolioHolding(
        user_id=current_user.id,
        symbol=symbol,
        quantity=payload.quantity,
        average_price=payload.average_price,
    )
    db.add(holding)
    db.commit()
    db.refresh(holding)
    return holding


@router.put("/holdings/{holding_id}", response_model=PortfolioHoldingOut)
def update_holding(
    holding_id: int,
    payload: PortfolioHoldingUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    holding = (
        db.query(PortfolioHolding)
        .filter(
            PortfolioHolding.id == holding_id,
            PortfolioHolding.user_id == current_user.id,
        )
        .first()
    )

    if not holding:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Holding not found",
        )

    holding.quantity = payload.quantity
    holding.average_price = payload.average_price
    db.commit()
    db.refresh(holding)
    return holding


@router.delete("/holdings/{holding_id}")
def delete_holding(
    holding_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    holding = (
        db.query(PortfolioHolding)
        .filter(
            PortfolioHolding.id == holding_id,
            PortfolioHolding.user_id == current_user.id,
        )
        .first()
    )

    if not holding:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Holding not found",
        )

    db.delete(holding)
    db.commit()
    return {"message": "Holding deleted successfully"}