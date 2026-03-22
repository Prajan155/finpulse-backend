from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.dependencies import get_db
from app.core.cache import cache

router = APIRouter(tags=["Health"])


@router.get("/health", summary="Basic health check")
def health():
    return {
        "ok": True,
        "status": "healthy",
    }


@router.get("/health/db", summary="Database health check")
def health_db(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {
        "ok": True,
        "status": "healthy",
        "database": "connected",
    }


@router.get("/health/cache", summary="Cache health check")
def health_cache():
    return {
        "ok": True,
        "status": "healthy",
        "cache_available": cache is not None,
    }