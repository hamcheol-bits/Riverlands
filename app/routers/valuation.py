"""
밸류에이션 API Router
app/routers/valuation.py

PER, PBR 기준 종목 스크리닝
"""
from fastapi import APIRouter, Depends, Query, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.services.valuation_service import get_valuation_service

router = APIRouter(prefix="/api/valuation", tags=["Valuation"])


@router.get("/{ticker}")
async def get_valuation(
        ticker: str,
        use_cache: bool = Query(True, description="캐시 사용 여부"),
        db: Session = Depends(get_db)
):
    """
    종목 밸류에이션 조회

    Examples:
        - GET /api/valuation/005930
        - GET /api/valuation/005930?use_cache=false  (실시간 계산)
    """
    service = get_valuation_service()
    valuation = service.get_valuation(db, ticker, use_cache)

    if not valuation:
        return {
            "ticker": ticker,
            "message": "No valuation data available"
        }

    return valuation


@router.post("/update/{ticker}")
async def update_valuation(
        ticker: str,
        db: Session = Depends(get_db)
):
    """
    단일 종목 밸류에이션 갱신

    Examples:
        - POST /api/valuation/update/005930
    """
    service = get_valuation_service()
    result = service.update_valuation_for_ticker(db, ticker)

    return result


@router.post("/update/batch")
async def batch_update_valuation(
        background_tasks: BackgroundTasks,
        limit: Optional[int] = Query(None, description="처리 종목 수 제한"),
        run_background: bool = Query(True, description="백그라운드 실행"),
        db: Session = Depends(get_db)
):
    """
    전체 종목 밸류에이션 갱신

    Examples:
        - POST /api/valuation/update/batch
        - POST /api/valuation/update/batch?limit=100
    """
    service = get_valuation_service()

    if run_background:
        background_tasks.add_task(
            service.update_all_valuations, db, limit
        )
        return {
            "status": "background_task_started",
            "message": f"Batch valuation update started (limit: {limit or 'all'})"
        }

    result = service.update_all_valuations(db, limit)
    return result


@router.get("/screen/undervalued")
async def screen_undervalued_stocks(
        max_per: float = Query(10.0, description="최대 PER"),
        max_pbr: float = Query(1.0, description="최대 PBR"),
        min_roe: float = Query(10.0, description="최소 ROE (%)"),
        limit: int = Query(50, ge=1, le=500, description="결과 개수"),
        db: Session = Depends(get_db)
):
    """
    저평가 종목 스크리닝

    기본 기준:
    - PER ≤ 10
    - PBR ≤ 1.0
    - ROE ≥ 10%

    Examples:
        - GET /api/valuation/screen/undervalued
        - GET /api/valuation/screen/undervalued?max_per=15&max_pbr=1.5&limit=100
    """
    service = get_valuation_service()

    stocks = service.screen_stocks(
        db,
        max_per=max_per,
        max_pbr=max_pbr,
        min_roe=min_roe,
        limit=limit
    )

    return {
        "criteria": {
            "max_per": max_per,
            "max_pbr": max_pbr,
            "min_roe": min_roe
        },
        "total": len(stocks),
        "items": stocks
    }


@router.get("/screen/custom")
async def screen_custom(
        min_per: Optional[float] = Query(None, description="최소 PER"),
        max_per: Optional[float] = Query(None, description="최대 PER"),
        min_pbr: Optional[float] = Query(None, description="최소 PBR"),
        max_pbr: Optional[float] = Query(None, description="최대 PBR"),
        min_roe: Optional[float] = Query(None, description="최소 ROE"),
        limit: int = Query(100, ge=1, le=500, description="결과 개수"),
        db: Session = Depends(get_db)
):
    """
    커스텀 밸류에이션 스크리닝

    Examples:
        - GET /api/valuation/screen/custom?min_per=5&max_per=15
        - GET /api/valuation/screen/custom?max_pbr=2.0&min_roe=15
    """
    service = get_valuation_service()

    stocks = service.screen_stocks(
        db,
        min_per=min_per,
        max_per=max_per,
        min_pbr=min_pbr,
        max_pbr=max_pbr,
        min_roe=min_roe,
        limit=limit
    )

    return {
        "criteria": {
            "min_per": min_per,
            "max_per": max_per,
            "min_pbr": min_pbr,
            "max_pbr": max_pbr,
            "min_roe": min_roe
        },
        "total": len(stocks),
        "items": stocks
    }


@router.get("/stats")
async def get_valuation_stats(
        db: Session = Depends(get_db)
):
    """
    밸류에이션 통계

    Examples:
        - GET /api/valuation/stats
    """
    from sqlalchemy import text, func

    # 캐시된 종목 수
    total_cached = db.execute(
        text("SELECT COUNT(*) FROM stock_valuation_cache")
    ).scalar()

    # PER 통계
    per_stats = db.execute(
        text("""
             SELECT AVG(per) as avg_per,
                    MIN(per) as min_per,
                    MAX(per) as max_per,
                    COUNT(*) as count
             FROM stock_valuation_cache
             WHERE per > 0 AND per < 100
             """)
    ).fetchone()

    # PBR 통계
    pbr_stats = db.execute(
        text("""
             SELECT AVG(pbr) as avg_pbr,
                    MIN(pbr) as min_pbr,
                    MAX(pbr) as max_pbr
             FROM stock_valuation_cache
             WHERE pbr > 0
               AND pbr < 10
             """)
    ).fetchone()

    return {
        "total_cached": total_cached,
        "per": {
            "average": round(float(per_stats.avg_per), 2) if per_stats.avg_per else None,
            "min": round(float(per_stats.min_per), 2) if per_stats.min_per else None,
            "max": round(float(per_stats.max_per), 2) if per_stats.max_per else None,
            "count": per_stats.count
        },
        "pbr": {
            "average": round(float(pbr_stats.avg_pbr), 2) if pbr_stats.avg_pbr else None,
            "min": round(float(pbr_stats.min_pbr), 2) if pbr_stats.min_pbr else None,
            "max": round(float(pbr_stats.max_pbr), 2) if pbr_stats.max_pbr else None
        }
    }