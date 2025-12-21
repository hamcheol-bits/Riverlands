"""
Stock Price API Router
주가 데이터 조회, 수집
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.services.stock_price_service import get_stock_price_service

router = APIRouter(prefix="/api/prices", tags=["prices"])


@router.get("/{ticker}/latest")
async def get_latest_price(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    최신 주가 조회

    Args:
        ticker: 종목코드

    Examples:
        - GET /api/prices/005930/latest

    Returns:
        최신 주가 데이터
    """
    service = get_stock_price_service()
    price = service.get_latest_price(db, ticker)

    if not price:
        raise HTTPException(status_code=404, detail=f"No price data for {ticker}")

    return price.to_dict()


@router.get("/{ticker}/date/{date}")
async def get_price_by_date(
    ticker: str,
    date: str,
    db: Session = Depends(get_db)
):
    """
    특정 날짜 주가 조회

    Args:
        ticker: 종목코드
        date: 날짜 (YYYY-MM-DD 또는 YYYYMMDD)

    Examples:
        - GET /api/prices/005930/date/2024-12-20
        - GET /api/prices/005930/date/20241220

    Returns:
        해당 날짜 주가 데이터
    """
    service = get_stock_price_service()
    price = service.get_price_by_date(db, ticker, date)

    if not price:
        raise HTTPException(
            status_code=404,
            detail=f"No price data for {ticker} on {date}"
        )

    return price.to_dict()


@router.get("/{ticker}/range")
async def get_prices_by_range(
    ticker: str,
    start_date: Optional[str] = Query(None, description="시작일 (YYYYMMDD)"),
    end_date: Optional[str] = Query(None, description="종료일 (YYYYMMDD)"),
    limit: int = Query(100, ge=1, le=1000, description="결과 개수"),
    db: Session = Depends(get_db)
):
    """
    기간별 주가 조회

    Args:
        ticker: 종목코드
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
        limit: 결과 개수 제한

    Examples:
        - GET /api/prices/005930/range
        - GET /api/prices/005930/range?start_date=20240101&end_date=20241231
        - GET /api/prices/005930/range?start_date=20240101&limit=50

    Returns:
        기간별 주가 데이터
    """
    service = get_stock_price_service()
    prices = service.get_prices_by_range(db, ticker, start_date, end_date, limit)

    return {
        "ticker": ticker,
        "start_date": start_date,
        "end_date": end_date,
        "total": len(prices),
        "items": [price.to_dict() for price in prices]
    }


@router.get("/{ticker}/recent")
async def get_recent_prices(
    ticker: str,
    days: int = Query(30, ge=1, le=365, description="조회 일수"),
    db: Session = Depends(get_db)
):
    """
    최근 N일 주가 조회

    Args:
        ticker: 종목코드
        days: 조회 일수

    Examples:
        - GET /api/prices/005930/recent
        - GET /api/prices/005930/recent?days=60

    Returns:
        최근 N일 주가 데이터
    """
    service = get_stock_price_service()
    prices = service.get_recent_prices(db, ticker, days)

    return {
        "ticker": ticker,
        "days": days,
        "total": len(prices),
        "items": [price.to_dict() for price in prices]
    }


@router.post("/{ticker}/collect")
async def collect_prices(
    ticker: str,
    start_date: Optional[str] = Query(None, description="시작일 (YYYYMMDD)"),
    end_date: Optional[str] = Query(None, description="종료일 (YYYYMMDD)"),
    db: Session = Depends(get_db)
):
    """
    주가 수집

    Args:
        ticker: 종목코드
        start_date: 시작일 (기본: 100일 전)
        end_date: 종료일 (기본: 오늘)

    Examples:
        - POST /api/prices/005930/collect
        - POST /api/prices/005930/collect?start_date=20240101&end_date=20241231

    Returns:
        수집 결과
    """
    service = get_stock_price_service()
    result = await service.collect_and_save(db, ticker, start_date, end_date)

    return result


@router.post("/{ticker}/collect/incremental")
async def collect_prices_incremental(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    주가 증분 수집 (마지막 수집일 이후만)

    Args:
        ticker: 종목코드

    Examples:
        - POST /api/prices/005930/collect/incremental

    Returns:
        수집 결과
    """
    service = get_stock_price_service()
    result = await service.collect_incremental(db, ticker)

    return result


@router.get("/{ticker}/stats")
async def get_price_stats(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    주가 통계 정보

    Args:
        ticker: 종목코드

    Examples:
        - GET /api/prices/005930/stats

    Returns:
        주가 통계
    """
    service = get_stock_price_service()

    total = service.count_prices(db, ticker)
    latest = service.get_latest_price(db, ticker)
    recent_30 = service.get_recent_prices(db, ticker, 30)

    # 최근 30일 평균가 계산
    avg_price = None
    if recent_30:
        avg_price = sum(p.stck_clpr for p in recent_30) / len(recent_30)

    return {
        "ticker": ticker,
        "total_records": total,
        "latest_date": latest.stck_bsop_date.isoformat() if latest else None,
        "latest_price": float(latest.stck_clpr) if latest else None,
        "avg_price_30d": round(float(avg_price), 2) if avg_price else None,
        "records_30d": len(recent_30)
    }