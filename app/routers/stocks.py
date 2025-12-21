"""
Stock API Router
종목 정보 조회, 수집, 관리
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.services.stock_service import get_stock_service

router = APIRouter(prefix="/api/stocks", tags=["stocks"])


@router.get("/{ticker}")
async def get_stock(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    단일 종목 조회

    Args:
        ticker: 종목코드 (6자리)

    Examples:
        - GET /api/stocks/005930

    Returns:
        종목 정보
    """
    service = get_stock_service()
    stock = service.get_stock(db, ticker)

    if not stock:
        raise HTTPException(status_code=404, detail=f"Stock {ticker} not found")

    return stock.to_dict()


@router.get("/")
async def list_stocks(
    market: Optional[str] = Query(None, description="KOSPI, KOSDAQ, ALL"),
    is_active: bool = Query(True, description="활성 종목만 조회"),
    skip: int = Query(0, ge=0, description="Skip"),
    limit: int = Query(100, ge=1, le=1000, description="Limit"),
    db: Session = Depends(get_db)
):
    """
    종목 목록 조회

    Args:
        market: 시장 구분 (KOSPI/KOSDAQ/ALL)
        is_active: 활성 종목만 조회
        skip: 페이지네이션 skip
        limit: 페이지네이션 limit

    Examples:
        - GET /api/stocks/
        - GET /api/stocks/?market=KOSPI
        - GET /api/stocks/?market=KOSDAQ&limit=50

    Returns:
        종목 목록
    """
    service = get_stock_service()

    stocks = service.get_stocks(db, market, is_active, skip, limit)
    total = service.count_stocks(db, market, is_active)

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "market": market,
        "items": [stock.to_dict() for stock in stocks]
    }


@router.get("/search/")
async def search_stocks(
    keyword: str = Query(..., description="검색 키워드 (종목명 또는 코드)"),
    market: Optional[str] = Query(None, description="시장 구분"),
    limit: int = Query(20, ge=1, le=100, description="결과 개수"),
    db: Session = Depends(get_db)
):
    """
    종목명 검색

    Args:
        keyword: 검색 키워드
        market: 시장 구분
        limit: 결과 개수 제한

    Examples:
        - GET /api/stocks/search/?keyword=삼성
        - GET /api/stocks/search/?keyword=005930
        - GET /api/stocks/search/?keyword=전자&market=KOSPI

    Returns:
        검색 결과
    """
    service = get_stock_service()

    stocks = service.search_stocks(db, keyword, market, limit)

    return {
        "keyword": keyword,
        "market": market,
        "total": len(stocks),
        "items": [stock.to_dict() for stock in stocks]
    }


@router.post("/{ticker}/collect")
async def collect_stock(
    ticker: str,
    use_api: bool = Query(True, description="KIS API 사용 여부"),
    db: Session = Depends(get_db)
):
    """
    단일 종목 수집

    Args:
        ticker: 종목코드
        use_api: KIS API로 상세 정보 조회 여부

    Examples:
        - POST /api/stocks/005930/collect
        - POST /api/stocks/005930/collect?use_api=true

    Returns:
        수집 결과
    """
    service = get_stock_service()
    result = await service.collect_stock(db, ticker, use_api)

    return result


@router.put("/{ticker}/deactivate")
async def deactivate_stock(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    종목 비활성화 (상장폐지 등)

    Args:
        ticker: 종목코드

    Examples:
        - PUT /api/stocks/005930/deactivate

    Returns:
        처리 결과
    """
    service = get_stock_service()
    success = service.deactivate_stock(db, ticker)

    if not success:
        raise HTTPException(status_code=404, detail=f"Stock {ticker} not found")

    return {
        "ticker": ticker,
        "status": "deactivated"
    }


@router.get("/stats/summary")
async def get_stocks_summary(
    db: Session = Depends(get_db)
):
    """
    종목 통계 요약

    Examples:
        - GET /api/stocks/stats/summary

    Returns:
        시장별 종목 통계
    """
    service = get_stock_service()

    total = service.count_stocks(db, market=None, is_active=True)
    kospi = service.count_stocks(db, market="KOSPI", is_active=True)
    kosdaq = service.count_stocks(db, market="KOSDAQ", is_active=True)

    return {
        "total": total,
        "kospi": kospi,
        "kosdaq": kosdaq,
        "inactive": service.count_stocks(db, market=None, is_active=False)
    }