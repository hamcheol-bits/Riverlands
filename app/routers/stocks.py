"""
종목 정보 수집 API 라우터
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.services.stock_collector import get_stock_collector

router = APIRouter(prefix="/api/stocks", tags=["stocks"])


@router.post("/collect")
async def collect_stocks(
    market: str = Query("ALL", description="시장 구분 (KOSPI/KOSDAQ/ALL)"),
    use_api: bool = Query(True, description="KIS API로 상세 정보 조회 여부"),
    date: Optional[str] = Query(None, description="기준일 (YYYYMMDD)"),
    db: Session = Depends(get_db)
):
    """
    종목 수집
    
    - **market**: KOSPI, KOSDAQ, ALL (기본값: ALL)
    - **use_api**: KIS API로 상세 정보 조회 여부 (기본값: True)
    - **date**: 기준일 (YYYYMMDD, 기본값: 오늘)
    
    예시:
    - `POST /api/stocks/collect` (전체 시장, API 사용)
    - `POST /api/stocks/collect?market=KOSPI` (KOSPI만)
    - `POST /api/stocks/collect?market=KOSDAQ&use_api=false` (KOSDAQ, API 미사용)
    - `POST /api/stocks/collect?market=ALL&date=20250101` (전체, 특정 날짜)
    """
    market = market.upper()
    
    if market not in ["KOSPI", "KOSDAQ", "ALL"]:
        return {
            "status": "error",
            "message": "market must be KOSPI, KOSDAQ, or ALL"
        }
    
    collector = get_stock_collector()
    
    if market == "ALL":
        # 전체 시장 수집 (KOSPI + KOSDAQ)
        result = await collector.collect_all_markets(db, use_api, date)
    else:
        # 특정 시장만 수집
        result = await collector.collect_market_stocks(db, market, use_api, date)
    
    return result


@router.put("/update/{ticker}")
async def update_stock_info(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    특정 종목 정보 업데이트 (KIS API 사용)
    
    - **ticker**: 종목코드 (6자리)
    
    예시:
    - `PUT /api/stocks/update/005930`
    """
    collector = get_stock_collector()
    result = await collector.update_stock_info(db, ticker)
    return result


@router.get("/list")
async def list_stocks(
    market: Optional[str] = Query(None, description="시장 필터 (KOSPI/KOSDAQ/ALL)"),
    is_active: bool = Query(True, description="활성 종목만 조회"),
    skip: int = Query(0, ge=0, description="Skip"),
    limit: int = Query(100, ge=1, le=1000, description="Limit"),
    db: Session = Depends(get_db)
):
    """
    종목 목록 조회
    
    - **market**: 시장 필터 (KOSPI/KOSDAQ/ALL, 기본값: ALL)
    - **is_active**: 활성 종목만 조회
    - **skip**: 페이지네이션 Skip
    - **limit**: 페이지네이션 Limit
    
    예시:
    - `GET /api/stocks/list` (전체)
    - `GET /api/stocks/list?market=KOSPI&limit=50` (KOSPI만 50개)
    - `GET /api/stocks/list?market=KOSDAQ` (KOSDAQ만)
    """
    from app.models.stock import Stock
    
    query = db.query(Stock)
    
    if market and market.upper() != "ALL":
        query = query.filter(Stock.mrkt_ctg_cls_code == market.upper())
    
    if is_active:
        query = query.filter(Stock.is_active == True)
    
    total = query.count()
    stocks = query.offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "items": [stock.to_dict() for stock in stocks]
    }


@router.get("/{ticker}")
async def get_stock_info(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    특정 종목 정보 조회
    
    - **ticker**: 종목코드 (6자리)
    
    예시:
    - `GET /api/stocks/005930`
    """
    from app.models.stock import Stock
    
    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
    
    if not stock:
        return {
            "status": "error",
            "message": f"Stock {ticker} not found"
        }
    
    return stock.to_dict()
