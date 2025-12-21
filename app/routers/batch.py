"""
Batch API Router
시장 단위 배치 처리
"""
from fastapi import APIRouter, Depends, Query, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Optional, List

from app.core.database import get_db
from app.services.batch_service import get_batch_service

router = APIRouter(prefix="/api/batch", tags=["batch"])


# ============================================================
# 시장별 종목 정보 배치
# ============================================================

@router.post("/stocks/{market}")
async def batch_collect_stocks(
        market: str,
        use_api: bool = Query(True, description="KIS API 사용 여부"),
        date: Optional[str] = Query(None, description="기준일 (YYYYMMDD)"),
        limit: Optional[int] = Query(None, description="수집 제한 (테스트용)"),
        db: Session = Depends(get_db)
):
    """
    시장별 종목 정보 배치 수집

    Args:
        market: KOSPI, KOSDAQ, ALL
        use_api: KIS API 사용 여부
        date: 기준일 (YYYYMMDD)
        limit: 수집 제한

    Examples:
        - POST /api/batch/stocks/KOSPI
        - POST /api/batch/stocks/KOSDAQ?use_api=true
        - POST /api/batch/stocks/ALL?limit=10

    Returns:
        배치 수집 결과
    """
    market = market.upper()
    if market not in ["KOSPI", "KOSDAQ", "ALL"]:
        return {"status": "error", "message": "market must be KOSPI, KOSDAQ, or ALL"}

    service = get_batch_service()
    result = await service.batch_collect_stocks(db, market, use_api, date, limit)

    return result


# ============================================================
# 시장별 주가 배치
# ============================================================

@router.post("/prices/{market}")
async def batch_collect_prices(
        market: str,
        background_tasks: BackgroundTasks,
        mode: str = Query("incremental", description="incremental 또는 full"),
        start_date: Optional[str] = Query(None, description="시작일 (YYYYMMDD)"),
        end_date: Optional[str] = Query(None, description="종료일 (YYYYMMDD)"),
        limit: Optional[int] = Query(None, description="처리 종목 수 제한"),
        run_background: bool = Query(False, description="백그라운드 실행 여부"),
        db: Session = Depends(get_db)
):
    """
    시장별 주가 배치 수집

    Args:
        market: KOSPI, KOSDAQ, ALL
        mode: incremental (증분) 또는 full (전체)
        start_date: 시작일 (full 모드)
        end_date: 종료일 (full 모드)
        limit: 처리 종목 수 제한
        run_background: 백그라운드 실행 여부

    Examples:
        - POST /api/batch/prices/KOSPI
        - POST /api/batch/prices/KOSDAQ?mode=full
        - POST /api/batch/prices/ALL?run_background=true

    Returns:
        배치 수집 결과
    """
    market = market.upper()
    if market not in ["KOSPI", "KOSDAQ", "ALL"]:
        return {"status": "error", "message": "market must be KOSPI, KOSDAQ, or ALL"}

    if mode not in ["incremental", "full"]:
        return {"status": "error", "message": "mode must be incremental or full"}

    service = get_batch_service()

    # 백그라운드 실행
    if run_background:
        background_tasks.add_task(
            service.batch_collect_prices,
            db, market, mode, start_date, end_date, limit
        )
        return {
            "status": "background_task_started",
            "market": market,
            "mode": mode,
            "message": f"Price batch collection started in background for {market}"
        }

    # 동기 실행
    result = await service.batch_collect_prices(db, market, mode, start_date, end_date, limit)
    return result


# ============================================================
# 시장별 재무제표 배치
# ============================================================

@router.post("/financials/{market}")
async def batch_collect_financials(
        market: str,
        background_tasks: BackgroundTasks,
        period_type: str = Query("0", description="0(연간), 1(분기)"),
        limit: Optional[int] = Query(None, description="처리 종목 수 제한"),
        run_background: bool = Query(False, description="백그라운드 실행 여부"),
        db: Session = Depends(get_db)
):
    """
    시장별 재무제표 배치 수집

    Args:
        market: KOSPI, KOSDAQ, ALL
        period_type: 0(연간), 1(분기)
        limit: 처리 종목 수 제한
        run_background: 백그라운드 실행 여부

    Examples:
        - POST /api/batch/financials/KOSPI
        - POST /api/batch/financials/KOSDAQ?period_type=1
        - POST /api/batch/financials/ALL?run_background=true

    Returns:
        배치 수집 결과
    """
    market = market.upper()
    if market not in ["KOSPI", "KOSDAQ", "ALL"]:
        return {"status": "error", "message": "market must be KOSPI, KOSDAQ, or ALL"}

    if period_type not in ["0", "1"]:
        return {"status": "error", "message": "period_type must be 0 or 1"}

    service = get_batch_service()

    # 백그라운드 실행
    if run_background:
        background_tasks.add_task(
            service.batch_collect_financials,
            db, market, period_type, limit
        )
        period_char = "Y" if period_type == "0" else "Q"
        return {
            "status": "background_task_started",
            "market": market,
            "period_type": period_char,
            "message": f"Financial batch collection started in background for {market}"
        }

    # 동기 실행
    result = await service.batch_collect_financials(db, market, period_type, limit)
    return result


# ============================================================
# 통합 배치 (종목 + 주가 + 재무제표)
# ============================================================

@router.post("/all/{market}")
async def batch_collect_all(
        market: str,
        background_tasks: BackgroundTasks,
        include_stocks: bool = Query(True, description="종목 정보 수집"),
        include_prices: bool = Query(True, description="주가 수집"),
        include_financials: bool = Query(True, description="재무제표 수집"),
        price_mode: str = Query("incremental", description="주가 수집 모드"),
        limit: Optional[int] = Query(None, description="처리 종목 수 제한"),
        run_background: bool = Query(False, description="백그라운드 실행 여부"),
        db: Session = Depends(get_db)
):
    """
    시장별 전체 데이터 통합 수집

    종목 정보 + 주가 + 연간 재무제표 + 분기 재무제표

    Args:
        market: KOSPI, KOSDAQ, ALL
        include_stocks: 종목 정보 수집 여부
        include_prices: 주가 수집 여부
        include_financials: 재무제표 수집 여부
        price_mode: incremental 또는 full
        limit: 처리 종목 수 제한
        run_background: 백그라운드 실행 여부

    Examples:
        - POST /api/batch/all/KOSPI
        - POST /api/batch/all/ALL?run_background=true
        - POST /api/batch/all/KOSDAQ?limit=10

    Returns:
        통합 수집 결과
    """
    market = market.upper()
    if market not in ["KOSPI", "KOSDAQ", "ALL"]:
        return {"status": "error", "message": "market must be KOSPI, KOSDAQ, or ALL"}

    if price_mode not in ["incremental", "full"]:
        return {"status": "error", "message": "price_mode must be incremental or full"}

    service = get_batch_service()

    # 백그라운드 실행
    if run_background:
        background_tasks.add_task(
            service.batch_collect_all,
            db, market, include_stocks, include_prices,
            include_financials, price_mode, limit
        )
        return {
            "status": "background_task_started",
            "market": market,
            "message": f"Full batch collection started in background for {market}"
        }

    # 동기 실행
    result = await service.batch_collect_all(
        db, market, include_stocks, include_prices,
        include_financials, price_mode, limit
    )
    return result


# ============================================================
# 여러 티커 일괄 처리
# ============================================================

@router.post("/tickers")
async def batch_collect_tickers(
        tickers: List[str] = Query(..., description="종목코드 리스트"),
        include_stocks: bool = Query(True, description="종목 정보 수집"),
        include_prices: bool = Query(True, description="주가 수집"),
        include_financials: bool = Query(True, description="재무제표 수집"),
        price_mode: str = Query("incremental", description="주가 수집 모드"),
        db: Session = Depends(get_db)
):
    """
    여러 티커 일괄 수집

    Args:
        tickers: 종목코드 리스트
        include_stocks: 종목 정보 수집 여부
        include_prices: 주가 수집 여부
        include_financials: 재무제표 수집 여부
        price_mode: incremental 또는 full

    Examples:
        - POST /api/batch/tickers?tickers=005930&tickers=000660
        - POST /api/batch/tickers?tickers=005930&tickers=035720&price_mode=full

    Returns:
        일괄 수집 결과
    """
    if price_mode not in ["incremental", "full"]:
        return {"status": "error", "message": "price_mode must be incremental or full"}

    service = get_batch_service()

    result = await service.batch_collect_tickers(
        db, tickers, include_stocks, include_prices,
        include_financials, price_mode
    )
    return result


# ============================================================
# 배치 상태 조회
# ============================================================

@router.get("/status")
async def get_batch_status(db: Session = Depends(get_db)):
    """
    배치 처리 상태 조회

    전체 데이터 수집 현황

    Examples:
        - GET /api/batch/status

    Returns:
        데이터 수집 현황
    """
    from app.models.stock import Stock
    from app.models.stock_price import StockPrice
    from app.models.financial_statement import FinancialStatement
    from sqlalchemy import func

    # 종목 통계
    total_stocks = db.query(Stock).filter(Stock.is_active == True).count()
    kospi_stocks = db.query(Stock).filter(
        Stock.is_active == True,
        Stock.mrkt_ctg_cls_code == "KOSPI"
    ).count()
    kosdaq_stocks = db.query(Stock).filter(
        Stock.is_active == True,
        Stock.mrkt_ctg_cls_code == "KOSDAQ"
    ).count()

    # 주가 데이터 통계
    stocks_with_price = db.query(StockPrice.ticker).distinct().count()
    latest_price_date = db.query(func.max(StockPrice.stck_bsop_date)).scalar()
    total_price_records = db.query(StockPrice).count()

    # 재무제표 통계
    stocks_with_annual = db.query(FinancialStatement.ticker).filter(
        FinancialStatement.period_type == "Y"
    ).distinct().count()

    stocks_with_quarterly = db.query(FinancialStatement.ticker).filter(
        FinancialStatement.period_type == "Q"
    ).distinct().count()

    latest_annual = db.query(func.max(FinancialStatement.stac_yymm)).filter(
        FinancialStatement.period_type == "Y"
    ).scalar()

    latest_quarterly = db.query(func.max(FinancialStatement.stac_yymm)).filter(
        FinancialStatement.period_type == "Q"
    ).scalar()

    return {
        "stocks": {
            "total": total_stocks,
            "kospi": kospi_stocks,
            "kosdaq": kosdaq_stocks
        },
        "prices": {
            "stocks_with_data": stocks_with_price,
            "coverage_rate": f"{stocks_with_price / total_stocks * 100:.1f}%" if total_stocks > 0 else "0%",
            "total_records": total_price_records,
            "latest_date": str(latest_price_date) if latest_price_date else None
        },
        "annual_financials": {
            "stocks_with_data": stocks_with_annual,
            "coverage_rate": f"{stocks_with_annual / total_stocks * 100:.1f}%" if total_stocks > 0 else "0%",
            "latest_period": latest_annual
        },
        "quarterly_financials": {
            "stocks_with_data": stocks_with_quarterly,
            "coverage_rate": f"{stocks_with_quarterly / total_stocks * 100:.1f}%" if total_stocks > 0 else "0%",
            "latest_period": latest_quarterly
        }
    }