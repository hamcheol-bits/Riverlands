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
# Batch API Router - 재무제표 수집 부분 개선
# 시장별 분기 데이터를 연도 단위로 수집
# ============================================================

@router.post("/financials/{market}")
async def batch_collect_financials(
        market: str,
        background_tasks: BackgroundTasks,
        period_type: str = Query("0", description="0(연간), 1(분기)"),
        year: Optional[int] = Query(None, ge=2000, le=2030, description="분기 수집시 연도 (예: 2024)"),
        limit: Optional[int] = Query(None, description="처리 종목 수 제한"),
        run_background: bool = Query(False, description="백그라운드 실행 여부"),
        db: Session = Depends(get_db)
):
    """
    시장별 재무제표 배치 수집

    Args:
        market: KOSPI, KOSDAQ, ALL
        period_type: 0(연간), 1(분기)
        year: 분기 수집시 연도 (예: 2024)
              - 미지정시 현재 연도
              - 현재 연도면 현재 분기까지만
              - 과거 연도면 Q1~Q4 모두
              - 연간 데이터는 무시됨
        limit: 처리 종목 수 제한
        run_background: 백그라운드 실행 여부

    Examples:
        - POST /api/batch/financials/KOSPI?period_type=0
          → KOSPI 연간 데이터 수집

        - POST /api/batch/financials/KOSPI?period_type=1
          → KOSPI 현재 연도 분기 데이터 수집

        - POST /api/batch/financials/KOSPI?period_type=1&year=2024
          → KOSPI 2024년 분기 데이터 수집 (Q1~Q4)

        - POST /api/batch/financials/KOSDAQ?period_type=1&year=2024&limit=10
          → KOSDAQ 10종목만 2024년 분기 수집

        - POST /api/batch/financials/ALL?period_type=1&year=2025&run_background=true
          → 전체 시장 2025년 분기 백그라운드 수집

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
            db, market, period_type, year, limit
        )
        period_char = "Y" if period_type == "0" else "Q"

        msg_parts = [f"Financial batch collection started for {market} ({period_char})"]
        if period_type == "1" and year:
            msg_parts.append(f"year {year}")

        return {
            "status": "background_task_started",
            "market": market,
            "period_type": period_char,
            "year": year if period_type == "1" else None,
            "message": " ".join(msg_parts)
        }

    # 동기 실행
    result = await service.batch_collect_financials(db, market, period_type, year, limit)
    return result


@router.post("/all/{market}")
async def batch_collect_all(
        market: str,
        background_tasks: BackgroundTasks,
        include_stocks: bool = Query(True, description="종목 정보 수집"),
        include_prices: bool = Query(True, description="주가 수집"),
        include_financials: bool = Query(True, description="재무제표 수집"),
        price_mode: str = Query("incremental", description="주가 수집 모드"),
        financial_year: Optional[int] = Query(None, ge=2000, le=2030, description="분기 재무제표 수집시 연도"),
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
        financial_year: 분기 재무제표 수집시 연도 (미지정시 현재 연도)
        limit: 처리 종목 수 제한
        run_background: 백그라운드 실행 여부

    Examples:
        - POST /api/batch/all/KOSPI
          → KOSPI 전체 (연간 + 현재 연도 분기)

        - POST /api/batch/all/KOSPI?financial_year=2024
          → KOSPI 전체 (연간 + 2024년 분기)

        - POST /api/batch/all/ALL?run_background=true
          → 전체 시장 백그라운드 수집

        - POST /api/batch/all/KOSDAQ?limit=10&financial_year=2024
          → KOSDAQ 10종목만 (연간 + 2024년 분기)

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
            include_financials, price_mode, financial_year, limit
        )

        msg_parts = [f"Full batch collection started for {market}"]
        if financial_year:
            msg_parts.append(f"(quarterly: {financial_year})")

        return {
            "status": "background_task_started",
            "market": market,
            "financial_year": financial_year,
            "message": " ".join(msg_parts)
        }

    # 동기 실행
    result = await service.batch_collect_all(
        db, market, include_stocks, include_prices,
        include_financials, price_mode, financial_year, limit
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

    # 배당 통계
    from app.models.dividend import Dividend
    stocks_with_dividend = db.query(Dividend.ticker).distinct().count()
    total_dividend_records = db.query(Dividend).count()
    latest_dividend_date = db.query(func.max(Dividend.record_date)).scalar()

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
        },
        "dividends": {
            "stocks_with_data": stocks_with_dividend,
            "coverage_rate": f"{stocks_with_dividend / total_stocks * 100:.1f}%" if total_stocks > 0 else "0%",
            "total_records": total_dividend_records,
            "latest_date": str(latest_dividend_date) if latest_dividend_date else None
        }
    }


# ============================================================
# 시장별 배당 배치
# ============================================================

@router.post("/dividends/{market}")
async def batch_collect_dividends(
        market: str,
        background_tasks: BackgroundTasks,
        year: Optional[int] = Query(None, ge=2000, le=2030, description="조회 연도 (예: 2025, 미지정시 최근 3년)"),
        incremental: bool = Query(False, description="증분 수집 여부"),
        limit: Optional[int] = Query(None, description="처리 종목 수 제한"),
        run_background: bool = Query(False, description="백그라운드 실행 여부"),
        db: Session = Depends(get_db)
):
    """
    시장별 배당 정보 배치 수집

    Args:
        market: KOSPI, KOSDAQ, ALL
        year: 조회 연도 (예: 2025, 미지정시 최근 3년)
        incremental: 증분 수집 여부 (기본값: False)
        limit: 처리 종목 수 제한
        run_background: 백그라운드 실행 여부

    Examples:
        - POST /api/batch/dividends/KOSPI
          → KOSPI 전체, 최근 3년
        - POST /api/batch/dividends/KOSPI?year=2025
          → KOSPI 전체, 2025년만 (20250101~20251231)
        - POST /api/batch/dividends/KOSDAQ?year=2024&limit=10
          → KOSDAQ 10종목, 2024년만
        - POST /api/batch/dividends/ALL?incremental=true&run_background=true
          → 전체 시장, 증분 수집, 백그라운드

    Returns:
        배치 수집 결과

    Note:
        - year 지정: 해당 연도 1월 1일 ~ 12월 31일
        - year 미지정: 현재부터 과거 3년
        - incremental=true: 각 종목의 마지막 배당 기준일 이후만 조회
        - incremental=false: 모든 종목에 대해 지정된 year 조회
        - 종목코드는 자동으로 9자리로 변환
        - 결산배당과 중간배당 모두 수집 (GB1=0)
        - API 호출 제한 고려하여 적절한 딜레이 적용
    """
    market = market.upper()
    if market not in ["KOSPI", "KOSDAQ", "ALL"]:
        return {"status": "error", "message": "market must be KOSPI, KOSDAQ, or ALL"}

    service = get_batch_service()

    # 백그라운드 실행
    if run_background:
        background_tasks.add_task(
            service.batch_collect_dividends,
            db, market, year, incremental, limit
        )

        msg_parts = [f"Dividend batch collection started for {market}"]
        if year:
            msg_parts.append(f"year {year}")
        if incremental:
            msg_parts.append("(incremental)")

        return {
            "status": "background_task_started",
            "market": market,
            "year": year,
            "incremental": incremental,
            "message": " ".join(msg_parts)
        }

    # 동기 실행
    result = await service.batch_collect_dividends(db, market, year, incremental, limit)
    return result

@router.post("/investment-opinions/{market}")
async def batch_collect_investment_opinions(
        market: str,
        background_tasks: BackgroundTasks,
        start_date: Optional[str] = Query(None, description="시작일 (YYYYMMDD), 기본값: 6개월 전"),
        end_date: Optional[str] = Query(None, description="종료일 (YYYYMMDD), 기본값: 오늘"),
        limit: Optional[int] = Query(None, description="처리 종목 수 제한"),
        run_background: bool = Query(False, description="백그라운드 실행 여부"),
        db: Session = Depends(get_db)
):
    """
    시장별 투자의견 컨센서스 배치 수집

    Args:
        market: KOSPI, KOSDAQ, ALL
        start_date: 시작일 (YYYYMMDD), 기본값: 6개월 전
        end_date: 종료일 (YYYYMMDD), 기본값: 오늘
        limit: 처리 종목 수 제한
        run_background: 백그라운드 실행 여부

    Examples:
        - POST /api/batch/investment-opinions/KOSPI
          → KOSPI 전체, 최근 6개월
        - POST /api/batch/investment-opinions/KOSPI?start_date=20240101&end_date=20241231
          → KOSPI 전체, 2024년
        - POST /api/batch/investment-opinions/KOSDAQ?limit=10
          → KOSDAQ 10종목, 최근 6개월
        - POST /api/batch/investment-opinions/ALL?run_background=true
          → 전체 시장, 백그라운드

    Returns:
        배치 수집 결과

    Note:
        - start_date 미지정: 6개월 전부터
        - end_date 미지정: 오늘까지
        - 증권사별 최신 투자의견만 유지 (UPSERT)
        - API 호출 제한 고려하여 적절한 딜레이 적용
    """
    market = market.upper()
    if market not in ["KOSPI", "KOSDAQ", "ALL"]:
        return {"status": "error", "message": "market must be KOSPI, KOSDAQ, or ALL"}

    service = get_batch_service()

    # 백그라운드 실행
    if run_background:
        background_tasks.add_task(
            service.batch_collect_investment_opinions,
            db, market, start_date, end_date, limit
        )

        msg_parts = [f"Investment opinion batch collection started for {market}"]
        if start_date or end_date:
            msg_parts.append(f"({start_date or '6mo ago'} ~ {end_date or 'today'})")

        return {
            "status": "background_task_started",
            "market": market,
            "start_date": start_date,
            "end_date": end_date,
            "message": " ".join(msg_parts)
        }

    # 동기 실행
    result = await service.batch_collect_investment_opinions(db, market, start_date, end_date, limit)
    return result