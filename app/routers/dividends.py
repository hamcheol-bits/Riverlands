"""
Dividend API Router
배당 정보 조회, 수집

KIS API: 예탁원정보(배당일정) [국내주식-145]
TR_ID: HHKDB669102C0

API Parameters:
- CTS: 연속조회 키 (공백)
- GB1: 조회구분 (0:전체, 1:결산배당, 2:중간배당)
- F_DT: 조회시작일 (YYYYMMDD, 자동계산)
- T_DT: 조회종료일 (YYYYMMDD, 자동계산)
- SHT_CD: 종목코드 (9자리, 자동변환)
- HIGH_GB: 고배당여부 (공백)
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.services.dividend_service import get_dividend_service

router = APIRouter(prefix="/api/dividends", tags=["dividends"])


@router.get("/{ticker}/latest")
async def get_latest_dividend(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    최신 배당 정보 조회

    Args:
        ticker: 종목코드

    Examples:
        - GET /api/dividends/005930/latest

    Returns:
        최신 배당 정보
    """
    service = get_dividend_service()
    dividend = service.get_latest_dividend(db, ticker)

    if not dividend:
        raise HTTPException(status_code=404, detail=f"No dividend data for {ticker}")

    return dividend.to_dict()


@router.get("/{ticker}/date/{record_date}")
async def get_dividend_by_date(
    ticker: str,
    record_date: str,
    divi_kind: Optional[str] = Query(None, description="배당종류"),
    db: Session = Depends(get_db)
):
    """
    특정 날짜 배당 정보 조회

    Args:
        ticker: 종목코드
        record_date: 기준일 (YYYY-MM-DD)
        divi_kind: 배당종류 (결산배당/중간배당)

    Examples:
        - GET /api/dividends/005930/date/2024-12-31
        - GET /api/dividends/005930/date/2024-12-31?divi_kind=결산배당

    Returns:
        해당 날짜 배당 정보
    """
    service = get_dividend_service()
    dividend = service.get_dividend_by_date(db, ticker, record_date, divi_kind)

    if not dividend:
        raise HTTPException(
            status_code=404,
            detail=f"No dividend data for {ticker} on {record_date}"
        )

    return dividend.to_dict()


@router.get("/{ticker}/list")
async def get_dividends(
    ticker: str,
    limit: int = Query(10, ge=1, le=100, description="조회 개수"),
    db: Session = Depends(get_db)
):
    """
    배당 목록 조회

    Args:
        ticker: 종목코드
        limit: 조회 개수

    Examples:
        - GET /api/dividends/005930/list
        - GET /api/dividends/005930/list?limit=20

    Returns:
        배당 목록
    """
    service = get_dividend_service()
    dividends = service.get_dividends(db, ticker, limit)

    return {
        "ticker": ticker,
        "total": len(dividends),
        "items": [d.to_dict() for d in dividends]
    }


@router.post("/{ticker}/collect")
async def collect_dividends(
    ticker: str,
    year: Optional[int] = Query(None, ge=2000, le=2030, description="조회 연도 (예: 2025, 미지정시 최근 3년)"),
    incremental: bool = Query(False, description="증분 수집 여부"),
    db: Session = Depends(get_db)
):
    """
    배당 정보 수집

    Args:
        ticker: 종목코드 (6자리, 예: 005930)
        year: 조회 연도 (예: 2025, 미지정시 최근 3년)
        incremental: 증분 수집 여부 (기본값: False)

    Examples:
        - POST /api/dividends/005930/collect
          → 최근 3년치 수집
        - POST /api/dividends/005930/collect?year=2025
          → 2025년 배당만 수집 (20250101~20251231)
        - POST /api/dividends/005930/collect?year=2024
          → 2024년 배당만 수집 (20240101~20241231)
        - POST /api/dividends/005930/collect?incremental=true
          → 마지막 배당 이후만 수집

    Returns:
        수집 결과

    Note:
        - year 지정: 해당 연도 1월 1일 ~ 12월 31일
        - year 미지정: 현재부터 과거 3년
        - incremental=true: 마지막 배당 기준일 이후 자동 계산
        - GB1=0 (전체 배당: 결산배당 + 중간배당)
        - 중복 데이터는 DB에서 자동 제거 (Upsert)
    """
    service = get_dividend_service()

    if incremental:
        # 증분 수집: 마지막 기준일 이후 자동 계산
        result = await service.collect_incremental(db, ticker)
    else:
        # 전체 수집: 지정된 year 또는 최근 3년
        result = await service.collect_and_save(db, ticker, year)

    return result


@router.get("/{ticker}/stats")
async def get_dividend_stats(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    배당 통계 정보

    Args:
        ticker: 종목코드

    Examples:
        - GET /api/dividends/005930/stats

    Returns:
        배당 통계
    """
    service = get_dividend_service()

    total = service.count_dividends(db, ticker)
    latest = service.get_latest_dividend(db, ticker)
    all_dividends = service.get_dividends(db, ticker, limit=100)

    # 평균 배당률 계산
    avg_divi_rate = None
    total_divi_amt = 0

    if all_dividends:
        valid_rates = [d.divi_rate for d in all_dividends if d.divi_rate]
        if valid_rates:
            avg_divi_rate = sum(valid_rates) / len(valid_rates)

        valid_amts = [d.per_sto_divi_amt for d in all_dividends if d.per_sto_divi_amt]
        if valid_amts:
            total_divi_amt = sum(valid_amts)

    return {
        "ticker": ticker,
        "total_records": total,
        "latest_record_date": latest.record_date.isoformat() if latest else None,
        "latest_divi_amt": float(latest.per_sto_divi_amt) if latest and latest.per_sto_divi_amt else None,
        "latest_divi_rate": float(latest.divi_rate) if latest and latest.divi_rate else None,
        "avg_divi_rate": round(float(avg_divi_rate), 2) if avg_divi_rate else None,
        "total_divi_amt": round(float(total_divi_amt), 2) if total_divi_amt else None,
        "is_high_dividend": latest.high_divi_gb == "Y" if latest else False
    }


@router.get("/high-dividend/list")
async def get_high_dividend_stocks(
    limit: int = Query(20, ge=1, le=100, description="조회 개수"),
    db: Session = Depends(get_db)
):
    """
    고배당 종목 목록 조회

    Args:
        limit: 조회 개수

    Examples:
        - GET /api/dividends/high-dividend/list
        - GET /api/dividends/high-dividend/list?limit=50

    Returns:
        고배당 종목 목록
    """
    service = get_dividend_service()
    dividends = service.get_high_dividend_stocks(db, limit)

    return {
        "total": len(dividends),
        "items": [d.to_dict() for d in dividends]
    }


@router.get("/{ticker}/summary")
async def get_dividend_summary(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    배당 요약 정보

    Args:
        ticker: 종목코드

    Examples:
        - GET /api/dividends/005930/summary

    Returns:
        배당 요약 (최근 3년)
    """
    service = get_dividend_service()

    dividends = service.get_dividends(db, ticker, limit=10)

    if not dividends:
        raise HTTPException(
            status_code=404,
            detail=f"No dividend data for {ticker}"
        )

    summary = {
        "ticker": ticker,
        "total_records": len(dividends),
        "recent_dividends": []
    }

    for d in dividends[:5]:  # 최근 5개만
        summary["recent_dividends"].append({
            "record_date": d.record_date.isoformat() if d.record_date else None,
            "divi_kind": d.divi_kind,
            "per_sto_divi_amt": float(d.per_sto_divi_amt) if d.per_sto_divi_amt else None,
            "divi_rate": float(d.divi_rate) if d.divi_rate else None,
            "divi_pay_dt": d.divi_pay_dt.isoformat() if d.divi_pay_dt else None
        })

    return summary