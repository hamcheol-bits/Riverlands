"""
Financial API Router (개선 버전)
재무제표 데이터 조회, 수집

주요 변경사항:
- 분기 데이터 수집시 year 파라미터 추가
- 연도 단위로 분기 실적 수집
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.services.financial_service import get_financial_service

router = APIRouter(prefix="/api/financials", tags=["financials"])


@router.get("/{ticker}/latest")
async def get_latest_financial(
    ticker: str,
    period_type: str = Query("Y", description="Y(연간), Q(분기)"),
    db: Session = Depends(get_db)
):
    """최신 재무제표 조회"""
    period_type = period_type.upper()
    if period_type not in ["Y", "Q"]:
        raise HTTPException(status_code=400, detail="period_type must be Y or Q")

    service = get_financial_service()
    financial = service.get_latest_financial(db, ticker, period_type)

    if not financial:
        raise HTTPException(
            status_code=404,
            detail=f"No financial data for {ticker} ({period_type})"
        )

    return financial.to_dict()


@router.get("/{ticker}/period/{stac_yymm}")
async def get_financial_by_period(
    ticker: str,
    stac_yymm: str,
    period_type: str = Query("Y", description="Y(연간), Q(분기)"),
    db: Session = Depends(get_db)
):
    """특정 기간 재무제표 조회"""
    period_type = period_type.upper()
    if period_type not in ["Y", "Q"]:
        raise HTTPException(status_code=400, detail="period_type must be Y or Q")

    service = get_financial_service()
    financial = service.get_financial_by_period(db, ticker, stac_yymm, period_type)

    if not financial:
        raise HTTPException(
            status_code=404,
            detail=f"No financial data for {ticker} {stac_yymm} ({period_type})"
        )

    return financial.to_dict()


@router.get("/{ticker}/list")
async def get_financials(
    ticker: str,
    period_type: Optional[str] = Query(None, description="Y(연간), Q(분기), None(전체)"),
    limit: int = Query(10, ge=1, le=100, description="조회 개수"),
    db: Session = Depends(get_db)
):
    """재무제표 목록 조회"""
    if period_type and period_type.upper() not in ["Y", "Q"]:
        raise HTTPException(status_code=400, detail="period_type must be Y or Q")

    service = get_financial_service()
    financials = service.get_financials(db, ticker, period_type, limit)

    return {
        "ticker": ticker,
        "period_type": period_type,
        "total": len(financials),
        "items": [f.to_dict() for f in financials]
    }


@router.post("/{ticker}/collect")
async def collect_financials(
    ticker: str,
    period_type: str = Query("0", description="0(연간), 1(분기)"),
    year: Optional[int] = Query(None, ge=2000, le=2030, description="분기 수집시 연도 (예: 2024, 2025)"),
    db: Session = Depends(get_db)
):
    """
    재무제표 수집

    Args:
        ticker: 종목코드
        period_type: 0(연간), 1(분기)
        year: 분기 데이터 수집시 연도 (예: 2024)
              - 미지정시 현재 연도
              - 현재 연도면 현재 분기까지만 수집
              - 과거 연도면 Q1~Q4 모두 수집

    Examples:
        - POST /api/financials/005930/collect?period_type=0
          → 연간 데이터 수집

        - POST /api/financials/005930/collect?period_type=1
          → 현재 연도 분기 데이터 수집 (예: 2025 Q1~Q3)

        - POST /api/financials/005930/collect?period_type=1&year=2024
          → 2024년 분기 데이터 수집 (Q1~Q4)

        - POST /api/financials/005930/collect?period_type=1&year=2025
          → 2025년 분기 데이터 수집 (현재 분기까지)

    Returns:
        수집 결과
    """
    if period_type not in ["0", "1"]:
        raise HTTPException(status_code=400, detail="period_type must be 0 or 1")

    service = get_financial_service()
    result = await service.collect_and_save(db, ticker, period_type, year)

    return result


@router.post("/{ticker}/collect/all")
async def collect_all_financials(
    ticker: str,
    year: Optional[int] = Query(None, ge=2000, le=2030, description="분기 데이터 수집시 연도"),
    db: Session = Depends(get_db)
):
    """
    재무제표 전체 수집 (연간 + 분기)

    Args:
        ticker: 종목코드
        year: 분기 데이터 수집시 연도 (미지정시 현재 연도)

    Examples:
        - POST /api/financials/005930/collect/all
          → 연간 + 현재 연도 분기

        - POST /api/financials/005930/collect/all?year=2024
          → 연간 + 2024년 분기

    Returns:
        수집 결과
    """
    service = get_financial_service()

    # 연간 수집
    annual_result = await service.collect_and_save(db, ticker, "0")

    # 분기 수집
    quarterly_result = await service.collect_and_save(db, ticker, "1", year)

    return {
        "ticker": ticker,
        "annual": annual_result,
        "quarterly": quarterly_result
    }


@router.get("/{ticker}/summary")
async def get_financial_summary(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    재무제표 요약 정보

    Args:
        ticker: 종목코드

    Examples:
        - GET /api/financials/005930/summary

    Returns:
        재무제표 요약 (최신 연간/분기)
    """
    service = get_financial_service()

    # 최신 연간
    latest_annual = service.get_latest_financial(db, ticker, "Y")

    # 최신 분기
    latest_quarterly = service.get_latest_financial(db, ticker, "Q")

    summary = {
        "ticker": ticker,
        "latest_annual": None,
        "latest_quarterly": None
    }

    if latest_annual:
        summary["latest_annual"] = {
            "stac_yymm": latest_annual.stac_yymm,
            "total_aset": latest_annual.total_aset,
            "total_lblt": latest_annual.total_lblt,
            "total_cptl": latest_annual.total_cptl,
            "sale_account": latest_annual.sale_account,
            "bsop_prti": latest_annual.bsop_prti,
            "thtr_ntin": latest_annual.thtr_ntin,
            "roe_val": float(latest_annual.roe_val) if latest_annual.roe_val else None,
            "eps": float(latest_annual.eps) if latest_annual.eps else None,
            "bps": float(latest_annual.bps) if latest_annual.bps else None,
            "lblt_rate": float(latest_annual.lblt_rate) if latest_annual.lblt_rate else None
        }

    if latest_quarterly:
        summary["latest_quarterly"] = {
            "stac_yymm": latest_quarterly.stac_yymm,
            "sale_account": latest_quarterly.sale_account,
            "bsop_prti": latest_quarterly.bsop_prti,
            "thtr_ntin": latest_quarterly.thtr_ntin,
            "roe_val": float(latest_quarterly.roe_val) if latest_quarterly.roe_val else None
        }

    return summary


@router.get("/{ticker}/stats")
async def get_financial_stats(
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    재무제표 통계 정보

    Args:
        ticker: 종목코드

    Examples:
        - GET /api/financials/005930/stats

    Returns:
        재무제표 통계
    """
    service = get_financial_service()

    annual_count = service.count_financials(db, ticker, "Y")
    quarterly_count = service.count_financials(db, ticker, "Q")

    latest_annual = service.get_latest_financial(db, ticker, "Y")
    latest_quarterly = service.get_latest_financial(db, ticker, "Q")

    return {
        "ticker": ticker,
        "annual_records": annual_count,
        "quarterly_records": quarterly_count,
        "latest_annual_period": latest_annual.stac_yymm if latest_annual else None,
        "latest_quarterly_period": latest_quarterly.stac_yymm if latest_quarterly else None
    }