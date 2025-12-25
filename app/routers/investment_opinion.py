"""
Investment Opinion Router
투자의견 컨센서스 API 엔드포인트
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.investment_opinion_service import get_investment_opinion_service
from app.models.investment_opinion import InvestmentOpinion

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/investment-opinions", tags=["Investment Opinions"])


@router.get("/{ticker}")
async def get_opinions_by_ticker(
        ticker: str,
        db: Session = Depends(get_db)
):
    """
    특정 종목의 모든 증권사 투자의견 조회

    - **ticker**: 종목코드 (예: 005930)
    """
    service = get_investment_opinion_service()
    opinions = service.get_opinions_by_ticker(db, ticker)

    if not opinions:
        raise HTTPException(status_code=404, detail=f"No opinions found for ticker {ticker}")

    return {
        "ticker": ticker,
        "total": len(opinions),
        "items": [
            {
                "ticker": op.ticker,
                "mbcr_name": op.mbcr_name,
                "stck_bsop_date": op.stck_bsop_date,
                "invt_opnn": op.invt_opnn,
                "invt_opnn_cls_code": op.invt_opnn_cls_code,
                "rgbf_invt_opnn": op.rgbf_invt_opnn,
                "rgbf_invt_opnn_cls_code": op.rgbf_invt_opnn_cls_code,
                "hts_goal_prc": op.hts_goal_prc,
                "created_at": op.created_at.isoformat() if op.created_at else None,
                "updated_at": op.updated_at.isoformat() if op.updated_at else None,
            }
            for op in opinions
        ]
    }


@router.get("/{ticker}/{mbcr_name}")
async def get_opinion_by_ticker_and_firm(
        ticker: str,
        mbcr_name: str,
        db: Session = Depends(get_db)
):
    """
    특정 종목 + 증권사의 투자의견 조회

    - **ticker**: 종목코드
    - **mbcr_name**: 증권사명 (예: 한국투자증권)
    """
    service = get_investment_opinion_service()
    opinion = service.get_opinion_by_ticker_and_firm(db, ticker, mbcr_name)

    if not opinion:
        raise HTTPException(
            status_code=404,
            detail=f"No opinion found for {ticker} from {mbcr_name}"
        )

    return {
        "ticker": opinion.ticker,
        "mbcr_name": opinion.mbcr_name,
        "stck_bsop_date": opinion.stck_bsop_date,
        "invt_opnn": opinion.invt_opnn,
        "invt_opnn_cls_code": opinion.invt_opnn_cls_code,
        "rgbf_invt_opnn": opinion.rgbf_invt_opnn,
        "rgbf_invt_opnn_cls_code": opinion.rgbf_invt_opnn_cls_code,
        "hts_goal_prc": opinion.hts_goal_prc,
        "created_at": opinion.created_at.isoformat() if opinion.created_at else None,
        "updated_at": opinion.updated_at.isoformat() if opinion.updated_at else None,
    }


@router.post("/{ticker}/collect")
async def collect_investment_opinions(
        ticker: str,
        start_date: Optional[str] = Query(None, description="시작일 (YYYYMMDD), 기본값: 6개월 전"),
        end_date: Optional[str] = Query(None, description="종료일 (YYYYMMDD), 기본값: 오늘"),
        db: Session = Depends(get_db)
):
    """
    특정 종목의 투자의견 수집

    - **ticker**: 종목코드
    - **start_date**: 시작일 (YYYYMMDD), 미입력시 6개월 전
    - **end_date**: 종료일 (YYYYMMDD), 미입력시 오늘

    증권사별로 최신 투자의견만 유지됩니다 (증분 처리)
    """
    try:
        service = get_investment_opinion_service()
        result = await service.collect_investment_opinions(db, ticker, start_date, end_date)
        return result
    except Exception as e:
        logger.error(f"Error collecting opinions for {ticker}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
async def get_all_opinions(
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
        db: Session = Depends(get_db)
):
    """
    모든 투자의견 조회 (페이지네이션)

    - **skip**: 건너뛸 개수
    - **limit**: 조회할 개수 (최대 1000)
    """
    service = get_investment_opinion_service()
    opinions = service.get_all_opinions(db, skip, limit)

    return {
        "total": len(opinions),
        "skip": skip,
        "limit": limit,
        "items": [
            {
                "ticker": op.ticker,
                "mbcr_name": op.mbcr_name,
                "stck_bsop_date": op.stck_bsop_date,
                "invt_opnn": op.invt_opnn,
                "hts_goal_prc": op.hts_goal_prc,
                "updated_at": op.updated_at.isoformat() if op.updated_at else None,
            }
            for op in opinions
        ]
    }


@router.delete("/{ticker}/{mbcr_name}")
async def delete_opinion(
        ticker: str,
        mbcr_name: str,
        db: Session = Depends(get_db)
):
    """
    특정 투자의견 삭제

    - **ticker**: 종목코드
    - **mbcr_name**: 증권사명
    """
    service = get_investment_opinion_service()
    success = service.delete_opinion(db, ticker, mbcr_name)

    if not success:
        raise HTTPException(
            status_code=404,
            detail=f"Opinion not found for {ticker} from {mbcr_name}"
        )

    return {"message": "Investment opinion deleted successfully"}


@router.delete("/{ticker}")
async def delete_all_opinions_by_ticker(
        ticker: str,
        db: Session = Depends(get_db)
):
    """
    특정 종목의 모든 투자의견 삭제

    - **ticker**: 종목코드
    """
    service = get_investment_opinion_service()
    deleted_count = service.delete_all_opinions_by_ticker(db, ticker)

    return {
        "message": f"Deleted {deleted_count} opinions for ticker {ticker}",
        "deleted_count": deleted_count
    }