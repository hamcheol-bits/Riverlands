"""
Valuation API Router
밸류에이션 지표 조회 (PER TTM, EPS TTM 등)
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.services.valuation_service import get_valuation_service

router = APIRouter(prefix="/api/valuations", tags=["valuations"])


@router.get("/{ticker}")
async def get_valuation_metrics(
    ticker: str,
    include_quarterly_trend: bool = Query(False, description="분기별 EPS 추이 포함 여부"),
    quarterly_limit: int = Query(8, ge=1, le=20, description="분기별 추이 조회 개수"),
    as_of_date: Optional[str] = Query(None, description="기준일 (YYYYMM)"),
    db: Session = Depends(get_db)
):
    """
    종목 밸류에이션 지표 조회 (통합 엔드포인트)

    기본 제공:
    - 현재가 정보
    - TTM 지표 (매출, 영업이익, 순이익, EPS, PER)
    - 연간 지표 (비교용)

    선택 제공 (파라미터):
    - 분기별 EPS 추이 (include_quarterly_trend=true)

    Args:
        ticker: 종목코드
        include_quarterly_trend: 분기별 EPS 추이 포함 여부
        quarterly_limit: 분기별 추이 조회 개수 (기본 8분기)
        as_of_date: 기준일 (YYYYMM), 미지정시 최신 분기

    Examples:
        - GET /api/valuations/005930
          → 기본 TTM + 연간 데이터

        - GET /api/valuations/005930?include_quarterly_trend=true
          → TTM + 연간 + 분기별 EPS 추이

        - GET /api/valuations/005930?as_of_date=202509
          → 2025년 9월 기준 TTM 데이터

        - GET /api/valuations/005930?include_quarterly_trend=true&quarterly_limit=12
          → 12분기 EPS 추이 포함

    Returns:
        {
            "ticker": "005930",
            "stock_name": "삼성전자",
            "current_price": 75000,
            "price_date": "2025-09-30",
            "ttm": {
                "base_quarter": "202509",
                "quarters": ["202509", "202506", "202503", "202412"],
                "sales": 258340000000000,
                "operating_income": 45230000000000,
                "net_income": 29834000000000,
                "eps": 5234.56,
                "per": 14.33
            },
            "annual": {
                "year": "202412",
                "sales": 240120000000000,
                "operating_income": 42100000000000,
                "net_income": 27890000000000,
                "eps": 4892.00,
                "per": 15.34
            },
            "quarterly_trend": [  // include_quarterly_trend=true인 경우만
                {
                    "quarter": "202509",
                    "net_income": 7450000000000,
                    "eps": 1308.45,
                    "roe": 9.2
                },
                ...
            ]
        }
    """
    service = get_valuation_service()

    # 기본 TTM 요약 조회
    result = service.get_ttm_summary(db, ticker, as_of_date)

    if result["status"] == "error":
        raise HTTPException(status_code=404, detail=result["message"])

    # 분기별 EPS 추이 (선택사항)
    if include_quarterly_trend:
        quarterly_trend = service.get_quarterly_eps_trend(db, ticker, quarterly_limit)
        result["quarterly_trend"] = quarterly_trend

    return result