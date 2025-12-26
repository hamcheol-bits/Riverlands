"""
네이버증권 리서치 API Router
app/routers/naver_research.py
"""
from fastapi import APIRouter, Depends, Query, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import datetime, timedelta

from app.core.database import get_db
from app.services.naver_research_service import get_naver_research_service

router = APIRouter(prefix="/api/naver-research", tags=["Naver Research"])


# ============================================================
# 수집 엔드포인트
# ============================================================

@router.post("/collect/incremental")
async def collect_incremental(
    background_tasks: BackgroundTasks,
    days: int = Query(1, ge=1, le=30, description="수집 기간 (일)"),
    categories: Optional[List[str]] = Query(None, description="카테고리"),
    auto_download: bool = Query(False, description="자동 PDF 다운로드"),
    run_background: bool = Query(False, description="백그라운드 실행"),
    db: Session = Depends(get_db)
):
    """
    증분 수집 (최근 N일)

    Examples:
        - POST /api/naver-research/collect/incremental?days=1
        - POST /api/naver-research/collect/incremental?days=7&categories=company
        - POST /api/naver-research/collect/incremental?days=3&auto_download=true
    """
    service = get_naver_research_service()

    if run_background:
        background_tasks.add_task(
            service.collect_incremental, db, days, categories, auto_download
        )
        return {
            "status": "background_task_started",
            "message": f"Incremental collection started (last {days} days)"
        }

    result = await service.collect_incremental(db, days, categories, auto_download)
    return result


@router.post("/collect/category/{category}")
async def collect_by_category(
    category: str,
    background_tasks: BackgroundTasks,
    days: int = Query(7, ge=1, le=90, description="수집 기간 (일)"),
    max_pages: int = Query(10, ge=1, le=50, description="최대 페이지"),
    auto_download: bool = Query(False, description="자동 PDF 다운로드"),
    run_background: bool = Query(False, description="백그라운드 실행"),
    db: Session = Depends(get_db)
):
    """
    특정 카테고리 수집

    Args:
        category: market/invest/company/industry/economy/debenture

    Examples:
        - POST /api/naver-research/collect/category/company?days=7
        - POST /api/naver-research/collect/category/industry?max_pages=20
    """
    valid_categories = ["market", "invest", "company", "industry", "economy", "debenture"]
    if category not in valid_categories:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category. Must be one of: {valid_categories}"
        )

    service = get_naver_research_service()
    start_date = datetime.now() - timedelta(days=days)

    if run_background:
        background_tasks.add_task(
            service.collect_by_category, db, category, start_date, max_pages, auto_download
        )
        return {
            "status": "background_task_started",
            "category": category,
            "message": f"Collection started for {category}"
        }

    result = await service.collect_by_category(db, category, start_date, max_pages, auto_download)
    return result


@router.post("/collect/ticker/{ticker}")
async def collect_by_ticker(
    ticker: str,
    background_tasks: BackgroundTasks,
    days: int = Query(30, ge=1, le=365, description="수집 기간 (일)"),
    auto_download: bool = Query(False, description="자동 PDF 다운로드"),
    run_background: bool = Query(False, description="백그라운드 실행"),
    db: Session = Depends(get_db)
):
    """
    특정 종목 리포트 수집

    Examples:
        - POST /api/naver-research/collect/ticker/005930?days=30
        - POST /api/naver-research/collect/ticker/035720?auto_download=true
    """
    service = get_naver_research_service()

    if run_background:
        background_tasks.add_task(
            service.collect_by_ticker, db, ticker, days, auto_download
        )
        return {
            "status": "background_task_started",
            "ticker": ticker,
            "message": f"Collection started for {ticker}"
        }

    result = await service.collect_by_ticker(db, ticker, days, auto_download)
    return result


# ============================================================
# PDF 다운로드 엔드포인트
# ============================================================

@router.post("/download/{report_id}")
async def download_pdf(
    report_id: str,
    db: Session = Depends(get_db)
):
    """
    특정 리포트 PDF 다운로드

    Examples:
        - POST /api/naver-research/download/naver_미래에셋_20241220_company_abc123
    """
    service = get_naver_research_service()
    success = await service.download_pdf(db, report_id)

    if success:
        return {
            "status": "success",
            "report_id": report_id,
            "message": "PDF downloaded successfully"
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to download PDF")


@router.post("/download/batch")
async def batch_download_pdfs(
    background_tasks: BackgroundTasks,
    limit: int = Query(100, ge=1, le=1000, description="다운로드 개수"),
    broker: Optional[str] = Query(None, description="특정 증권사만"),
    days: Optional[int] = Query(None, description="최근 N일만"),
    run_background: bool = Query(True, description="백그라운드 실행"),
    db: Session = Depends(get_db)
):
    """
    미다운로드 PDF 배치 다운로드

    Examples:
        - POST /api/naver-research/download/batch?limit=50
        - POST /api/naver-research/download/batch?broker=미래에셋증권&days=7
    """
    service = get_naver_research_service()

    if run_background:
        background_tasks.add_task(
            service.batch_download_pdfs, db, limit, broker, days
        )
        return {
            "status": "background_task_started",
            "message": f"Batch download started (limit: {limit})"
        }

    result = await service.batch_download_pdfs(db, limit, broker, days)
    return result


# ============================================================
# 조회 엔드포인트
# ============================================================

@router.get("/reports/recent")
async def get_recent_reports(
    days: int = Query(7, ge=1, le=90, description="조회 기간 (일)"),
    limit: int = Query(100, ge=1, le=500, description="결과 개수"),
    category: Optional[str] = Query(None, description="카테고리"),
    db: Session = Depends(get_db)
):
    """
    최근 리포트 조회

    Examples:
        - GET /api/naver-research/reports/recent?days=7
        - GET /api/naver-research/reports/recent?days=30&category=company
    """
    service = get_naver_research_service()
    reports = service.get_recent_reports(db, days, limit, category)

    return {
        "days": days,
        "category": category,
        "total": len(reports),
        "items": [report.to_dict(include_relations=True) for report in reports]
    }


@router.get("/reports/ticker/{ticker}")
async def get_reports_by_ticker(
    ticker: str,
    days: int = Query(30, ge=1, le=365, description="조회 기간 (일)"),
    include_non_main: bool = Query(True, description="주요 종목 아닌 리포트도 포함"),
    db: Session = Depends(get_db)
):
    """
    종목별 리포트 조회

    Examples:
        - GET /api/naver-research/reports/ticker/005930?days=30
        - GET /api/naver-research/reports/ticker/005930?include_non_main=false
    """
    service = get_naver_research_service()
    reports = service.get_reports_by_ticker(db, ticker, days, include_non_main)

    return {
        "ticker": ticker,
        "days": days,
        "include_non_main": include_non_main,
        "total": len(reports),
        "items": [report.to_dict(include_relations=True) for report in reports]
    }


@router.get("/reports/ticker/{ticker}/consensus")
async def get_ticker_consensus(
    ticker: str,
    days: int = Query(30, ge=1, le=365, description="조회 기간 (일)"),
    db: Session = Depends(get_db)
):
    """
    종목별 컨센서스 (여러 증권사 의견 집계)

    Examples:
        - GET /api/naver-research/reports/ticker/005930/consensus?days=30

    Returns:
        {
            "ticker": "005930",
            "report_count": 15,
            "opinions": {"BUY": 10, "HOLD": 3, "SELL": 2},
            "avg_target_price": 82000,
            "target_price_range": {"min": 75000, "max": 90000}
        }
    """
    service = get_naver_research_service()
    consensus = service.get_ticker_consensus(db, ticker, days)

    return consensus


@router.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    """
    수집 통계

    Examples:
        - GET /api/naver-research/stats
    """
    service = get_naver_research_service()
    stats = service.get_stats(db)

    return stats


@router.get("/health")
async def health_check():
    """
    헬스체크

    Examples:
        - GET /api/naver-research/health
    """
    from app.services.naver_research_crawler import NaverResearchCrawler

    crawler = NaverResearchCrawler()

    return {
        "status": "healthy",
        "categories": list(crawler.RESEARCH_URLS.keys()),
        "category_count": len(crawler.RESEARCH_URLS)
    }