"""
네이버 리서치 서비스 (다대다 관계 지원)
app/services/naver_research_service.py
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from pathlib import Path

from app.models.research_report import ResearchReport, ReportStockRelation, ReportIndustry
from app.services.naver_research_crawler import NaverResearchCrawler

logger = logging.getLogger(__name__)


class NaverResearchService:
    """네이버증권 리서치 수집 서비스"""

    def __init__(self, pdf_storage_path: str = "./data/research_reports/naver"):
        self.crawler = NaverResearchCrawler()
        self.pdf_storage_path = Path(pdf_storage_path)
        self.pdf_storage_path.mkdir(parents=True, exist_ok=True)

    # ============================================================
    # 수집 기능
    # ============================================================

    async def collect_incremental(
        self,
        db: Session,
        days: int = 1,
        categories: Optional[List[str]] = None,
        auto_download: bool = False
    ) -> Dict[str, Any]:
        """증분 수집 (최근 N일)"""
        start_date = datetime.now() - timedelta(days=days)

        logger.info(f"Starting incremental collection (last {days} days)")

        results = await self.crawler.crawl_all_categories(
            start_date=start_date,
            max_pages_per_category=5,
            categories=categories
        )

        saved_count = 0
        total_ticker_relations = 0
        total_industry_relations = 0
        downloaded_count = 0

        for category, reports in results.items():
            for report_data in reports:
                report = self._save_report_with_relations(db, report_data)

                if report:
                    saved_count += 1

                    ticker_count = len(report_data.get("related_tickers", []))
                    industry_count = len(report_data.get("related_industries", []))

                    total_ticker_relations += ticker_count
                    total_industry_relations += industry_count

                    if auto_download:
                        success = await self.download_pdf(db, report.id)
                        if success:
                            downloaded_count += 1

        db.commit()

        logger.info(
            f"Collection complete: {saved_count} reports, "
            f"{total_ticker_relations} ticker relations, "
            f"{total_industry_relations} industry relations"
        )

        return {
            "status": "success",
            "days": days,
            "categories": list(results.keys()),
            "total_collected": sum(len(r) for r in results.values()),
            "saved": saved_count,
            "downloaded": downloaded_count,
            "ticker_relations": total_ticker_relations,
            "industry_relations": total_industry_relations,
            "by_category": {cat: len(reports) for cat, reports in results.items()}
        }

    async def collect_by_category(
        self,
        db: Session,
        category: str,
        start_date: Optional[datetime] = None,
        max_pages: int = 10,
        auto_download: bool = False
    ) -> Dict[str, Any]:
        """특정 카테고리 수집"""
        if not start_date:
            start_date = datetime.now() - timedelta(days=7)

        logger.info(f"Collecting {category} category")

        reports = await self.crawler.crawl_category(
            category=category,
            start_date=start_date,
            max_pages=max_pages
        )

        saved_count = 0
        downloaded_count = 0

        for report_data in reports:
            report = self._save_report_with_relations(db, report_data)
            if report:
                saved_count += 1

                if auto_download:
                    success = await self.download_pdf(db, report.id)
                    if success:
                        downloaded_count += 1

        db.commit()

        return {
            "status": "success",
            "category": category,
            "collected": len(reports),
            "saved": saved_count,
            "downloaded": downloaded_count
        }

    async def collect_by_ticker(
        self,
        db: Session,
        ticker: str,
        days: int = 30,
        auto_download: bool = False
    ) -> Dict[str, Any]:
        """특정 종목 리포트 수집"""
        start_date = datetime.now() - timedelta(days=days)

        logger.info(f"Collecting reports for {ticker}")

        results = await self.crawler.crawl_all_categories(
            start_date=start_date,
            max_pages_per_category=10,
            categories=["company", "industry"]
        )

        ticker_reports = []
        for reports in results.values():
            for report in reports:
                related_tickers = report.get("related_tickers", [])
                if any(t["ticker"] == ticker for t in related_tickers):
                    ticker_reports.append(report)

        saved_count = 0
        downloaded_count = 0

        for report_data in ticker_reports:
            report = self._save_report_with_relations(db, report_data)
            if report:
                saved_count += 1

                if auto_download:
                    success = await self.download_pdf(db, report.id)
                    if success:
                        downloaded_count += 1

        db.commit()

        return {
            "status": "success",
            "ticker": ticker,
            "collected": len(ticker_reports),
            "saved": saved_count,
            "downloaded": downloaded_count
        }

    def _save_report_with_relations(
        self,
        db: Session,
        report_data: Dict[str, Any]
    ) -> Optional[ResearchReport]:
        """리포트 및 관련 정보 저장 (다대다 관계)"""
        try:
            report_id = report_data["id"]

            # 1. 리포트 기본 정보
            existing_report = db.query(ResearchReport).filter(
                ResearchReport.id == report_id
            ).first()

            if existing_report:
                for key, value in report_data.items():
                    if key not in ["related_tickers", "related_industries"]:
                        if hasattr(existing_report, key) and value is not None:
                            setattr(existing_report, key, value)
                report = existing_report
            else:
                report_dict = {
                    k: v for k, v in report_data.items()
                    if k not in ["related_tickers", "related_industries"]
                }
                report = ResearchReport(**report_dict)
                db.add(report)

            # 2. 종목 관계 저장
            related_tickers = report_data.get("related_tickers", [])

            if related_tickers:
                # 기존 관계 삭제
                db.query(ReportStockRelation).filter(
                    ReportStockRelation.report_id == report_id
                ).delete()

                # 새로운 관계 삽입
                from app.models.stock import Stock
                for ticker_info in related_tickers:
                    stock_exists = db.query(Stock).filter(
                        Stock.ticker == ticker_info["ticker"]
                    ).first()

                    if not stock_exists:
                        logger.warning(f"Stock {ticker_info['ticker']} not found, skipping")
                        continue

                    relation = ReportStockRelation(
                        report_id=report_id,
                        ticker=ticker_info["ticker"],
                        investment_opinion=ticker_info.get("investment_opinion"),
                        target_price=ticker_info.get("target_price"),
                        is_main_ticker=ticker_info.get("is_main_ticker", False)
                    )
                    db.add(relation)

            # 3. 산업 관계 저장
            related_industries = report_data.get("related_industries", [])

            if related_industries:
                db.query(ReportIndustry).filter(
                    ReportIndustry.report_id == report_id
                ).delete()

                for industry_name in related_industries:
                    industry = ReportIndustry(
                        report_id=report_id,
                        industry_name=industry_name
                    )
                    db.add(industry)

            db.flush()
            return report

        except Exception as e:
            logger.error(f"Failed to save report: {e}")
            db.rollback()
            return None

    # ============================================================
    # PDF 다운로드
    # ============================================================

    async def download_pdf(self, db: Session, report_id: str) -> bool:
        """PDF 다운로드"""
        report = db.query(ResearchReport).filter(
            ResearchReport.id == report_id
        ).first()

        if not report or not report.pdf_url:
            return False

        if report.download_status == "downloaded":
            return True

        filename = f"{report_id}.pdf"
        save_path = self.pdf_storage_path / report.broker / filename
        save_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            success = await self.crawler.download_pdf(report.pdf_url, str(save_path))

            if success:
                report.pdf_local_path = str(save_path)
                report.file_size_bytes = save_path.stat().st_size
                report.download_status = "downloaded"
                db.commit()
                return True
            else:
                report.download_status = "failed"
                db.commit()
                return False
        except Exception as e:
            logger.error(f"Failed to download PDF: {e}")
            report.download_status = "failed"
            db.commit()
            return False

    async def batch_download_pdfs(
        self,
        db: Session,
        limit: int = 100,
        broker: Optional[str] = None,
        days: Optional[int] = None
    ) -> Dict[str, Any]:
        """미다운로드 PDF 배치 다운로드"""
        query = db.query(ResearchReport).filter(
            ResearchReport.download_status == "pending",
            ResearchReport.pdf_url.isnot(None)
        )

        if broker:
            query = query.filter(ResearchReport.broker == broker)

        if days:
            cutoff = datetime.now().date() - timedelta(days=days)
            query = query.filter(ResearchReport.published_date >= cutoff)

        pending_reports = query.limit(limit).all()

        success_count = 0
        failed_count = 0

        for report in pending_reports:
            success = await self.download_pdf(db, report.id)
            if success:
                success_count += 1
            else:
                failed_count += 1

        return {
            "status": "success",
            "total": len(pending_reports),
            "success": success_count,
            "failed": failed_count
        }

    # ============================================================
    # 조회 기능
    # ============================================================

    def get_recent_reports(
        self,
        db: Session,
        days: int = 7,
        limit: int = 100,
        category: Optional[str] = None
    ) -> List[ResearchReport]:
        """최근 리포트 조회"""
        cutoff = datetime.now().date() - timedelta(days=days)

        query = db.query(ResearchReport).filter(
            ResearchReport.published_date >= cutoff
        )

        if category:
            query = query.filter(ResearchReport.category == category)

        return query.order_by(
            ResearchReport.published_date.desc()
        ).limit(limit).all()

    def get_reports_by_ticker(
        self,
        db: Session,
        ticker: str,
        days: int = 30,
        include_non_main: bool = True
    ) -> List[ResearchReport]:
        """종목별 리포트 조회"""
        cutoff = datetime.now().date() - timedelta(days=days)

        query = db.query(ResearchReport).join(
            ReportStockRelation,
            ResearchReport.id == ReportStockRelation.report_id
        ).filter(
            ReportStockRelation.ticker == ticker,
            ResearchReport.published_date >= cutoff
        )

        if not include_non_main:
            query = query.filter(ReportStockRelation.is_main_ticker == True)

        return query.order_by(
            ResearchReport.published_date.desc()
        ).all()

    def get_ticker_consensus(
        self,
        db: Session,
        ticker: str,
        days: int = 30
    ) -> Dict[str, Any]:
        """종목별 컨센서스 집계"""
        cutoff = datetime.now().date() - timedelta(days=days)

        relations = db.query(ReportStockRelation).join(
            ResearchReport,
            ReportStockRelation.report_id == ResearchReport.id
        ).filter(
            ReportStockRelation.ticker == ticker,
            ResearchReport.published_date >= cutoff
        ).all()

        if not relations:
            return {
                "ticker": ticker,
                "days": days,
                "report_count": 0,
                "opinions": {},
                "avg_target_price": None
            }

        opinions = {}
        target_prices = []

        for rel in relations:
            if rel.investment_opinion:
                opinions[rel.investment_opinion] = opinions.get(rel.investment_opinion, 0) + 1
            if rel.target_price:
                target_prices.append(rel.target_price)

        avg_target = sum(target_prices) / len(target_prices) if target_prices else None

        return {
            "ticker": ticker,
            "days": days,
            "report_count": len(relations),
            "opinions": opinions,
            "avg_target_price": int(avg_target) if avg_target else None,
            "target_price_range": {
                "min": min(target_prices) if target_prices else None,
                "max": max(target_prices) if target_prices else None
            }
        }

    def get_stats(self, db: Session) -> Dict[str, Any]:
        """수집 통계"""
        from sqlalchemy import func

        total = db.query(ResearchReport).count()

        by_type = db.query(
            ResearchReport.report_type,
            func.count(ResearchReport.id).label("count")
        ).group_by(ResearchReport.report_type).all()

        by_broker = db.query(
            ResearchReport.broker,
            func.count(ResearchReport.id).label("count")
        ).group_by(ResearchReport.broker).all()

        downloaded = db.query(ResearchReport).filter(
            ResearchReport.download_status == "downloaded"
        ).count()

        recent = self.get_recent_reports(db, days=7)

        return {
            "total_reports": total,
            "by_type": {t: c for t, c in by_type},
            "by_broker": {b: c for b, c in by_broker},
            "downloaded": downloaded,
            "download_rate": f"{downloaded/total*100:.1f}%" if total > 0 else "0%",
            "recent_7days": len(recent)
        }


def get_naver_research_service() -> NaverResearchService:
    """서비스 싱글톤"""
    return NaverResearchService()