"""
Investment Opinion Service
투자의견 컨센서스 데이터 수집 및 관리
"""
import logging
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.models.investment_opinion import InvestmentOpinion
from app.services.kis_client import get_kis_client

logger = logging.getLogger(__name__)


class InvestmentOpinionService:
    """투자의견 컨센서스 서비스"""

    def __init__(self):
        self.kis_client = get_kis_client()

    def get_opinions_by_ticker(self, db: Session, ticker: str) -> list[InvestmentOpinion]:
        """특정 종목의 모든 증권사 투자의견 조회"""
        return db.query(InvestmentOpinion).filter(
            InvestmentOpinion.ticker == ticker
        ).all()

    def get_opinion_by_ticker_and_firm(
        self, db: Session, ticker: str, mbcr_name: str
    ) -> Optional[InvestmentOpinion]:
        """특정 종목 + 증권사 투자의견 조회"""
        return db.query(InvestmentOpinion).filter(
            InvestmentOpinion.ticker == ticker,
            InvestmentOpinion.mbcr_name == mbcr_name
        ).first()

    async def collect_investment_opinions(
        self,
        db: Session,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """
        특정 종목의 투자의견 수집

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            start_date: 시작일(YYYYMMDD), 기본값: 6개월 전
            end_date: 종료일(YYYYMMDD), 기본값: 오늘

        Returns:
            수집 결과 딕셔너리
        """
        # 날짜 기본값 설정
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")

        logger.info(f"Collecting investment opinions for {ticker} from {start_date} to {end_date}")

        try:
            # KIS API 호출
            endpoint = "/uapi/domestic-stock/v1/quotations/invest-opinion"
            tr_id = "FHKST663300C0"

            params = {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_COND_SCR_DIV_CODE": "16633",
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": start_date,
                "FID_INPUT_DATE_2": end_date,
            }

            response = await self.kis_client._request("GET", endpoint, tr_id, params)

            if not response or "output" not in response:
                logger.warning(f"No data received for ticker {ticker}")
                return {
                    "ticker": ticker,
                    "collected": 0,
                    "updated": 0,
                    "message": "No data from API"
                }

            opinions = response["output"]
            if not opinions:
                logger.info(f"No opinions found for ticker {ticker}")
                return {
                    "ticker": ticker,
                    "collected": 0,
                    "updated": 0,
                    "message": "No opinions available"
                }

            # API 응답 내에서 증권사별로 최신 데이터만 선택
            latest_by_firm = {}
            for opinion_data in opinions:
                mbcr_name = opinion_data.get("mbcr_name")
                stck_bsop_date = opinion_data.get("stck_bsop_date", "")

                if not mbcr_name:
                    continue

                # 해당 증권사의 기존 데이터가 없거나, 더 최신 데이터인 경우
                if mbcr_name not in latest_by_firm or stck_bsop_date > latest_by_firm[mbcr_name].get("stck_bsop_date", ""):
                    latest_by_firm[mbcr_name] = opinion_data

            logger.info(f"Filtered {len(latest_by_firm)} unique firms from {len(opinions)} total opinions")

            # UPSERT 처리
            collected = 0
            updated = 0

            for mbcr_name, opinion_data in latest_by_firm.items():
                try:
                    result = self._upsert_opinion(db, ticker, opinion_data)

                    if result == "inserted":
                        collected += 1
                    elif result == "updated":
                        updated += 1

                except Exception as e:
                    logger.error(f"Error upserting {ticker} - {mbcr_name}: {e}")
                    db.rollback()
                    continue

            # 전체 commit
            try:
                db.commit()
            except Exception as e:
                logger.error(f"Error committing for {ticker}: {e}")
                db.rollback()
                raise

            logger.info(
                f"Investment opinions for {ticker}: "
                f"collected={collected}, updated={updated}"
            )

            return {
                "ticker": ticker,
                "collected": collected,
                "updated": updated,
                "total": len(latest_by_firm),
                "message": "Success"
            }

        except Exception as e:
            db.rollback()
            logger.error(f"Error collecting investment opinions for {ticker}: {str(e)}")
            raise

    def _upsert_opinion(self, db: Session, ticker: str, opinion_data: dict) -> str:
        """
        투자의견 UPSERT (증분 처리)

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            opinion_data: 투자의견 데이터

        Returns:
            "inserted" | "updated" | "skipped"
        """
        mbcr_name = opinion_data.get("mbcr_name")

        if not mbcr_name:
            logger.warning(f"Missing mbcr_name: {opinion_data}")
            return "skipped"

        # 기존 데이터 확인
        existing = db.query(InvestmentOpinion).filter(
            InvestmentOpinion.ticker == ticker,
            InvestmentOpinion.mbcr_name == mbcr_name
        ).first()

        # 새 데이터의 날짜
        new_date = opinion_data.get("stck_bsop_date", "")

        if existing:
            # 날짜가 더 최신인 경우에만 업데이트
            if new_date > existing.stck_bsop_date:
                existing.stck_bsop_date = new_date
                existing.invt_opnn = opinion_data.get("invt_opnn")
                existing.invt_opnn_cls_code = opinion_data.get("invt_opnn_cls_code")
                existing.rgbf_invt_opnn = opinion_data.get("rgbf_invt_opnn")
                existing.rgbf_invt_opnn_cls_code = opinion_data.get("rgbf_invt_opnn_cls_code")
                existing.hts_goal_prc = opinion_data.get("hts_goal_prc")

                logger.debug(f"Updated opinion: {ticker} - {mbcr_name} ({new_date})")
                return "updated"
            else:
                logger.debug(f"Skipped (old date): {ticker} - {mbcr_name} ({new_date} <= {existing.stck_bsop_date})")
                return "skipped"
        else:
            # 신규 삽입
            new_opinion = InvestmentOpinion(
                ticker=ticker,
                mbcr_name=mbcr_name,
                stck_bsop_date=new_date,
                invt_opnn=opinion_data.get("invt_opnn"),
                invt_opnn_cls_code=opinion_data.get("invt_opnn_cls_code"),
                rgbf_invt_opnn=opinion_data.get("rgbf_invt_opnn"),
                rgbf_invt_opnn_cls_code=opinion_data.get("rgbf_invt_opnn_cls_code"),
                hts_goal_prc=opinion_data.get("hts_goal_prc"),
            )
            db.add(new_opinion)
            logger.debug(f"Inserted new opinion: {ticker} - {mbcr_name} ({new_date})")
            return "inserted"

    def get_all_opinions(
        self,
        db: Session,
        skip: int = 0,
        limit: int = 100
    ) -> list[InvestmentOpinion]:
        """모든 투자의견 조회 (페이지네이션)"""
        return db.query(InvestmentOpinion).offset(skip).limit(limit).all()

    def delete_opinion(self, db: Session, ticker: str, mbcr_name: str) -> bool:
        """특정 투자의견 삭제"""
        opinion = self.get_opinion_by_ticker_and_firm(db, ticker, mbcr_name)
        if opinion:
            db.delete(opinion)
            db.commit()
            return True
        return False

    def delete_all_opinions_by_ticker(self, db: Session, ticker: str) -> int:
        """특정 종목의 모든 투자의견 삭제"""
        deleted = db.query(InvestmentOpinion).filter(
            InvestmentOpinion.ticker == ticker
        ).delete()
        db.commit()
        return deleted


def get_investment_opinion_service() -> InvestmentOpinionService:
    """InvestmentOpinionService 싱글톤 반환"""
    return InvestmentOpinionService()