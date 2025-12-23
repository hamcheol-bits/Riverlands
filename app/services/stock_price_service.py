"""
Stock Price 서비스
주가 데이터 조회, 수집, 관리
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from app.services.kis_client import get_kis_client
from app.models.stock import Stock
from app.models.stock_price import StockPrice

logger = logging.getLogger(__name__)


class StockPriceService:
    """
    주가 데이터 서비스

    - 주가 조회 (단일/기간/최신)
    - 주가 수집 (KIS API)
    - 증분 갱신
    """

    def __init__(self):
        self.kis_client = get_kis_client()

    # ============================================================
    # 조회 기능
    # ============================================================

    def get_latest_price(
        self,
        db: Session,
        ticker: str
    ) -> Optional[StockPrice]:
        """
        최신 주가 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            StockPrice 객체 또는 None
        """
        return db.query(StockPrice).filter(
            StockPrice.ticker == ticker
        ).order_by(
            StockPrice.stck_bsop_date.desc()
        ).first()

    def get_price_by_date(
        self,
        db: Session,
        ticker: str,
        date: str
    ) -> Optional[StockPrice]:
        """
        특정 날짜 주가 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            date: 날짜 (YYYY-MM-DD 또는 YYYYMMDD)

        Returns:
            StockPrice 객체 또는 None
        """
        # 날짜 형식 변환
        if len(date) == 8:  # YYYYMMDD
            date_obj = datetime.strptime(date, "%Y%m%d").date()
        else:  # YYYY-MM-DD
            date_obj = datetime.strptime(date, "%Y-%m-%d").date()

        return db.query(StockPrice).filter(
            and_(
                StockPrice.ticker == ticker,
                StockPrice.stck_bsop_date == date_obj
            )
        ).first()

    def get_prices_by_range(
        self,
        db: Session,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> List[StockPrice]:
        """
        기간별 주가 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)
            limit: 결과 개수 제한

        Returns:
            StockPrice 리스트
        """
        query = db.query(StockPrice).filter(StockPrice.ticker == ticker)

        if start_date:
            start_obj = datetime.strptime(start_date, "%Y%m%d").date()
            query = query.filter(StockPrice.stck_bsop_date >= start_obj)

        if end_date:
            end_obj = datetime.strptime(end_date, "%Y%m%d").date()
            query = query.filter(StockPrice.stck_bsop_date <= end_obj)

        return query.order_by(
            StockPrice.stck_bsop_date.desc()
        ).limit(limit).all()

    def get_recent_prices(
        self,
        db: Session,
        ticker: str,
        days: int = 30
    ) -> List[StockPrice]:
        """
        최근 N일 주가 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            days: 조회 일수

        Returns:
            StockPrice 리스트
        """
        return db.query(StockPrice).filter(
            StockPrice.ticker == ticker
        ).order_by(
            StockPrice.stck_bsop_date.desc()
        ).limit(days).all()

    def count_prices(
        self,
        db: Session,
        ticker: str
    ) -> int:
        """
        주가 데이터 개수 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            레코드 수
        """
        return db.query(StockPrice).filter(
            StockPrice.ticker == ticker
        ).count()

    # ============================================================
    # 수집 기능
    # ============================================================

    async def collect_daily_prices(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        KIS API로 일별 주가 수집

        Args:
            ticker: 종목코드
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)

        Returns:
            주가 데이터 리스트
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=100)).strftime("%Y%m%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        endpoint = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        tr_id = "FHKST03010100"

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0"
        }

        try:
            response = await self.kis_client._request("GET", endpoint, tr_id, params)

            if response.get("rt_cd") != "0":
                logger.warning(f"API error: {response.get('msg1')}")
                return []

            prices = response.get("output2", [])
            logger.info(f"Collected {len(prices)} price records for {ticker}")
            return prices

        except Exception as e:
            logger.error(f"Failed to collect prices for {ticker}: {e}")
            return []

    def save_prices(
        self,
        db: Session,
        ticker: str,
        prices: List[Dict[str, Any]]
    ) -> int:
        """
        주가 데이터 저장

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            prices: KIS API 응답 데이터

        Returns:
            저장된 레코드 수
        """
        if not prices:
            return 0

        saved_count = 0

        for item in prices:
            try:
                stock_price = StockPrice(
                    ticker=ticker,
                    stck_bsop_date=datetime.strptime(item["stck_bsop_date"], "%Y%m%d").date(),
                    stck_oprc=float(item["stck_oprc"]) if item.get("stck_oprc") else None,
                    stck_hgpr=float(item["stck_hgpr"]) if item.get("stck_hgpr") else None,
                    stck_lwpr=float(item["stck_lwpr"]) if item.get("stck_lwpr") else None,
                    stck_clpr=float(item["stck_clpr"]),
                    acml_vol=int(item["acml_vol"]) if item.get("acml_vol") else None,
                    acml_tr_pbmn=int(item.get("acml_tr_pbmn", 0)) if item.get("acml_tr_pbmn") else None,
                    prdy_vrss=float(item.get("prdy_vrss", 0)) if item.get("prdy_vrss") else None,
                    prdy_vrss_sign=item.get("prdy_vrss_sign")
                )

                # Upsert
                existing = db.query(StockPrice).filter(
                    and_(
                        StockPrice.ticker == ticker,
                        StockPrice.stck_bsop_date == stock_price.stck_bsop_date
                    )
                ).first()

                if existing:
                    # 업데이트
                    existing.stck_oprc = stock_price.stck_oprc
                    existing.stck_hgpr = stock_price.stck_hgpr
                    existing.stck_lwpr = stock_price.stck_lwpr
                    existing.stck_clpr = stock_price.stck_clpr
                    existing.acml_vol = stock_price.acml_vol
                    existing.acml_tr_pbmn = stock_price.acml_tr_pbmn
                    existing.prdy_vrss = stock_price.prdy_vrss
                    existing.prdy_vrss_sign = stock_price.prdy_vrss_sign
                else:
                    # 삽입
                    db.add(stock_price)

                saved_count += 1

            except Exception as e:
                logger.error(f"Failed to save price for {ticker} on {item.get('stck_bsop_date')}: {e}")
                continue

        db.commit()
        logger.info(f"Saved {saved_count} price records for {ticker}")
        return saved_count

    async def collect_and_save(
        self,
        db: Session,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        주가 수집 및 저장 (통합)

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            start_date: 시작일
            end_date: 종료일

        Returns:
            수집 결과
        """
        # 종목 존재 확인
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return {
                "ticker": ticker,
                "status": "error",
                "message": "Stock not found",
                "collected": 0,
                "saved": 0
            }

        # 데이터 수집
        prices = await self.collect_daily_prices(ticker, start_date, end_date)

        if not prices:
            return {
                "ticker": ticker,
                "status": "no_data",
                "message": "No price data returned",
                "collected": 0,
                "saved": 0
            }

        # 데이터 저장
        saved_count = self.save_prices(db, ticker, prices)

        return {
            "ticker": ticker,
            "status": "success",
            "collected": len(prices),
            "saved": saved_count
        }

    async def collect_incremental(
        self,
        db: Session,
        ticker: str
    ) -> Dict[str, Any]:
        """
        증분 수집 (마지막 수집일 이후만)

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            수집 결과
        """
        # 마지막 수집일 조회
        last_price = db.query(StockPrice).filter(
            StockPrice.ticker == ticker
        ).order_by(StockPrice.stck_bsop_date.desc()).first()

        if last_price:
            start_date = (last_price.stck_bsop_date + timedelta(days=1)).strftime("%Y%m%d")
        else:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

        end_date = datetime.now().strftime("%Y%m%d")

        # 이미 최신
        if start_date > end_date:
            return {
                "ticker": ticker,
                "status": "up_to_date",
                "message": "Already have latest data",
                "collected": 0,
                "saved": 0
            }

        return await self.collect_and_save(db, ticker, start_date, end_date)


def get_stock_price_service() -> StockPriceService:
    """StockPriceService 싱글톤 반환"""
    return StockPriceService()