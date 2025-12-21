"""
Stock 서비스
종목 기본 정보 조회, 수집, 관리
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from pykrx import stock as pykrx_stock

from app.services.kis_client import get_kis_client
from app.models.stock import Stock

logger = logging.getLogger(__name__)


class StockService:
    """
    종목 정보 서비스

    - 종목 조회 (단일/다중/시장별)
    - 종목 수집 (pykrx + KIS API)
    - 종목 정보 갱신
    """

    def __init__(self):
        self.kis_client = get_kis_client()

    # ============================================================
    # 조회 기능
    # ============================================================

    def get_stock(self, db: Session, ticker: str) -> Optional[Stock]:
        """
        단일 종목 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            Stock 객체 또는 None
        """
        return db.query(Stock).filter(Stock.ticker == ticker).first()

    def get_stocks(
        self,
        db: Session,
        market: Optional[str] = None,
        is_active: bool = True,
        skip: int = 0,
        limit: int = 100
    ) -> List[Stock]:
        """
        종목 목록 조회

        Args:
            db: 데이터베이스 세션
            market: 시장 구분 (KOSPI/KOSDAQ/None)
            is_active: 활성 종목만 조회
            skip: 페이지네이션 skip
            limit: 페이지네이션 limit

        Returns:
            Stock 리스트
        """
        query = db.query(Stock)

        if is_active:
            query = query.filter(Stock.is_active == True)

        if market and market.upper() != "ALL":
            query = query.filter(Stock.mrkt_ctg_cls_code == market.upper())

        return query.offset(skip).limit(limit).all()

    def get_stocks_by_tickers(
        self,
        db: Session,
        tickers: List[str]
    ) -> List[Stock]:
        """
        여러 종목 일괄 조회

        Args:
            db: 데이터베이스 세션
            tickers: 종목코드 리스트

        Returns:
            Stock 리스트
        """
        return db.query(Stock).filter(Stock.ticker.in_(tickers)).all()

    def count_stocks(
        self,
        db: Session,
        market: Optional[str] = None,
        is_active: bool = True
    ) -> int:
        """
        종목 수 조회

        Args:
            db: 데이터베이스 세션
            market: 시장 구분
            is_active: 활성 종목만 카운트

        Returns:
            종목 수
        """
        query = db.query(Stock)

        if is_active:
            query = query.filter(Stock.is_active == True)

        if market and market.upper() != "ALL":
            query = query.filter(Stock.mrkt_ctg_cls_code == market.upper())

        return query.count()

    def search_stocks(
        self,
        db: Session,
        keyword: str,
        market: Optional[str] = None,
        limit: int = 20
    ) -> List[Stock]:
        """
        종목명 검색

        Args:
            db: 데이터베이스 세션
            keyword: 검색 키워드
            market: 시장 구분
            limit: 결과 개수 제한

        Returns:
            Stock 리스트
        """
        query = db.query(Stock).filter(Stock.is_active == True)

        # 종목명 또는 종목코드로 검색
        query = query.filter(
            or_(
                Stock.hts_kor_isnm.like(f"%{keyword}%"),
                Stock.ticker.like(f"%{keyword}%")
            )
        )

        if market and market.upper() != "ALL":
            query = query.filter(Stock.mrkt_ctg_cls_code == market.upper())

        return query.limit(limit).all()

    # ============================================================
    # 수집 기능
    # ============================================================

    def get_ticker_list_from_pykrx(
        self,
        market: str,
        date: Optional[str] = None
    ) -> List[str]:
        """
        pykrx로 시장별 티커 리스트 조회

        Args:
            market: KOSPI 또는 KOSDAQ
            date: 조회 기준일 (YYYYMMDD)

        Returns:
            티커 리스트
        """
        if date is None:
            date = datetime.now().strftime("%Y%m%d")

        try:
            tickers = pykrx_stock.get_market_ticker_list(date, market=market)
            logger.info(f"Found {len(tickers)} tickers in {market} (date: {date})")
            return tickers
        except Exception as e:
            logger.error(f"Failed to get ticker list for {market}: {e}")
            return []

    async def get_stock_info_from_kis(
        self,
        ticker: str
    ) -> Optional[Dict[str, Any]]:
        """
        KIS API로 종목 상세 정보 조회

        Args:
            ticker: 종목코드

        Returns:
            종목 정보 딕셔너리
        """
        endpoint = "/uapi/domestic-stock/v1/quotations/search-stock-info"
        tr_id = "CTPF1002R"

        params = {
            "PRDT_TYPE_CD": "300",
            "PDNO": ticker
        }

        try:
            response = await self.kis_client._request("GET", endpoint, tr_id, params)

            if response.get("rt_cd") != "0":
                logger.warning(f"API error for {ticker}: {response.get('msg1')}")
                return None

            output = response.get("output", {})
            if not output:
                return None

            return output

        except Exception as e:
            logger.error(f"Failed to get stock info for {ticker}: {e}")
            return None

    def save_stock(
        self,
        db: Session,
        ticker: str,
        market: str,
        stock_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        종목 정보 저장/갱신

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            market: 시장 구분
            stock_info: KIS API 응답 (선택)

        Returns:
            저장 성공 여부
        """
        try:
            existing = db.query(Stock).filter(Stock.ticker == ticker).first()

            if stock_info:
                hts_kor_isnm = stock_info.get("prdt_name", "")
                bstp_kor_isnm = stock_info.get("std_idst_clsf_cd_name", "")
            else:
                hts_kor_isnm = f"Unknown_{ticker}"
                bstp_kor_isnm = ""

            mrkt_ctg_cls_code = market.upper()

            if existing:
                # 업데이트
                existing.hts_kor_isnm = hts_kor_isnm
                existing.mrkt_ctg_cls_code = mrkt_ctg_cls_code
                existing.bstp_kor_isnm = bstp_kor_isnm
                existing.is_active = True
                logger.debug(f"Updated stock: {ticker}")
            else:
                # 신규 삽입
                new_stock = Stock(
                    ticker=ticker,
                    hts_kor_isnm=hts_kor_isnm,
                    mrkt_ctg_cls_code=mrkt_ctg_cls_code,
                    bstp_kor_isnm=bstp_kor_isnm,
                    is_active=True
                )
                db.add(new_stock)
                logger.debug(f"Inserted new stock: {ticker}")

            db.commit()
            return True

        except Exception as e:
            logger.error(f"Failed to save stock {ticker}: {e}")
            db.rollback()
            return False

    async def collect_stock(
        self,
        db: Session,
        ticker: str,
        use_api: bool = True
    ) -> Dict[str, Any]:
        """
        단일 종목 수집

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            use_api: KIS API 사용 여부

        Returns:
            수집 결과
        """
        stock_info = None

        if use_api:
            stock_info = await self.get_stock_info_from_kis(ticker)

        # 시장 구분 확인 (기존 DB 또는 pykrx)
        existing = db.query(Stock).filter(Stock.ticker == ticker).first()
        market = existing.mrkt_ctg_cls_code if existing else "KOSPI"

        success = self.save_stock(db, ticker, market, stock_info)

        return {
            "ticker": ticker,
            "status": "success" if success else "error",
            "updated": success
        }

    async def collect_stocks_by_market(
        self,
        db: Session,
        market: str,
        use_api: bool = True,
        date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        시장별 종목 수집

        Args:
            db: 데이터베이스 세션
            market: KOSPI 또는 KOSDAQ
            use_api: KIS API 사용 여부
            date: 기준일 (YYYYMMDD)
            limit: 수집 제한 (테스트용)

        Returns:
            수집 결과
        """
        # pykrx로 티커 리스트 조회
        tickers = self.get_ticker_list_from_pykrx(market, date)

        if not tickers:
            return {
                "market": market,
                "status": "error",
                "message": "No tickers found",
                "total": 0,
                "saved": 0
            }

        if limit:
            tickers = tickers[:limit]

        # 각 종목 수집
        saved_count = 0
        for ticker in tickers:
            stock_info = None

            if use_api:
                stock_info = await self.get_stock_info_from_kis(ticker)

            success = self.save_stock(db, ticker, market, stock_info)
            if success:
                saved_count += 1

        logger.info(f"Collected {saved_count}/{len(tickers)} stocks from {market}")

        return {
            "market": market,
            "status": "success",
            "total": len(tickers),
            "saved": saved_count
        }

    # ============================================================
    # 관리 기능
    # ============================================================

    def deactivate_stock(self, db: Session, ticker: str) -> bool:
        """
        종목 비활성화 (상장폐지 등)

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            성공 여부
        """
        try:
            stock = db.query(Stock).filter(Stock.ticker == ticker).first()
            if stock:
                stock.is_active = False
                db.commit()
                logger.info(f"Deactivated stock: {ticker}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to deactivate stock {ticker}: {e}")
            db.rollback()
            return False


def get_stock_service() -> StockService:
    """StockService 싱글톤 반환"""
    return StockService()