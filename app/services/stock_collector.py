"""
주식 기본 정보 수집 서비스
pykrx로 티커 리스트 조회 + KIS API로 상세 정보 조회
"""
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from pykrx import stock as pykrx_stock

from app.services.kis_client import get_kis_client
from app.models.stock import Stock

logger = logging.getLogger(__name__)


class StockCollector:
    """
    주식 기본 정보 수집기
    
    전략:
    1. pykrx로 티커 리스트 조회 (KOSPI/KOSDAQ)
    2. KIS API로 각 종목 상세 정보 조회
    3. DB에 저장
    """

    def __init__(self):
        self.kis_client = get_kis_client()

    def get_ticker_list(self, market: str, date: Optional[str] = None) -> List[str]:
        """
        pykrx로 시장별 티커 리스트 조회
        
        Args:
            market: 시장 구분 ("KOSPI" 또는 "KOSDAQ")
            date: 조회 기준일 (YYYYMMDD, 기본값: 오늘)

        Returns:
            티커 리스트 (예: ['005930', '000660', ...])
        """
        if date is None:
            date = datetime.now().strftime("%Y%m%d")

        try:
            tickers = pykrx_stock.get_market_ticker_list(date, market=market)
            logger.info(f"Found {len(tickers)} tickers in {market} market (date: {date})")
            return tickers
        except Exception as e:
            logger.error(f"Failed to get ticker list for {market}: {e}")
            return []

    async def get_stock_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        KIS API로 종목 상세 정보 조회
        
        KIS API: CTPF1002R (주식현재가 기본조회)
        엔드포인트: /uapi/domestic-stock/v1/quotations/search-stock-info
        
        Args:
            ticker: 종목코드 (6자리)

        Returns:
            종목 정보 딕셔너리 (KIS API output 필드명)
        """
        endpoint = "/uapi/domestic-stock/v1/quotations/search-stock-info"
        tr_id = "CTPF1002R"  # 주식현재가 기본조회

        params = {
            "PRDT_TYPE_CD": "300",  # 상품유형코드 (300: 주식)
            "PDNO": ticker  # 종목코드
        }

        try:
            response = await self.kis_client._request("GET", endpoint, tr_id, params)
            
            # KIS API 응답 구조: { rt_cd, msg_cd, msg1, output }
            if response.get("rt_cd") != "0":
                logger.warning(f"API Error for {ticker}: {response.get('msg1')}")
                return None

            output = response.get("output", {})
            
            if not output:
                logger.warning(f"No data for {ticker}")
                return None

            logger.debug(f"Got stock info for {ticker}: {output.get('prdt_name')}")
            return output

        except Exception as e:
            logger.error(f"Failed to get stock info for {ticker}: {e}")
            return None

    async def save_stock_to_db(
        self,
        db: Session,
        ticker: str,
        market: str,
        stock_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        종목 정보를 DB에 저장
        
        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            market: 시장 구분 (KOSPI/KOSDAQ)
            stock_info: KIS API 응답 (없으면 기본 정보만 저장)

        Returns:
            저장 성공 여부
        """
        try:
            # 기존 종목 조회
            existing = db.query(Stock).filter(Stock.ticker == ticker).first()

            if stock_info:
                # KIS API 응답 필드명 사용
                hts_kor_isnm = stock_info.get("prdt_name", "")  # 상품명
                bstp_kor_isnm = stock_info.get("std_idst_clsf_cd_name", "")  # 표준산업분류코드명
                
                # 시장 구분 코드 매핑
                mrkt_ctg_cls_code = "KOSPI" if market == "KOSPI" else "KOSDAQ"

            else:
                # API 정보 없으면 기본값만 저장
                hts_kor_isnm = f"Unknown_{ticker}"
                bstp_kor_isnm = ""
                mrkt_ctg_cls_code = market

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

    async def collect_market_stocks(
        self,
        db: Session,
        market: str,
        use_api: bool = True,
        date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        특정 시장의 전체 종목 수집
        
        Args:
            db: 데이터베이스 세션
            market: 시장 구분 ("KOSPI" 또는 "KOSDAQ")
            use_api: KIS API로 상세 정보 조회 여부
            date: 기준일 (YYYYMMDD)

        Returns:
            수집 결과
        """
        # 1. pykrx로 티커 리스트 조회
        tickers = self.get_ticker_list(market, date)
        
        if not tickers:
            return {
                "market": market,
                "status": "error",
                "message": "No tickers found",
                "total": 0,
                "saved": 0
            }

        # 2. 각 종목 정보 수집 및 저장
        saved_count = 0
        
        for ticker in tickers:
            stock_info = None
            
            if use_api:
                # KIS API로 상세 정보 조회
                stock_info = await self.get_stock_info(ticker)
            
            # DB에 저장 (API 정보 없어도 티커는 저장)
            success = await self.save_stock_to_db(db, ticker, market, stock_info)
            
            if success:
                saved_count += 1
            
            # API 호출 제한 고려 (필요시 delay 추가)
            # await asyncio.sleep(0.1)

        logger.info(f"Collected {saved_count}/{len(tickers)} stocks from {market}")

        return {
            "market": market,
            "status": "success",
            "total": len(tickers),
            "saved": saved_count
        }

    async def collect_all_markets(
        self,
        db: Session,
        use_api: bool = True,
        date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        전체 시장(KOSPI + KOSDAQ) 종목 수집
        
        Args:
            db: 데이터베이스 세션
            use_api: KIS API로 상세 정보 조회 여부
            date: 기준일 (YYYYMMDD)

        Returns:
            전체 수집 결과
        """
        logger.info("Starting to collect all market stocks...")

        # KOSPI
        kospi_result = await self.collect_market_stocks(db, "KOSPI", use_api, date)
        
        # KOSDAQ
        kosdaq_result = await self.collect_market_stocks(db, "KOSDAQ", use_api, date)

        total_saved = kospi_result["saved"] + kosdaq_result["saved"]
        total_count = kospi_result["total"] + kosdaq_result["total"]

        logger.info(f"Completed: {total_saved}/{total_count} stocks saved")

        return {
            "status": "success",
            "kospi": kospi_result,
            "kosdaq": kosdaq_result,
            "total_saved": total_saved,
            "total_count": total_count
        }

    async def update_stock_info(
        self,
        db: Session,
        ticker: str
    ) -> Dict[str, Any]:
        """
        특정 종목 정보 업데이트 (KIS API 사용)
        
        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            업데이트 결과
        """
        # 종목 존재 확인
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        
        if not stock:
            return {
                "ticker": ticker,
                "status": "error",
                "message": "Stock not found in database"
            }

        # KIS API로 최신 정보 조회
        stock_info = await self.get_stock_info(ticker)
        
        if not stock_info:
            return {
                "ticker": ticker,
                "status": "error",
                "message": "Failed to get stock info from API"
            }

        # DB 업데이트
        success = await self.save_stock_to_db(
            db,
            ticker,
            stock.mrkt_ctg_cls_code,
            stock_info
        )

        if success:
            return {
                "ticker": ticker,
                "status": "success",
                "message": "Stock info updated"
            }
        else:
            return {
                "ticker": ticker,
                "status": "error",
                "message": "Failed to save to database"
            }


def get_stock_collector() -> StockCollector:
    """StockCollector 싱글톤 반환"""
    return StockCollector()
