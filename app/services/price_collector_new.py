"""
주가 데이터 수집 서비스 (KIS API 응답 필드명 사용)
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from app.services.kis_client import get_kis_client
from app.models.stock import Stock
from app.models.stock_price import StockPrice

logger = logging.getLogger(__name__)


class PriceCollector:
    """
    주가 데이터 수집기
    
    KIS API 응답 필드명을 그대로 사용하여 데이터베이스에 저장
    """

    def __init__(self):
        self.kis_client = get_kis_client()

    async def collect_daily_prices(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        일별 주가 데이터 수집
        
        KIS API: FHKST03010100 (국내주식기간별시세)
        
        Args:
            ticker: 종목코드 (6자리)
            start_date: 시작일 (YYYYMMDD, 기본값: 100일 전)
            end_date: 종료일 (YYYYMMDD, 기본값: 오늘)

        Returns:
            주가 데이터 리스트 (KIS API output 필드명 그대로 반환)
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=100)).strftime("%Y%m%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        endpoint = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        tr_id = "FHKST03010100"  # 국내주식기간별시세(일/주/월/년)

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # 시장 분류 코드 (J:주식/ETF/ETN)
            "FID_INPUT_ISCD": ticker,  # 종목코드
            "FID_INPUT_DATE_1": start_date,  # 시작일자
            "FID_INPUT_DATE_2": end_date,  # 종료일자
            "FID_PERIOD_DIV_CODE": "D",  # 기간 분류 코드 (D:일, W:주, M:월, Y:년)
            "FID_ORG_ADJ_PRC": "0"  # 수정주가 여부 (0:수정주가 반영 안함, 1:반영)
        }

        try:
            response = await self.kis_client._request("GET", endpoint, tr_id, params)
            
            # KIS API 응답 구조: { rt_cd, msg_cd, msg1, output1, output2 }
            # output2: 일자별 주가 데이터 배열
            if response.get("rt_cd") != "0":
                logger.warning(f"API Error: {response.get('msg1')}")
                return []

            prices = response.get("output2", [])
            logger.info(f"Collected {len(prices)} price records for {ticker}")
            
            return prices  # KIS API 응답 필드명 그대로 반환

        except Exception as e:
            logger.error(f"Failed to collect prices for {ticker}: {e}")
            return []

    async def save_prices_to_db(
        self,
        db: Session,
        ticker: str,
        prices: List[Dict[str, Any]]
    ) -> int:
        """
        수집한 주가 데이터를 DB에 저장
        
        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            prices: KIS API 응답 데이터 (output2)

        Returns:
            저장된 레코드 수
        """
        if not prices:
            return 0

        saved_count = 0
        
        for item in prices:
            try:
                # KIS API 응답 필드명 그대로 사용
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
                    prdy_vrss_sign=item.get("prdy_vrss_sign"),
                    prdy_ctrt=float(item.get("prdy_ctrt", 0)) if item.get("prdy_ctrt") else None,
                    hts_frgn_ehrt=float(item.get("hts_frgn_ehrt", 0)) if item.get("hts_frgn_ehrt") else None,
                    frgn_ntby_qty=int(item.get("frgn_ntby_qty", 0)) if item.get("frgn_ntby_qty") else None
                )

                # Upsert: 기존 데이터가 있으면 업데이트, 없으면 삽입
                existing = db.query(StockPrice).filter(
                    StockPrice.ticker == ticker,
                    StockPrice.stck_bsop_date == stock_price.stck_bsop_date
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
                    existing.prdy_ctrt = stock_price.prdy_ctrt
                    existing.hts_frgn_ehrt = stock_price.hts_frgn_ehrt
                    existing.frgn_ntby_qty = stock_price.frgn_ntby_qty
                else:
                    # 삽입
                    db.add(stock_price)

                saved_count += 1

            except Exception as e:
                logger.error(f"Failed to save price data for {ticker} on {item.get('stck_bsop_date')}: {e}")
                continue

        # 커밋
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
        주가 데이터 수집 및 저장 (통합)
        
        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)

        Returns:
            수집 결과
        """
        # 종목 존재 확인
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return {
                "ticker": ticker,
                "status": "error",
                "message": f"Stock {ticker} not found in database",
                "collected": 0
            }

        # 데이터 수집
        prices = await self.collect_daily_prices(ticker, start_date, end_date)
        
        if not prices:
            return {
                "ticker": ticker,
                "status": "no_data",
                "message": "No price data returned from API",
                "collected": 0
            }

        # 데이터 저장
        saved_count = await self.save_prices_to_db(db, ticker, prices)

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
        증분 수집 (마지막 수집일 이후 데이터만)
        
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
            # 마지막 수집일 다음날부터
            start_date = (last_price.stck_bsop_date + timedelta(days=1)).strftime("%Y%m%d")
        else:
            # 최초 수집: 1년 전부터
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

        end_date = datetime.now().strftime("%Y%m%d")

        # 이미 최신인 경우
        if start_date > end_date:
            return {
                "ticker": ticker,
                "status": "up_to_date",
                "message": "Already have latest data",
                "collected": 0,
                "saved": 0
            }

        return await self.collect_and_save(db, ticker, start_date, end_date)


def get_price_collector() -> PriceCollector:
    """PriceCollector 싱글톤 반환"""
    return PriceCollector()
