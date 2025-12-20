"""
주가 데이터 수집 서비스
"""
import logging
from datetime import datetime, timedelta
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.services.kis_client import get_kis_client
from app.models.stock import Stock
from app.models.stock_price import StockPrice

logger = logging.getLogger(__name__)


class PriceCollector:
    """주가 데이터 수집기"""

    def __init__(self):
        self.client = get_kis_client()

    async def collect_daily_prices(
        self,
        db: Session,
        ticker: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> dict:
        """
        종목의 일별 주가 데이터 수집

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            start_date: 시작일 (None이면 마지막 수집일 다음날부터)
            end_date: 종료일 (None이면 오늘)

        Returns:
            수집 결과 딕셔너리
        """
        # 종목 존재 확인
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            raise ValueError(f"Stock not found: {ticker}")

        # 날짜 범위 설정
        if end_date is None:
            end_date = datetime.now()

        if start_date is None:
            # 마지막 수집일 조회
            last_price = db.query(func.max(StockPrice.trade_date)).filter(
                StockPrice.ticker == ticker
            ).scalar()

            if last_price:
                start_date = last_price + timedelta(days=1)
            else:
                # 최초 수집: 1년 전부터
                start_date = end_date - timedelta(days=365)

        # 이미 최신 데이터인 경우
        if start_date > end_date:
            logger.info(f"Already up to date for {ticker}")
            return {
                "ticker": ticker,
                "status": "up_to_date",
                "collected": 0
            }

        # 수집 시작 기록
        collection = CollectionHistory(
            collection_type="daily_price",
            target_date=end_date.date(),
            status="running",
            started_at=datetime.now()
        )
        db.add(collection)
        db.commit()

        try:
            # KIS API 호출
            start_date_str = self.client.format_date(start_date)
            end_date_str = self.client.format_date(end_date)

            logger.info(f"Collecting prices for {ticker} from {start_date_str} to {end_date_str}")
            response = await self.client.get_daily_price(ticker, start_date_str, end_date_str)

            # 데이터 파싱 및 저장
            prices = self._parse_price_data(ticker, response)
            saved_count = self._save_prices(db, prices)

            # 수집 완료 기록
            collection.status = "success"
            collection.total_count = len(prices)
            collection.success_count = saved_count
            collection.completed_at = datetime.now()
            db.commit()

            logger.info(f"Successfully collected {saved_count} price records for {ticker}")

            return {
                "ticker": ticker,
                "status": "success",
                "collected": saved_count,
                "start_date": start_date_str,
                "end_date": end_date_str
            }

        except Exception as e:
            logger.error(f"Error collecting prices for {ticker}: {e}")

            # 수집 실패 기록
            collection.status = "failed"
            collection.error_message = str(e)
            collection.completed_at = datetime.now()
            db.commit()

            raise

    def _parse_price_data(self, ticker: str, response: dict) -> List[dict]:
        """
        KIS API 응답 데이터 파싱

        Args:
            ticker: 종목코드
            response: KIS API 응답

        Returns:
            파싱된 가격 데이터 리스트
        """
        prices = []
        output = response.get("output2", [])

        for item in output:
            try:
                price_data = {
                    "ticker": ticker,
                    "trade_date": datetime.strptime(item["stck_bsop_date"], "%Y%m%d").date(),
                    "open": float(item.get("stck_oprc", 0)),
                    "high": float(item.get("stck_hgpr", 0)),
                    "low": float(item.get("stck_lwpr", 0)),
                    "close": float(item.get("stck_clpr", 0)),
                    "volume": int(item.get("acml_vol", 0)),
                    "trading_value": float(item.get("acml_tr_pbmn", 0)),
                    "market_cap": None,  # KIS API에서 직접 제공하지 않음
                    "change_rate": float(item.get("prdy_ctrt", 0))
                }
                prices.append(price_data)
            except (ValueError, KeyError) as e:
                logger.warning(f"Failed to parse price data: {e}, item: {item}")
                continue

        return prices

    def _save_prices(self, db: Session, prices: List[dict]) -> int:
        """
        가격 데이터 저장 (중복 시 업데이트)

        Args:
            db: 데이터베이스 세션
            prices: 저장할 가격 데이터 리스트

        Returns:
            저장된 레코드 수
        """
        saved_count = 0

        for price_data in prices:
            try:
                # 기존 레코드 확인
                existing = db.query(StockPrice).filter(
                    StockPrice.ticker == price_data["ticker"],
                    StockPrice.trade_date == price_data["trade_date"]
                ).first()

                if existing:
                    # 업데이트
                    for key, value in price_data.items():
                        setattr(existing, key, value)
                else:
                    # 신규 생성
                    price = StockPrice(**price_data)
                    db.add(price)

                saved_count += 1

            except Exception as e:
                logger.error(f"Error saving price data: {e}")
                continue

        db.commit()
        return saved_count

    async def collect_all_active_stocks(
        self,
        db: Session,
        batch_size: int = 10
    ) -> dict:
        """
        모든 활성 종목의 주가 데이터 수집

        Args:
            db: 데이터베이스 세션
            batch_size: 배치 크기

        Returns:
            수집 통계
        """
        # 활성 종목 조회
        active_stocks = db.query(Stock).filter(Stock.is_active == True).all()

        logger.info(f"Starting price collection for {len(active_stocks)} active stocks")

        total_collected = 0
        failed_tickers = []

        for i, stock in enumerate(active_stocks):
            try:
                result = await self.collect_daily_prices(db, stock.ticker)
                total_collected += result.get("collected", 0)

                # 배치 단위로 진행 상황 로그
                if (i + 1) % batch_size == 0:
                    logger.info(f"Progress: {i + 1}/{len(active_stocks)} stocks processed")

            except Exception as e:
                logger.error(f"Failed to collect prices for {stock.ticker}: {e}")
                failed_tickers.append(stock.ticker)

        return {
            "total_stocks": len(active_stocks),
            "total_collected": total_collected,
            "failed_count": len(failed_tickers),
            "failed_tickers": failed_tickers
        }
