"""
Batch 서비스
시장(KOSPI/KOSDAQ/ALL) 단위 배치 처리
"""
import logging
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session

from app.models.stock import Stock
from app.services.stock_service import get_stock_service
from app.services.stock_price_service import get_stock_price_service
from app.services.financial_service import get_financial_service
from app.services.dividend_service import get_dividend_service

logger = logging.getLogger(__name__)


class BatchService:
    """
    시장 단위 배치 처리 서비스

    - 시장별 종목 정보 수집
    - 시장별 주가 수집
    - 시장별 재무제표 수집
    - 통합 수집
    """

    def __init__(self):
        self.stock_service = get_stock_service()
        self.price_service = get_stock_price_service()
        self.financial_service = get_financial_service()
        self.dividend_service = get_dividend_service()

    # ============================================================
    # 시장별 종목 정보 배치
    # ============================================================

    async def batch_collect_stocks(
        self,
        db: Session,
        market: str = "ALL",
        use_api: bool = True,
        date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        시장별 종목 정보 배치 수집

        Args:
            db: 데이터베이스 세션
            market: KOSPI, KOSDAQ, ALL
            use_api: KIS API 사용 여부
            date: 기준일 (YYYYMMDD)
            limit: 수집 제한 (테스트용)

        Returns:
            배치 수집 결과
        """
        market = market.upper()
        logger.info(f"Starting batch stock collection for {market}")

        if market == "ALL":
            # KOSPI + KOSDAQ
            kospi_result = await self.stock_service.collect_stocks_by_market(
                db, "KOSPI", use_api, date, limit
            )
            kosdaq_result = await self.stock_service.collect_stocks_by_market(
                db, "KOSDAQ", use_api, date, limit
            )

            return {
                "market": "ALL",
                "status": "success",
                "kospi": kospi_result,
                "kosdaq": kosdaq_result,
                "total_saved": kospi_result["saved"] + kosdaq_result["saved"]
            }
        else:
            # 단일 시장
            result = await self.stock_service.collect_stocks_by_market(
                db, market, use_api, date, limit
            )
            return result

    # ============================================================
    # 시장별 주가 배치
    # ============================================================

    async def batch_collect_prices(
        self,
        db: Session,
        market: str = "ALL",
        mode: str = "incremental",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        시장별 주가 배치 수집

        Args:
            db: 데이터베이스 세션
            market: KOSPI, KOSDAQ, ALL
            mode: "incremental" (증분) 또는 "full" (전체)
            start_date: 시작일 (full 모드)
            end_date: 종료일 (full 모드)
            limit: 처리 종목 수 제한

        Returns:
            배치 수집 결과
        """
        market = market.upper()
        logger.info(f"Starting batch price collection for {market}, mode: {mode}")

        # 종목 리스트 조회
        query = db.query(Stock).filter(Stock.is_active == True)

        if market != "ALL":
            query = query.filter(Stock.mrkt_ctg_cls_code == market)

        if limit:
            query = query.limit(limit)

        stocks = query.all()
        total_stocks = len(stocks)

        logger.info(f"Found {total_stocks} stocks to process")

        # 결과 집계
        success_count = 0
        total_collected = 0
        total_saved = 0
        results = []

        # 각 종목 처리
        for idx, stock in enumerate(stocks, 1):
            logger.info(f"Processing {idx}/{total_stocks}: {stock.ticker} ({stock.hts_kor_isnm})")

            try:
                if mode == "incremental":
                    result = await self.price_service.collect_incremental(db, stock.ticker)
                else:
                    result = await self.price_service.collect_and_save(
                        db, stock.ticker, start_date, end_date
                    )

                if result["status"] in ["success", "up_to_date"]:
                    success_count += 1
                    total_collected += result.get("collected", 0)
                    total_saved += result.get("saved", 0)

                results.append(result)

            except Exception as e:
                logger.error(f"Failed to process {stock.ticker}: {e}")
                results.append({
                    "ticker": stock.ticker,
                    "status": "error",
                    "message": str(e)
                })

        logger.info(
            f"Batch price collection completed: {success_count}/{total_stocks} stocks, "
            f"collected: {total_collected}, saved: {total_saved}"
        )

        return {
            "market": market,
            "mode": mode,
            "total_stocks": total_stocks,
            "success_count": success_count,
            "total_collected": total_collected,
            "total_saved": total_saved,
            "results": results
        }

    # ============================================================
    # 시장별 재무제표 배치
    # ============================================================

    async def batch_collect_financials(
            self,
            db: Session,
            market: str = "ALL",
            period_type: str = "0",
            year: Optional[int] = None,
            limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        시장별 재무제표 배치 수집

        Args:
            db: 데이터베이스 세션
            market: KOSPI, KOSDAQ, ALL
            period_type: 0(연간), 1(분기)
            year: 분기 수집시 연도 (예: 2024)
                  - 미지정시 현재 연도
                  - 연간 데이터는 무시됨
            limit: 처리 종목 수 제한

        Returns:
            배치 수집 결과
        """
        market = market.upper()
        period_char = "Y" if period_type == "0" else "Q"

        log_msg = f"Starting batch financial collection for {market}, period: {period_char}"
        if period_type == "1" and year:
            log_msg += f", year: {year}"
        logger.info(log_msg)

        # 종목 리스트 조회
        query = db.query(Stock).filter(Stock.is_active == True)

        if market != "ALL":
            query = query.filter(Stock.mrkt_ctg_cls_code == market)

        if limit:
            query = query.limit(limit)

        stocks = query.all()
        total_stocks = len(stocks)

        logger.info(f"Found {total_stocks} stocks to process")

        # 결과 집계
        success_count = 0
        total_saved = 0
        results = []

        # 각 종목 처리
        for idx, stock in enumerate(stocks, 1):
            logger.info(f"Processing {idx}/{total_stocks}: {stock.ticker} ({stock.hts_kor_isnm})")

            try:
                # period_type과 year 모두 전달
                result = await self.financial_service.collect_and_save(
                    db, stock.ticker, period_type, year
                )

                if result["status"] == "success":
                    success_count += 1
                    total_saved += result.get("saved", 0)

                results.append(result)

            except Exception as e:
                logger.error(f"Failed to process {stock.ticker}: {e}")
                results.append({
                    "ticker": stock.ticker,
                    "status": "error",
                    "message": str(e)
                })

        logger.info(
            f"Batch financial collection completed: {success_count}/{total_stocks} stocks, "
            f"saved: {total_saved}"
        )

        batch_result = {
            "market": market,
            "period_type": period_char,
            "total_stocks": total_stocks,
            "success_count": success_count,
            "total_saved": total_saved,
            "results": results
        }

        if period_type == "1" and year:
            batch_result["year"] = year

        return batch_result

    # batch_collect_all 메서드도 수정 필요
    async def batch_collect_all(
            self,
            db: Session,
            market: str = "ALL",
            include_stocks: bool = True,
            include_prices: bool = True,
            include_financials: bool = True,
            price_mode: str = "incremental",
            financial_year: Optional[int] = None,
            limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        시장별 전체 데이터 통합 수집

        Args:
            db: 데이터베이스 세션
            market: KOSPI, KOSDAQ, ALL
            include_stocks: 종목 정보 수집 여부
            include_prices: 주가 수집 여부
            include_financials: 재무제표 수집 여부
            price_mode: 주가 수집 모드 (incremental/full)
            financial_year: 분기 재무제표 수집시 연도 (미지정시 현재 연도)
            limit: 처리 종목 수 제한

        Returns:
            통합 수집 결과
        """
        market = market.upper()
        logger.info(f"Starting batch collection for {market}")

        result = {
            "market": market,
            "status": "success",
            "stocks_result": None,
            "prices_result": None,
            "annual_result": None,
            "quarterly_result": None
        }

        # 1. 종목 정보 수집
        if include_stocks:
            logger.info("Step 1: Collecting stock info...")
            try:
                stocks_result = await self.batch_collect_stocks(
                    db, market, use_api=True, limit=limit
                )
                result["stocks_result"] = stocks_result
            except Exception as e:
                logger.error(f"Failed to collect stocks: {e}")
                result["stocks_result"] = {"status": "error", "message": str(e)}

        # 2. 주가 수집
        if include_prices:
            logger.info("Step 2: Collecting prices...")
            try:
                prices_result = await self.batch_collect_prices(
                    db, market, mode=price_mode, limit=limit
                )
                result["prices_result"] = prices_result
            except Exception as e:
                logger.error(f"Failed to collect prices: {e}")
                result["prices_result"] = {"status": "error", "message": str(e)}

        # 3. 연간 재무제표 수집
        if include_financials:
            logger.info("Step 3: Collecting annual financials...")
            try:
                annual_result = await self.batch_collect_financials(
                    db, market, period_type="0", year=None, limit=limit
                )
                result["annual_result"] = annual_result
            except Exception as e:
                logger.error(f"Failed to collect annual financials: {e}")
                result["annual_result"] = {"status": "error", "message": str(e)}

            # 4. 분기 재무제표 수집 (연도 단위)
            logger.info(f"Step 4: Collecting quarterly financials (year={financial_year or 'current'})...")
            try:
                quarterly_result = await self.batch_collect_financials(
                    db, market, period_type="1", year=financial_year, limit=limit
                )
                result["quarterly_result"] = quarterly_result
            except Exception as e:
                logger.error(f"Failed to collect quarterly financials: {e}")
                result["quarterly_result"] = {"status": "error", "message": str(e)}

        logger.info(f"Batch collection completed for {market}")
        return result

    # ============================================================
    # 여러 티커 일괄 처리
    # ============================================================

    async def batch_collect_tickers(
        self,
        db: Session,
        tickers: List[str],
        include_stocks: bool = True,
        include_prices: bool = True,
        include_financials: bool = True,
        price_mode: str = "incremental"
    ) -> Dict[str, Any]:
        """
        여러 티커 일괄 수집

        Args:
            db: 데이터베이스 세션
            tickers: 종목코드 리스트
            include_stocks: 종목 정보 수집 여부
            include_prices: 주가 수집 여부
            include_financials: 재무제표 수집 여부
            price_mode: 주가 수집 모드

        Returns:
            일괄 수집 결과
        """
        logger.info(f"Starting batch collection for {len(tickers)} tickers")

        results = []
        success_count = 0

        for ticker in tickers:
            ticker_result = {
                "ticker": ticker,
                "stock_result": None,
                "price_result": None,
                "annual_result": None,
                "quarterly_result": None
            }

            # 1. 종목 정보
            if include_stocks:
                try:
                    stock_result = await self.stock_service.collect_stock(
                        db, ticker, use_api=True
                    )
                    ticker_result["stock_result"] = stock_result
                except Exception as e:
                    logger.error(f"Failed to collect stock {ticker}: {e}")
                    ticker_result["stock_result"] = {"status": "error", "message": str(e)}

            # 2. 주가
            if include_prices:
                try:
                    if price_mode == "incremental":
                        price_result = await self.price_service.collect_incremental(db, ticker)
                    else:
                        price_result = await self.price_service.collect_and_save(db, ticker)
                    ticker_result["price_result"] = price_result
                except Exception as e:
                    logger.error(f"Failed to collect prices for {ticker}: {e}")
                    ticker_result["price_result"] = {"status": "error", "message": str(e)}

            # 3. 재무제표
            if include_financials:
                try:
                    annual_result = await self.financial_service.collect_and_save(
                        db, ticker, "0"
                    )
                    ticker_result["annual_result"] = annual_result

                    quarterly_result = await self.financial_service.collect_and_save(
                        db, ticker, "1"
                    )
                    ticker_result["quarterly_result"] = quarterly_result
                except Exception as e:
                    logger.error(f"Failed to collect financials for {ticker}: {e}")
                    ticker_result["annual_result"] = {"status": "error", "message": str(e)}
                    ticker_result["quarterly_result"] = {"status": "error", "message": str(e)}

            # 성공 여부 체크
            all_success = all([
                not include_stocks or ticker_result.get("stock_result", {}).get("status") == "success",
                not include_prices or ticker_result.get("price_result", {}).get("status") in ["success", "up_to_date"],
                not include_financials or ticker_result.get("annual_result", {}).get("status") == "success"
            ])

            if all_success:
                success_count += 1

            results.append(ticker_result)

        logger.info(f"Batch collection completed: {success_count}/{len(tickers)} tickers")

        return {
            "total_tickers": len(tickers),
            "success_count": success_count,
            "results": results
        }

    # ============================================================
    # 시장별 배당 배치
    # ============================================================

    async def batch_collect_dividends(
            self,
            db: Session,
            market: str = "ALL",
            year: Optional[int] = None,
            incremental: bool = False,
            limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        시장별 배당 정보 배치 수집

        Args:
            db: 데이터베이스 세션
            market: KOSPI, KOSDAQ, ALL
            year: 조회 연도 (예: 2025, None이면 최근 3년)
            incremental: 증분 수집 여부 (기본값: False)
            limit: 처리 종목 수 제한

        Returns:
            배치 수집 결과
        """
        market = market.upper()
        period_info = f"year {year}" if year else "last 3 years"
        logger.info(f"Starting batch dividend collection for {market} ({period_info}, incremental={incremental})")

        # 종목 리스트 조회
        query = db.query(Stock).filter(Stock.is_active == True)

        if market != "ALL":
            query = query.filter(Stock.mrkt_ctg_cls_code == market)

        if limit:
            query = query.limit(limit)

        stocks = query.all()
        total_stocks = len(stocks)

        logger.info(f"Found {total_stocks} stocks to process")

        # 결과 집계
        success_count = 0
        total_collected = 0
        total_saved = 0
        results = []

        # 각 종목 처리
        for idx, stock in enumerate(stocks, 1):
            ticker = stock.ticker
            name = stock.hts_kor_isnm

            logger.info(f"Processing {idx}/{total_stocks}: {ticker} ({name})")

            try:
                if incremental:
                    result = await self.dividend_service.collect_incremental(db, ticker)
                else:
                    result = await self.dividend_service.collect_and_save(db, ticker, year)

                if result["status"] in ["success", "up_to_date"]:
                    success_count += 1
                    total_collected += result.get("collected", 0)
                    total_saved += result.get("saved", 0)

                results.append(result)

            except Exception as e:
                db.rollback()
                logger.error(f"Failed to process {ticker}: {e}")
                results.append({
                    "ticker": ticker,
                    "status": "error",
                    "message": str(e)
                })

        logger.info(
            f"Batch dividend collection completed: {success_count}/{total_stocks} stocks, "
            f"collected: {total_collected}, saved: {total_saved}"
        )

        batch_result = {
            "market": market,
            "incremental": incremental,
            "total_stocks": total_stocks,
            "success_count": success_count,
            "total_collected": total_collected,
            "total_saved": total_saved,
            "results": results
        }

        if year:
            batch_result["year"] = year

        return batch_result


    # ============================================================
    # 시장별 투자의견 배치 (batch_service.py에 추가)
    # ============================================================

    async def batch_collect_investment_opinions(
            self,
            db: Session,
            market: str = "ALL",
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        시장별 투자의견 컨센서스 배치 수집

        Args:
            db: 데이터베이스 세션
            market: KOSPI, KOSDAQ, ALL
            start_date: 시작일 (YYYYMMDD), 기본값: 6개월 전
            end_date: 종료일 (YYYYMMDD), 기본값: 오늘
            limit: 처리 종목 수 제한

        Returns:
            배치 수집 결과
        """
        from app.services.investment_opinion_service import get_investment_opinion_service

        market = market.upper()
        logger.info(f"Starting batch investment opinion collection for {market}")

        # 종목 리스트 조회
        query = db.query(Stock).filter(Stock.is_active == True)

        if market != "ALL":
            query = query.filter(Stock.mrkt_ctg_cls_code == market)

        if limit:
            query = query.limit(limit)

        stocks = query.all()
        total_stocks = len(stocks)

        logger.info(f"Found {total_stocks} stocks to process")

        # 투자의견 서비스
        opinion_service = get_investment_opinion_service()

        # 결과 집계
        success_count = 0
        total_collected = 0
        total_updated = 0
        results = []

        # 각 종목 처리
        for idx, stock in enumerate(stocks, 1):
            ticker = stock.ticker
            name = stock.hts_kor_isnm

            logger.info(f"Processing {idx}/{total_stocks}: {ticker} ({name})")

            try:
                result = await opinion_service.collect_investment_opinions(
                    db, ticker, start_date, end_date
                )

                if result["message"] == "Success":
                    success_count += 1
                    total_collected += result.get("collected", 0)
                    total_updated += result.get("updated", 0)

                results.append(result)

            except Exception as e:
                db.rollback()
                logger.error(f"Failed to process {ticker}: {e}")
                results.append({
                    "ticker": ticker,
                    "status": "error",
                    "message": str(e)
                })

        logger.info(
            f"Batch investment opinion collection completed: {success_count}/{total_stocks} stocks, "
            f"collected: {total_collected}, updated: {total_updated}"
        )

        return {
            "market": market,
            "total_stocks": total_stocks,
            "success_count": success_count,
            "total_collected": total_collected,
            "total_updated": total_updated,
            "results": results
        }

def get_batch_service() -> BatchService:
    """BatchService 싱글톤 반환"""
    return BatchService()