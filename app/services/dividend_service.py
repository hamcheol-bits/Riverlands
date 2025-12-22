"""
Dividend 서비스
배당 정보 조회, 수집, 관리
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from app.services.kis_client import get_kis_client
from app.models.stock import Stock
from app.models.dividend import Dividend

logger = logging.getLogger(__name__)


class DividendService:
    """
    배당 정보 서비스

    - 배당 조회 (단일/목록/최신)
    - 배당 수집 (KIS API)
    - 증분 갱신

    KIS API 파라미터:
    - CTS: 연속조회 키 (공백)
    - GB1: 조회구분 (0:전체, 1:결산배당, 2:중간배당)
    - F_DT: 조회시작일 (자동계산, 10년 전)
    - T_DT: 조회종료일 (자동계산, 오늘)
    - SHT_CD: 종목코드 (9자리, 자동변환)
    - HIGH_GB: 고배당여부 (공백)
    """

    def __init__(self):
        self.kis_client = get_kis_client()

    # ============================================================
    # 조회 기능
    # ============================================================

    def get_latest_dividend(
        self,
        db: Session,
        ticker: str
    ) -> Optional[Dividend]:
        """
        최신 배당 정보 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            Dividend 객체 또는 None
        """
        return db.query(Dividend).filter(
            Dividend.ticker == ticker
        ).order_by(
            Dividend.record_date.desc()
        ).first()

    def get_dividend_by_date(
        self,
        db: Session,
        ticker: str,
        record_date: str,
        divi_kind: Optional[str] = None
    ) -> Optional[Dividend]:
        """
        특정 날짜 배당 정보 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            record_date: 기준일 (YYYY-MM-DD)
            divi_kind: 배당종류 (선택)

        Returns:
            Dividend 객체 또는 None
        """
        date_obj = datetime.strptime(record_date, "%Y-%m-%d").date()

        query = db.query(Dividend).filter(
            and_(
                Dividend.ticker == ticker,
                Dividend.record_date == date_obj
            )
        )

        if divi_kind:
            query = query.filter(Dividend.divi_kind == divi_kind)

        return query.first()

    def get_dividends(
        self,
        db: Session,
        ticker: str,
        limit: int = 10
    ) -> List[Dividend]:
        """
        배당 목록 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            limit: 조회 개수

        Returns:
            Dividend 리스트
        """
        return db.query(Dividend).filter(
            Dividend.ticker == ticker
        ).order_by(
            Dividend.record_date.desc()
        ).limit(limit).all()

    def count_dividends(
        self,
        db: Session,
        ticker: str
    ) -> int:
        """
        배당 정보 개수 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            레코드 수
        """
        return db.query(Dividend).filter(
            Dividend.ticker == ticker
        ).count()

    def get_high_dividend_stocks(
        self,
        db: Session,
        limit: int = 20
    ) -> List[Dividend]:
        """
        고배당 종목 조회

        Args:
            db: 데이터베이스 세션
            limit: 조회 개수

        Returns:
            고배당 종목 리스트
        """
        return db.query(Dividend).filter(
            Dividend.high_divi_gb == "Y"
        ).order_by(
            Dividend.divi_rate.desc()
        ).limit(limit).all()

    # ============================================================
    # 수집 기능
    # ============================================================

    async def collect_dividends_from_kis(
        self,
        ticker: str,
        year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        KIS API로 배당 정보 수집

        Args:
            ticker: 종목코드 (6자리)
            year: 조회 연도 (예: 2025, None이면 최근 3년)

        Returns:
            배당 정보 리스트
        """
        endpoint = "/uapi/domestic-stock/v1/ksdinfo/dividend"
        tr_id = "HHKDB669102C0"

        # 조회 기간 설정
        from datetime import datetime, timedelta

        if year:
            # 특정 연도: YYYY0101 ~ YYYY1231
            f_dt = f"{year}0101"
            t_dt = f"{year}1231"
            logger.debug(f"Collecting dividends for {ticker} in year {year}")
        else:
            # 기본값: 최근 3년
            t_dt = datetime.now().strftime("%Y%m%d")
            f_dt = (datetime.now() - timedelta(days=365 * 3)).strftime("%Y%m%d")
            logger.debug(f"Collecting dividends for {ticker} (last 3 years)")

        params = {
            "CTS": "",  # 공백
            "GB1": "0",  # 0:배당전체, 1:결산배당, 2:중간배당
            "F_DT": f_dt,  # 조회일자From
            "T_DT": t_dt,  # 조회일자To
            "SHT_CD": ticker,  # 종목코드 (6자리)
            "HIGH_GB": ""  # 고배당여부 (공백)
        }

        try:
            response = await self.kis_client._request("GET", endpoint, tr_id, params)

            if response.get("rt_cd") != "0":
                logger.warning(f"API error: {response.get('msg1')}")
                return []

            dividends = response.get("output", [])
            period_info = f"year {year}" if year else "last 3 years"
            logger.info(f"Collected {len(dividends)} dividend records for {ticker} ({period_info})")
            return dividends

        except Exception as e:
            logger.error(f"Failed to collect dividends for {ticker}: {e}")
            return []

    def save_dividends(
        self,
        db: Session,
        ticker: str,
        dividends: List[Dict[str, Any]]
    ) -> int:
        """
        배당 정보 저장

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            dividends: KIS API 응답 데이터

        Returns:
            저장된 레코드 수
        """
        if not dividends:
            return 0

        saved_count = 0

        for item in dividends:
            try:
                # 날짜 변환
                record_date = self._parse_date(item.get("record_date"))
                if not record_date:
                    logger.warning(f"Invalid record_date for {ticker}: {item.get('record_date')}")
                    continue

                divi_pay_dt = self._parse_date(item.get("divi_pay_dt"))
                stk_div_pay_dt = self._parse_date(item.get("stk_div_pay_dt"))

                dividend = Dividend(
                    ticker=ticker,
                    record_date=record_date,
                    divi_kind=item.get("divi_kind"),
                    face_val=self._parse_int(item.get("face_val")),
                    per_sto_divi_amt=self._parse_decimal(item.get("per_sto_divi_amt")),
                    divi_rate=self._parse_decimal(item.get("divi_rate")),
                    stk_divi_rate=self._parse_decimal(item.get("stk_divi_rate")),
                    divi_pay_dt=divi_pay_dt,
                    stk_div_pay_dt=stk_div_pay_dt,
                    stk_kind=item.get("stk_kind"),
                    high_divi_gb=item.get("high_divi_gb")
                )

                # Upsert
                existing = db.query(Dividend).filter(
                    and_(
                        Dividend.ticker == ticker,
                        Dividend.record_date == record_date,
                        Dividend.divi_kind == item.get("divi_kind")
                    )
                ).first()

                if existing:
                    # 업데이트
                    existing.face_val = dividend.face_val
                    existing.per_sto_divi_amt = dividend.per_sto_divi_amt
                    existing.divi_rate = dividend.divi_rate
                    existing.stk_divi_rate = dividend.stk_divi_rate
                    existing.divi_pay_dt = dividend.divi_pay_dt
                    existing.stk_div_pay_dt = dividend.stk_div_pay_dt
                    existing.stk_kind = dividend.stk_kind
                    existing.high_divi_gb = dividend.high_divi_gb
                else:
                    # 삽입
                    db.add(dividend)

                saved_count += 1

            except Exception as e:
                logger.error(f"Failed to save dividend for {ticker} on {item.get('record_date')}: {e}")
                continue

        db.commit()
        logger.info(f"Saved {saved_count} dividend records for {ticker}")
        return saved_count

    async def collect_and_save(
        self,
        db: Session,
        ticker: str,
        year: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        배당 정보 수집 및 저장 (통합)

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            year: 조회 연도 (예: 2025, None이면 최근 3년)

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
        dividends = await self.collect_dividends_from_kis(ticker, year)

        if not dividends:
            return {
                "ticker": ticker,
                "status": "no_data",
                "message": "No dividend data returned",
                "collected": 0,
                "saved": 0
            }

        # 데이터 저장
        saved_count = self.save_dividends(db, ticker, dividends)

        result = {
            "ticker": ticker,
            "status": "success",
            "collected": len(dividends),
            "saved": saved_count
        }

        if year:
            result["year"] = year

        return result

    async def collect_incremental(
        self,
        db: Session,
        ticker: str
    ) -> Dict[str, Any]:
        """
        증분 수집 (마지막 기준일 이후만)

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            수집 결과
        """
        # 마지막 배당 기준일 조회
        latest = self.get_latest_dividend(db, ticker)

        if latest and latest.record_date:
            # 마지막 기준일 이후 ~ 현재까지
            from datetime import datetime

            days_since_last = (datetime.now().date() - latest.record_date).days

            # 이미 최신이면 (30일 이내)
            if days_since_last < 30:
                return {
                    "ticker": ticker,
                    "status": "up_to_date",
                    "message": f"Already have recent data (last: {latest.record_date})",
                    "collected": 0,
                    "saved": 0
                }

            # 마지막 배당 이후 올해 포함해서 조회
            current_year = datetime.now().year
            last_year = latest.record_date.year

            # 작년, 올해 포함하여 수집
            if last_year == current_year:
                # 올해만
                year = current_year
            else:
                # 작년부터 올해까지 (None으로 최근 3년 조회)
                year = None

            logger.info(f"Incremental collection for {ticker}: since {latest.record_date}, querying year={year}")
        else:
            # 배당 데이터 없으면 최근 3년 수집
            year = None
            logger.info(f"No existing dividend data for {ticker}, collecting last 3 years")

        return await self.collect_and_save(db, ticker, year)

    # ============================================================
    # 유틸리티 메서드
    # ============================================================

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """날짜 문자열을 date 객체로 변환"""
        if not date_str or date_str.strip() == "":
            return None

        try:
            # YYYYMMDD 형식
            if len(date_str) == 8:
                return datetime.strptime(date_str, "%Y%m%d").date()
            # YYYY-MM-DD 형식
            elif len(date_str) == 10:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            else:
                return None
        except ValueError:
            return None

    def _parse_int(self, value) -> Optional[int]:
        """정수 변환"""
        if value is None or value == "":
            return None
        try:
            return int(float(str(value).replace(',', '')))
        except (ValueError, TypeError):
            return None

    def _parse_decimal(self, value) -> Optional[float]:
        """소수 변환"""
        if value is None or value == "":
            return None
        try:
            return float(str(value).replace(',', ''))
        except (ValueError, TypeError):
            return None


def get_dividend_service() -> DividendService:
    """DividendService 싱글톤 반환"""
    return DividendService()