"""
Financial 서비스 (개선 버전)
재무제표 데이터 조회, 수집, 관리

주요 개선사항:
- 분기 데이터를 누적이 아닌 분기별 실적으로 저장
- 연도 단위로 분기 데이터 수집 및 변환
"""
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from app.services.kis_client import get_kis_client
from app.models.stock import Stock
from app.models.financial_statement import FinancialStatement

logger = logging.getLogger(__name__)


class FinancialService:
    """
    재무제표 데이터 서비스

    - 재무제표 조회 (연간/분기/최신)
    - 재무제표 수집 (6개 KIS API 통합)
    - 분기 데이터 실적 변환 및 저장
    """

    def __init__(self):
        self.kis_client = get_kis_client()

    # ============================================================
    # 조회 기능 (변경 없음)
    # ============================================================

    def get_latest_financial(
        self,
        db: Session,
        ticker: str,
        period_type: str = "Y"
    ) -> Optional[FinancialStatement]:
        return db.query(FinancialStatement).filter(
            and_(
                FinancialStatement.ticker == ticker,
                FinancialStatement.period_type == period_type.upper()
            )
        ).order_by(
            FinancialStatement.stac_yymm.desc()
        ).first()

    def get_financial_by_period(
        self,
        db: Session,
        ticker: str,
        stac_yymm: str,
        period_type: str = "Y"
    ) -> Optional[FinancialStatement]:
        return db.query(FinancialStatement).filter(
            and_(
                FinancialStatement.ticker == ticker,
                FinancialStatement.stac_yymm == stac_yymm,
                FinancialStatement.period_type == period_type.upper()
            )
        ).first()

    def get_financials(
        self,
        db: Session,
        ticker: str,
        period_type: Optional[str] = None,
        limit: int = 10
    ) -> List[FinancialStatement]:
        query = db.query(FinancialStatement).filter(
            FinancialStatement.ticker == ticker
        )

        if period_type:
            query = query.filter(
                FinancialStatement.period_type == period_type.upper()
            )

        return query.order_by(
            FinancialStatement.stac_yymm.desc()
        ).limit(limit).all()

    def count_financials(
        self,
        db: Session,
        ticker: str,
        period_type: Optional[str] = None
    ) -> int:
        query = db.query(FinancialStatement).filter(
            FinancialStatement.ticker == ticker
        )

        if period_type:
            query = query.filter(
                FinancialStatement.period_type == period_type.upper()
            )

        return query.count()

    # ============================================================
    # KIS API 수집 기능 (변경 없음)
    # ============================================================

    async def collect_balance_sheet(self, ticker: str, period_type: str = "0") -> List[Dict[str, Any]]:
        endpoint = "/uapi/domestic-stock/v1/finance/balance-sheet"
        tr_id = "FHKST66430100"
        return await self._fetch_data(endpoint, tr_id, ticker, period_type)

    async def collect_income_statement(self, ticker: str, period_type: str = "0") -> List[Dict[str, Any]]:
        endpoint = "/uapi/domestic-stock/v1/finance/income-statement"
        tr_id = "FHKST66430200"
        return await self._fetch_data(endpoint, tr_id, ticker, period_type)

    async def collect_financial_ratios(self, ticker: str, period_type: str = "0") -> List[Dict[str, Any]]:
        endpoint = "/uapi/domestic-stock/v1/finance/financial-ratio"
        tr_id = "FHKST66430300"
        return await self._fetch_data(endpoint, tr_id, ticker, period_type)

    async def collect_profit_ratios(self, ticker: str, period_type: str = "0") -> List[Dict[str, Any]]:
        endpoint = "/uapi/domestic-stock/v1/finance/profit-ratio"
        tr_id = "FHKST66430400"
        return await self._fetch_data(endpoint, tr_id, ticker, period_type)

    async def collect_other_major_ratios(self, ticker: str, period_type: str = "0") -> List[Dict[str, Any]]:
        endpoint = "/uapi/domestic-stock/v1/finance/other-major-ratios"
        tr_id = "FHKST66430500"
        return await self._fetch_data(endpoint, tr_id, ticker, period_type)

    async def collect_growth_ratios(self, ticker: str, period_type: str = "0") -> List[Dict[str, Any]]:
        endpoint = "/uapi/domestic-stock/v1/finance/growth-ratio"
        tr_id = "FHKST66430800"
        return await self._fetch_data(endpoint, tr_id, ticker, period_type)

    async def _fetch_data(self, endpoint: str, tr_id: str, ticker: str, period_type: str) -> List[Dict[str, Any]]:
        """API 호출 공통 메서드"""
        params = {
            "FID_DIV_CLS_CODE": period_type,
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": ticker
        }
        try:
            response = await self.kis_client._request("GET", endpoint, tr_id, params)
            if response.get("rt_cd") != "0":
                return []
            return response.get("output", [])
        except Exception as e:
            logger.error(f"Failed to collect data from {endpoint} for {ticker}: {e}")
            return []

    # ============================================================
    # 데이터 병합 (변경 없음)
    # ============================================================

    def merge_financial_data(
        self,
        balance_sheets: List[Dict],
        income_statements: List[Dict],
        financial_ratios: List[Dict],
        profit_ratios: List[Dict],
        other_ratios: List[Dict],
        growth_ratios: List[Dict]
    ) -> List[Dict[str, Any]]:
        """stac_yymm 기준으로 데이터 병합"""
        merged = {}

        sources = [
            balance_sheets, income_statements, financial_ratios,
            profit_ratios, other_ratios, growth_ratios
        ]

        for source in sources:
            if not source:
                continue
            for item in source:
                yymm = item.get("stac_yymm")
                if yymm:
                    if yymm in merged:
                        merged[yymm].update(item)
                    else:
                        merged[yymm] = item.copy()

        sorted_data = sorted(merged.values(), key=lambda x: x.get("stac_yymm", ""), reverse=True)
        return sorted_data

    # ============================================================
    # 연간 재무제표 저장 (기존 로직 유지)
    # ============================================================

    def save_financials(
        self,
        db: Session,
        ticker: str,
        period_type: str,
        merged_data: List[Dict[str, Any]]
    ) -> int:
        """연간 재무제표 저장"""
        if not merged_data:
            return 0

        saved_count = 0
        period_char = "Y" if period_type == "0" else "Q"
        valid_columns = {c.name for c in FinancialStatement.__table__.columns}

        for data in merged_data:
            try:
                stac_yymm = data.get("stac_yymm")
                if not stac_yymm:
                    continue

                existing = db.query(FinancialStatement).filter(
                    and_(
                        FinancialStatement.ticker == ticker,
                        FinancialStatement.stac_yymm == stac_yymm,
                        FinancialStatement.period_type == period_char
                    )
                ).first()

                if existing:
                    for key, value in data.items():
                        if key == "stac_yymm":
                            continue
                        if key in valid_columns and hasattr(existing, key) and value is not None:
                            converted_val = self._convert_value(key, value)
                            if converted_val is not None:
                                setattr(existing, key, converted_val)
                else:
                    fs_data = {
                        "ticker": ticker,
                        "stac_yymm": stac_yymm,
                        "period_type": period_char
                    }
                    for key, value in data.items():
                        if key != "stac_yymm" and value is not None and key in valid_columns:
                            converted_val = self._convert_value(key, value)
                            if converted_val is not None:
                                fs_data[key] = converted_val

                    fs = FinancialStatement(**fs_data)
                    db.add(fs)

                saved_count += 1

            except Exception as e:
                logger.error(f"Failed to save financial data for {ticker} {stac_yymm}: {e}")
                continue

        db.commit()
        return saved_count

    def _convert_value(self, key: str, value):
        """데이터 타입 변환"""
        bigint_fields = ['cras', 'fxas', 'total_aset', 'flow_lblt', 'fix_lblt',
                        'total_lblt', 'cpfn', 'total_cptl', 'sale_account',
                        'sale_cost', 'sale_totl_prfi', 'bsop_prti', 'op_prfi',
                        'spec_prfi', 'thtr_ntin', 'eva', 'ebitda']

        decimal_fields = ['eps', 'sps', 'bps', 'grs', 'bsop_prfi_inrt', 'ntin_inrt',
                         'roe_val', 'rsrv_rate', 'lblt_rate', 'cptl_ntin_rate',
                         'self_cptl_ntin_inrt', 'sale_ntin_rate', 'sale_totl_rate',
                         'ev_ebitda', 'equt_inrt', 'totl_aset_inrt']

        try:
            if value is None:
                return None

            if isinstance(value, str):
                value = value.replace(',', '').strip()
                if not value:
                    return None

            if key in bigint_fields:
                return int(float(value))
            elif key in decimal_fields:
                return float(value)
            else:
                return value
        except (ValueError, TypeError):
            return None

    # ============================================================
    # 개선된 수집 및 저장 로직
    # ============================================================

    async def collect_and_save(
        self,
        db: Session,
        ticker: str,
        period_type: str = "0",
        year: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        재무제표 수집 및 저장 (통합)

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            period_type: "0" (연간) 또는 "1" (분기)
            year: 분기 데이터 수집시 연도 (예: 2024, 2025)
                  연간 데이터는 year 파라미터 무시

        Returns:
            수집 결과
        """
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return {
                "ticker": ticker,
                "status": "error",
                "message": "Stock not found",
                "saved": 0
            }

        # 연간 데이터는 기존 로직 유지
        if period_type == "0":
            balance_sheets = await self.collect_balance_sheet(ticker, period_type)
            income_statements = await self.collect_income_statement(ticker, period_type)
            financial_ratios = await self.collect_financial_ratios(ticker, period_type)
            profit_ratios = await self.collect_profit_ratios(ticker, period_type)
            other_ratios = await self.collect_other_major_ratios(ticker, period_type)
            growth_ratios = await self.collect_growth_ratios(ticker, period_type)

            merged_data = self.merge_financial_data(
                balance_sheets, income_statements, financial_ratios,
                profit_ratios, other_ratios, growth_ratios
            )

            if not merged_data:
                return {
                    "ticker": ticker,
                    "status": "no_data",
                    "message": "No financial data returned",
                    "saved": 0
                }

            saved_count = self.save_financials(db, ticker, period_type, merged_data)

            return {
                "ticker": ticker,
                "status": "success",
                "period_type": "Y",
                "total_periods": len(merged_data),
                "saved": saved_count
            }

        # 분기 데이터는 새로운 로직 적용
        else:
            if year is None:
                from datetime import datetime
                year = datetime.now().year

            return await self.collect_and_save_quarterly(db, ticker, year)

    async def collect_and_save_quarterly(
        self,
        db: Session,
        ticker: str,
        year: int
    ) -> Dict[str, Any]:
        """
        분기 재무제표 수집 및 저장 (연도 단위)

        분기 데이터는 KIS API에서 누적 합산으로 제공되므로,
        Q1, Q2, Q3, Q4를 모두 수집한 후 분기별 실적으로 변환하여 저장

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            year: 조회 연도 (예: 2024, 2025)

        Returns:
            수집 결과
        """
        from datetime import datetime

        current_year = datetime.now().year
        current_month = datetime.now().month

        # 현재 연도면 현재 분기까지만, 과거 연도면 Q4까지
        if year == current_year:
            max_quarter = (current_month - 1) // 3 + 1
        else:
            max_quarter = 4

        logger.info(f"Collecting quarterly data for {ticker} - {year} (Q1~Q{max_quarter})")

        # 1. 전체 분기 데이터 수집 (누적)
        balance_sheets = await self.collect_balance_sheet(ticker, "1")
        income_statements = await self.collect_income_statement(ticker, "1")
        financial_ratios = await self.collect_financial_ratios(ticker, "1")
        profit_ratios = await self.collect_profit_ratios(ticker, "1")
        other_ratios = await self.collect_other_major_ratios(ticker, "1")
        growth_ratios = await self.collect_growth_ratios(ticker, "1")

        # 2. 데이터 병합
        merged_data = self.merge_financial_data(
            balance_sheets, income_statements, financial_ratios,
            profit_ratios, other_ratios, growth_ratios
        )

        if not merged_data:
            return {
                "ticker": ticker,
                "year": year,
                "status": "no_data",
                "message": "No quarterly data returned",
                "saved": 0
            }

        # 3. 해당 연도 데이터만 필터링 및 분기별 정렬
        year_data = [
            item for item in merged_data
            if item.get("stac_yymm", "")[:4] == str(year)
        ]

        if not year_data:
            return {
                "ticker": ticker,
                "year": year,
                "status": "no_data",
                "message": f"No data for year {year}",
                "saved": 0
            }

        # stac_yymm 오름차순 정렬 (Q1 -> Q4)
        year_data.sort(key=lambda x: x.get("stac_yymm", ""))

        logger.info(f"Found {len(year_data)} quarters for {year}: {[d.get('stac_yymm') for d in year_data]}")

        # 4. 분기별 실적 계산 및 저장
        saved_count = self._save_quarterly_actuals(db, ticker, year_data)

        return {
            "ticker": ticker,
            "year": year,
            "status": "success",
            "period_type": "Q",
            "quarters_collected": len(year_data),
            "saved": saved_count
        }

    def _save_quarterly_actuals(
        self,
        db: Session,
        ticker: str,
        year_data: List[Dict[str, Any]]
    ) -> int:
        """
        분기별 실적 계산 및 저장

        누적 데이터를 분기 실적으로 변환:
        - Q1: 누적 Q1
        - Q2: 누적 Q2 - 누적 Q1
        - Q3: 누적 Q3 - 누적 Q2
        - Q4: 누적 Q4 - 누적 Q3

        대차대조표(스톡 데이터)는 누적 개념이 아니므로 그대로 사용
        손익계산서(플로우 데이터)는 차감 계산

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            year_data: 분기별 누적 데이터 (정렬된 상태)

        Returns:
            저장된 레코드 수
        """
        saved_count = 0
        valid_columns = {c.name for c in FinancialStatement.__table__.columns}

        # 손익계산서 항목 (누적 합산이므로 차감 필요)
        cumulative_fields = {
            'sale_account', 'sale_cost', 'sale_totl_prfi',
            'bsop_prti', 'op_prfi', 'spec_prfi', 'thtr_ntin'
        }

        # 이전 분기 누적값 저장
        prev_quarter = None

        for idx, current_quarter in enumerate(year_data):
            try:
                stac_yymm = current_quarter.get("stac_yymm")
                if not stac_yymm:
                    continue

                logger.debug(f"Processing quarter: {stac_yymm}")

                # 분기 실적 데이터 생성
                actual_data = {}

                for key, value in current_quarter.items():
                    if key == "stac_yymm" or value is None:
                        continue

                    if key not in valid_columns:
                        continue

                    # 손익계산서 항목은 차감 계산
                    if key in cumulative_fields and prev_quarter is not None:
                        prev_value = prev_quarter.get(key)

                        if prev_value is not None:
                            # 이전 분기 차감
                            converted_current = self._convert_value(key, value)
                            converted_prev = self._convert_value(key, prev_value)

                            if converted_current is not None and converted_prev is not None:
                                actual_value = converted_current - converted_prev
                                actual_data[key] = actual_value
                            else:
                                actual_data[key] = converted_current
                        else:
                            actual_data[key] = self._convert_value(key, value)
                    else:
                        # 대차대조표, 비율 등은 그대로 사용
                        actual_data[key] = self._convert_value(key, value)

                # DB 저장
                existing = db.query(FinancialStatement).filter(
                    and_(
                        FinancialStatement.ticker == ticker,
                        FinancialStatement.stac_yymm == stac_yymm,
                        FinancialStatement.period_type == "Q"
                    )
                ).first()

                if existing:
                    # 업데이트
                    for key, value in actual_data.items():
                        if value is not None and hasattr(existing, key):
                            setattr(existing, key, value)

                    logger.debug(f"Updated Q data: {ticker} {stac_yymm}")
                else:
                    # 신규 삽입
                    fs_data = {
                        "ticker": ticker,
                        "stac_yymm": stac_yymm,
                        "period_type": "Q"
                    }
                    fs_data.update(actual_data)

                    fs = FinancialStatement(**fs_data)
                    db.add(fs)

                    logger.debug(f"Inserted Q data: {ticker} {stac_yymm}")

                saved_count += 1

                # 다음 분기를 위해 현재 분기를 이전 분기로 저장
                prev_quarter = current_quarter

            except Exception as e:
                logger.error(f"Failed to save quarterly actual for {ticker} {stac_yymm}: {e}")
                continue

        db.commit()
        logger.info(f"Saved {saved_count} quarterly actuals for {ticker}")

        return saved_count


def get_financial_service() -> FinancialService:
    return FinancialService()