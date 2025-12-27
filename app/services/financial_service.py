"""
Financial 서비스
재무제표 데이터 조회, 수집, 관리
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
    - 데이터 병합 및 저장
    """

    def __init__(self):
        self.kis_client = get_kis_client()

    # ============================================================
    # 조회 기능
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
    # KIS API 수집 기능 (6개 API)
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
    # 데이터 병합 및 저장
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

        # 모든 데이터 소스를 순회하며 병합
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

    def save_financials(
        self,
        db: Session,
        ticker: str,
        period_type: str,
        merged_data: List[Dict[str, Any]]
    ) -> int:
        """재무제표 데이터 저장"""
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
                            # 변환된 값이 None이 아니거나, 원래 의도가 NULL인 경우 처리
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
        """데이터 타입 변환 (수정됨: 소수점 포함 문자열 처리)"""
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

            # 문자열인 경우 쉼표 제거
            if isinstance(value, str):
                value = value.replace(',', '').strip()
                if not value:
                    return None

            if key in bigint_fields:
                # "123.00" 문자열을 int로 바로 변환하면 에러 발생
                # float로 먼저 변환 후 int로 캐스팅
                return int(float(value))
            elif key in decimal_fields:
                return float(value)
            else:
                return value
        except (ValueError, TypeError):
            # 변환 실패 시 로그를 남기는 것이 좋으나, 너무 많을 수 있으므로 생략하거나 debug로 처리
            return None

    async def collect_and_save(
        self,
        db: Session,
        ticker: str,
        period_type: str = "0"
    ) -> Dict[str, Any]:
        """재무제표 수집 및 저장 (통합)"""
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return {
                "ticker": ticker,
                "status": "error",
                "message": "Stock not found",
                "saved": 0
            }

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

        # ✅ 연간 재무제표 저장 후 밸류에이션 갱신
        if saved_count > 0 and period_type == "0":  # 연간만
            from app.services.valuation_service import get_valuation_service
            valuation_service = get_valuation_service()
            valuation_service.update_valuation_for_ticker(db, ticker)

        return {
            "ticker": ticker,
            "status": "success",
            "period_type": "Y" if period_type == "0" else "Q",
            "total_periods": len(merged_data),
            "saved": saved_count
        }


def get_financial_service() -> FinancialService:
    return FinancialService()