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
        """
        최신 재무제표 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            period_type: Y(연간) 또는 Q(분기)

        Returns:
            FinancialStatement 객체 또는 None
        """
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
        """
        특정 기간 재무제표 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            stac_yymm: 결산년월 (YYYYMM)
            period_type: Y(연간) 또는 Q(분기)

        Returns:
            FinancialStatement 객체 또는 None
        """
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
        """
        재무제표 목록 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            period_type: Y(연간), Q(분기), None(전체)
            limit: 조회 개수

        Returns:
            FinancialStatement 리스트
        """
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
        """
        재무제표 개수 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            period_type: Y(연간), Q(분기), None(전체)

        Returns:
            레코드 수
        """
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

    async def collect_balance_sheet(
        self,
        ticker: str,
        period_type: str = "0"
    ) -> List[Dict[str, Any]]:
        """대차대조표 조회"""
        endpoint = "/uapi/domestic-stock/v1/finance/balance-sheet"
        tr_id = "FHKST66430200"

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
            logger.error(f"Failed to collect balance sheet for {ticker}: {e}")
            return []

    async def collect_income_statement(
        self,
        ticker: str,
        period_type: str = "0"
    ) -> List[Dict[str, Any]]:
        """손익계산서 조회"""
        endpoint = "/uapi/domestic-stock/v1/finance/income-statement"
        tr_id = "FHKST66430300"

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
            logger.error(f"Failed to collect income statement for {ticker}: {e}")
            return []

    async def collect_financial_ratios(
        self,
        ticker: str,
        period_type: str = "0"
    ) -> List[Dict[str, Any]]:
        """재무비율 조회"""
        endpoint = "/uapi/domestic-stock/v1/finance/financial-ratio"
        tr_id = "FHKST66430400"

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
            logger.error(f"Failed to collect financial ratios for {ticker}: {e}")
            return []

    async def collect_profit_ratios(
        self,
        ticker: str,
        period_type: str = "0"
    ) -> List[Dict[str, Any]]:
        """수익성비율 조회"""
        endpoint = "/uapi/domestic-stock/v1/finance/profit-ratio"
        tr_id = "FHKST66430500"

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
            logger.error(f"Failed to collect profit ratios for {ticker}: {e}")
            return []

    async def collect_other_major_ratios(
        self,
        ticker: str,
        period_type: str = "0"
    ) -> List[Dict[str, Any]]:
        """기타주요비율 조회"""
        endpoint = "/uapi/domestic-stock/v1/finance/other-major-ratios"
        tr_id = "FHKST66430600"

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
            logger.error(f"Failed to collect other major ratios for {ticker}: {e}")
            return []

    async def collect_growth_ratios(
        self,
        ticker: str,
        period_type: str = "0"
    ) -> List[Dict[str, Any]]:
        """성장성비율 조회"""
        endpoint = "/uapi/domestic-stock/v1/finance/growth-ratio"
        tr_id = "FHKST66430900"

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
            logger.error(f"Failed to collect growth ratios for {ticker}: {e}")
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

        for bs in balance_sheets:
            yymm = bs.get("stac_yymm")
            if yymm:
                merged[yymm] = bs.copy()

        for source in [income_statements, financial_ratios, profit_ratios, other_ratios, growth_ratios]:
            for item in source:
                yymm = item.get("stac_yymm")
                if yymm:
                    if yymm in merged:
                        merged[yymm].update(item)
                    else:
                        merged[yymm] = item.copy()

        sorted_data = sorted(merged.values(), key=lambda x: x.get("stac_yymm", ""), reverse=True)
        logger.info(f"Merged {len(sorted_data)} financial periods")
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

        for data in merged_data:
            try:
                stac_yymm = data.get("stac_yymm")
                if not stac_yymm:
                    continue

                # Upsert
                existing = db.query(FinancialStatement).filter(
                    and_(
                        FinancialStatement.ticker == ticker,
                        FinancialStatement.stac_yymm == stac_yymm,
                        FinancialStatement.period_type == period_char
                    )
                ).first()

                if existing:
                    # 업데이트
                    for key, value in data.items():
                        if key == "stac_yymm":
                            continue
                        if hasattr(existing, key) and value is not None:
                            setattr(existing, key, self._convert_value(key, value))
                else:
                    # 신규 삽입
                    fs_data = {
                        "ticker": ticker,
                        "stac_yymm": stac_yymm,
                        "period_type": period_char
                    }

                    for key, value in data.items():
                        if key != "stac_yymm" and value is not None:
                            fs_data[key] = self._convert_value(key, value)

                    fs = FinancialStatement(**fs_data)
                    db.add(fs)

                saved_count += 1

            except Exception as e:
                logger.error(f"Failed to save financial data for {ticker} {stac_yymm}: {e}")
                continue

        db.commit()
        logger.info(f"Saved {saved_count} financial records for {ticker} ({period_char})")
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
            if key in bigint_fields:
                return int(value)
            elif key in decimal_fields:
                return float(value)
            else:
                return value
        except (ValueError, TypeError):
            return None

    async def collect_and_save(
        self,
        db: Session,
        ticker: str,
        period_type: str = "0"
    ) -> Dict[str, Any]:
        """
        재무제표 수집 및 저장 (통합)

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            period_type: 0(연간), 1(분기)

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
                "saved": 0
            }

        # 6개 API 호출
        balance_sheets = await self.collect_balance_sheet(ticker, period_type)
        income_statements = await self.collect_income_statement(ticker, period_type)
        financial_ratios = await self.collect_financial_ratios(ticker, period_type)
        profit_ratios = await self.collect_profit_ratios(ticker, period_type)
        other_ratios = await self.collect_other_major_ratios(ticker, period_type)
        growth_ratios = await self.collect_growth_ratios(ticker, period_type)

        # 데이터 병합
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

        # 저장
        saved_count = self.save_financials(db, ticker, period_type, merged_data)

        return {
            "ticker": ticker,
            "status": "success",
            "period_type": "Y" if period_type == "0" else "Q",
            "total_periods": len(merged_data),
            "saved": saved_count
        }


def get_financial_service() -> FinancialService:
    """FinancialService 싱글톤 반환"""
    return FinancialService()