"""
밸류에이션 계산 서비스 (완전판)
app/services/valuation_service.py

TTM 계산, 캐시 관리, 스크리닝 기능 포함
"""
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text, and_, desc

from app.models.stock import Stock
from app.models.financial_statement import FinancialStatement

logger = logging.getLogger(__name__)


class ValuationService:
    """
    밸류에이션 지표 계산 및 캐시 관리

    주요 기능:
    1. TTM (Trailing Twelve Months) 계산
    2. 연간 밸류에이션 계산
    3. 캐시 관리 (stock_valuation_cache)
    4. 스크리닝
    """

    # ============================================================
    # TTM 계산 (핵심 기능)
    # ============================================================

    def calculate_ttm_valuation(
        self,
        db: Session,
        ticker: str,
        as_of_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        TTM 밸류에이션 계산

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            as_of_date: 기준일 (YYYYMM), None이면 최신 분기

        Returns:
            TTM 계산 결과
        """
        try:
            # 1. 기준 분기 결정
            if as_of_date:
                base_quarter = as_of_date
            else:
                # 최신 분기 조회
                latest = db.query(FinancialStatement.stac_yymm).filter(
                    and_(
                        FinancialStatement.ticker == ticker,
                        FinancialStatement.period_type == "Q"
                    )
                ).order_by(FinancialStatement.stac_yymm.desc()).first()

                if not latest:
                    return {
                        "status": "error",
                        "message": "No quarterly data available"
                    }

                base_quarter = latest.stac_yymm

            # 2. 최근 4개 분기 조회
            quarters = db.query(FinancialStatement).filter(
                and_(
                    FinancialStatement.ticker == ticker,
                    FinancialStatement.period_type == "Q",
                    FinancialStatement.stac_yymm <= base_quarter
                )
            ).order_by(FinancialStatement.stac_yymm.desc()).limit(4).all()

            if len(quarters) < 4:
                return {
                    "status": "error",
                    "message": f"Insufficient quarterly data (found {len(quarters)}, need 4)"
                }

            # 3. TTM 합산 (Flow 필드)
            ttm_sales = sum(q.sale_account or 0 for q in quarters)
            ttm_operating_income = sum(q.bsop_prti or 0 for q in quarters)
            ttm_net_income = sum(q.thtr_ntin or 0 for q in quarters)

            # 4. 주식 수 조회 (최신 분기 기준)
            latest_quarter = quarters[0]

            # cpfn (자본금)이 있으면 주식 수 계산
            shares_outstanding = None
            if latest_quarter.cpfn:
                # 액면가 5,000원 가정
                shares_outstanding = (latest_quarter.cpfn or 0) / 5000

            # 5. EPS 계산
            eps_ttm = None
            if shares_outstanding and shares_outstanding > 0:
                eps_ttm = ttm_net_income / shares_outstanding

            # 6. 현재가 조회
            price_result = db.execute(
                text("""
                    SELECT stck_clpr, stck_bsop_date
                    FROM stock_prices
                    WHERE ticker = :ticker
                    ORDER BY stck_bsop_date DESC
                    LIMIT 1
                """),
                {"ticker": ticker}
            ).fetchone()

            current_price = None
            price_date = None
            per_ttm = None

            if price_result:
                current_price = float(price_result.stck_clpr)
                price_date = price_result.stck_bsop_date

                # PER 계산
                if eps_ttm and eps_ttm > 0:
                    per_ttm = current_price / eps_ttm

            return {
                "status": "success",
                "ticker": ticker,
                "base_quarter": base_quarter,
                "quarters_used": [q.stac_yymm for q in quarters],
                "ttm": {
                    "sales": ttm_sales,
                    "operating_income": ttm_operating_income,
                    "net_income": ttm_net_income,
                    "eps": round(eps_ttm, 2) if eps_ttm else None,
                    "per": round(per_ttm, 2) if per_ttm else None
                },
                "current_price": current_price,
                "price_date": price_date.isoformat() if price_date else None
            }

        except Exception as e:
            logger.error(f"Failed to calculate TTM for {ticker}: {e}")
            return {
                "status": "error",
                "message": str(e)
            }

    def get_ttm_summary(
        self,
        db: Session,
        ticker: str,
        as_of_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        TTM 요약 정보 조회 (TTM + 연간 비교)

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            as_of_date: 기준일 (YYYYMM)

        Returns:
            TTM 요약 정보
        """
        # 종목 정보 조회
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return {
                "status": "error",
                "message": f"Stock {ticker} not found"
            }

        # TTM 계산
        ttm_result = self.calculate_ttm_valuation(db, ticker, as_of_date)

        if ttm_result["status"] == "error":
            return ttm_result

        # 최신 연간 데이터 조회
        annual = self.get_latest_financial(db, ticker, "Y")

        annual_data = None
        if annual:
            # 연간 EPS, PER 계산
            annual_eps = None
            annual_per = None

            shares_outstanding = None
            if annual.cpfn:
                shares_outstanding = (annual.cpfn or 0) / 5000

            if shares_outstanding and shares_outstanding > 0 and annual.thtr_ntin:
                annual_eps = annual.thtr_ntin / shares_outstanding

            if annual_eps and annual_eps > 0 and ttm_result.get("current_price"):
                annual_per = ttm_result["current_price"] / annual_eps

            annual_data = {
                "year": annual.stac_yymm,
                "sales": annual.sale_account,
                "operating_income": annual.bsop_prti,
                "net_income": annual.thtr_ntin,
                "eps": round(annual_eps, 2) if annual_eps else None,
                "per": round(annual_per, 2) if annual_per else None,
                "roe": float(annual.roe_val) if annual.roe_val else None
            }

        return {
            "status": "success",
            "ticker": ticker,
            "stock_name": stock.hts_kor_isnm,
            "current_price": ttm_result.get("current_price"),
            "price_date": ttm_result.get("price_date"),
            "ttm": ttm_result["ttm"],
            "ttm_base_quarter": ttm_result["base_quarter"],
            "ttm_quarters_used": ttm_result["quarters_used"],
            "annual": annual_data
        }

    def get_quarterly_eps_trend(
        self,
        db: Session,
        ticker: str,
        limit: int = 8
    ) -> List[Dict[str, Any]]:
        """
        분기별 EPS 추이 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            limit: 조회 분기 수

        Returns:
            분기별 EPS 리스트
        """
        try:
            quarters = db.query(FinancialStatement).filter(
                and_(
                    FinancialStatement.ticker == ticker,
                    FinancialStatement.period_type == "Q"
                )
            ).order_by(FinancialStatement.stac_yymm.desc()).limit(limit).all()

            trend = []
            for q in quarters:
                shares_outstanding = None
                if q.cpfn:
                    shares_outstanding = (q.cpfn or 0) / 5000

                eps = None
                if shares_outstanding and shares_outstanding > 0 and q.thtr_ntin:
                    eps = q.thtr_ntin / shares_outstanding

                trend.append({
                    "quarter": q.stac_yymm,
                    "net_income": q.thtr_ntin,
                    "eps": round(eps, 2) if eps else None,
                    "roe": float(q.roe_val) if q.roe_val else None
                })

            return trend

        except Exception as e:
            logger.error(f"Failed to get quarterly EPS trend for {ticker}: {e}")
            return []

    # ============================================================
    # 연간 밸류에이션 (기존 기능)
    # ============================================================

    def get_latest_financial(
        self,
        db: Session,
        ticker: str,
        period_type: str = "Y"
    ) -> Optional[FinancialStatement]:
        """최신 재무제표 조회"""
        return db.query(FinancialStatement).filter(
            and_(
                FinancialStatement.ticker == ticker,
                FinancialStatement.period_type == period_type.upper()
            )
        ).order_by(
            FinancialStatement.stac_yymm.desc()
        ).first()

    def update_valuation_cache(
        self,
        db: Session,
        ticker: str
    ) -> Dict[str, Any]:
        """
        단일 종목 밸류에이션 갱신 (연간 기준)

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            갱신 결과
        """
        try:
            # MySQL 프로시저 호출
            db.execute(
                text("CALL update_valuation_cache(:ticker)"),
                {"ticker": ticker}
            )
            db.commit()

            # 결과 조회
            result = db.execute(
                text("""
                     SELECT ticker,
                            current_price,
                            price_date,
                            eps,
                            per,
                            bps,
                            pbr,
                            roe_val,
                            last_calculated_at
                     FROM stock_valuation_cache
                     WHERE ticker = :ticker
                     """),
                {"ticker": ticker}
            ).fetchone()

            if result:
                return {
                    "status": "success",
                    "ticker": ticker,
                    "valuation": {
                        "current_price": float(result.current_price) if result.current_price else None,
                        "price_date": result.price_date.isoformat() if result.price_date else None,
                        "eps": float(result.eps) if result.eps else None,
                        "per": float(result.per) if result.per else None,
                        "bps": float(result.bps) if result.bps else None,
                        "pbr": float(result.pbr) if result.pbr else None,
                        "roe_val": float(result.roe_val) if result.roe_val else None,
                        "last_calculated_at": result.last_calculated_at.isoformat()
                    }
                }
            else:
                return {
                    "status": "no_data",
                    "ticker": ticker,
                    "message": "No price or financial data available"
                }

        except Exception as e:
            logger.error(f"Failed to update valuation for {ticker}: {e}")
            db.rollback()
            return {
                "status": "error",
                "ticker": ticker,
                "message": str(e)
            }

    def update_all_valuation_cache(
        self,
        db: Session,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        전체 종목 밸류에이션 갱신

        Args:
            db: 데이터베이스 세션
            limit: 처리 종목 수 제한 (None이면 전체)

        Returns:
            갱신 결과
        """
        try:
            if limit:
                # 제한된 개수만 처리
                stocks = db.query(Stock).filter(
                    Stock.is_active == True
                ).limit(limit).all()

                success_count = 0
                for stock in stocks:
                    result = self.update_valuation_cache(db, stock.ticker)
                    if result["status"] == "success":
                        success_count += 1

                return {
                    "status": "success",
                    "total_processed": len(stocks),
                    "success_count": success_count,
                    "message": f"Updated {success_count}/{len(stocks)} stocks"
                }
            else:
                # 전체 처리 (프로시저 사용)
                db.execute(text("CALL update_all_valuation_cache()"))
                db.commit()

                # 갱신된 개수 확인
                count = db.execute(
                    text("SELECT COUNT(*) FROM stock_valuation_cache")
                ).scalar()

                return {
                    "status": "success",
                    "total_cached": count,
                    "message": f"Updated all valuations ({count} stocks)"
                }

        except Exception as e:
            logger.error(f"Failed to update all valuations: {e}")
            db.rollback()
            return {
                "status": "error",
                "message": str(e)
            }

    def get_valuation(
        self,
        db: Session,
        ticker: str,
        use_cache: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        종목 밸류에이션 조회

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드
            use_cache: 캐시 사용 여부 (False면 VIEW 사용)

        Returns:
            밸류에이션 정보
        """
        try:
            if use_cache:
                # 캐시에서 조회
                result = db.execute(
                    text("""
                         SELECT ticker,
                                current_price,
                                price_date,
                                eps,
                                per,
                                bps,
                                pbr,
                                roe_val,
                                stac_yymm,
                                last_calculated_at
                         FROM stock_valuation_cache
                         WHERE ticker = :ticker
                         """),
                    {"ticker": ticker}
                ).fetchone()
            else:
                # VIEW에서 실시간 계산
                result = db.execute(
                    text("""
                         SELECT ticker,
                                current_price,
                                price_date,
                                eps,
                                per,
                                bps,
                                pbr,
                                roe_val,
                                stac_yymm
                         FROM v_stock_valuation
                         WHERE ticker = :ticker
                         """),
                    {"ticker": ticker}
                ).fetchone()

            if not result:
                return None

            return {
                "ticker": result.ticker,
                "current_price": float(result.current_price) if result.current_price else None,
                "price_date": result.price_date.isoformat() if result.price_date else None,
                "eps": float(result.eps) if result.eps else None,
                "per": float(result.per) if result.per else None,
                "bps": float(result.bps) if result.bps else None,
                "pbr": float(result.pbr) if result.pbr else None,
                "roe_val": float(result.roe_val) if result.roe_val else None,
                "stac_yymm": result.stac_yymm,
                "last_calculated_at": result.last_calculated_at.isoformat() if hasattr(result,
                                                                                       'last_calculated_at') and result.last_calculated_at else None
            }

        except Exception as e:
            logger.error(f"Failed to get valuation for {ticker}: {e}")
            return None

    # ============================================================
    # 스크리닝
    # ============================================================

    def screen_stocks(
        self,
        db: Session,
        min_per: Optional[float] = None,
        max_per: Optional[float] = None,
        min_pbr: Optional[float] = None,
        max_pbr: Optional[float] = None,
        min_roe: Optional[float] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        밸류에이션 기준 스크리닝

        Args:
            db: 데이터베이스 세션
            min_per: 최소 PER
            max_per: 최대 PER
            min_pbr: 최소 PBR
            max_pbr: 최대 PBR
            min_roe: 최소 ROE
            limit: 결과 개수 제한

        Returns:
            스크리닝 결과
        """
        conditions = []
        params = {}

        if min_per is not None:
            conditions.append("per >= :min_per")
            params["min_per"] = min_per

        if max_per is not None:
            conditions.append("per <= :max_per")
            params["max_per"] = max_per

        if min_pbr is not None:
            conditions.append("pbr >= :min_pbr")
            params["min_pbr"] = min_pbr

        if max_pbr is not None:
            conditions.append("pbr <= :max_pbr")
            params["max_pbr"] = max_pbr

        if min_roe is not None:
            conditions.append("roe_val >= :min_roe")
            params["min_roe"] = min_roe

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT 
                c.ticker,
                s.hts_kor_isnm AS stock_name,
                c.current_price,
                c.per,
                c.pbr,
                c.roe_val,
                c.eps,
                c.bps,
                c.price_date
            FROM stock_valuation_cache c
            JOIN stocks s ON c.ticker = s.ticker
            WHERE {where_clause}
              AND c.per IS NOT NULL
              AND c.pbr IS NOT NULL
            ORDER BY c.per ASC
            LIMIT :limit
        """

        params["limit"] = limit

        try:
            results = db.execute(text(query), params).fetchall()

            return [
                {
                    "ticker": r.ticker,
                    "stock_name": r.stock_name,
                    "current_price": float(r.current_price),
                    "per": float(r.per),
                    "pbr": float(r.pbr),
                    "roe_val": float(r.roe_val) if r.roe_val else None,
                    "eps": float(r.eps) if r.eps else None,
                    "bps": float(r.bps) if r.bps else None,
                    "price_date": r.price_date.isoformat() if r.price_date else None
                }
                for r in results
            ]

        except Exception as e:
            logger.error(f"Failed to screen stocks: {e}")
            return []


def get_valuation_service() -> ValuationService:
    """ValuationService 싱글톤"""
    return ValuationService()