"""
밸류에이션 계산 서비스
app/services/valuation_service.py

PER, PBR 등 계산 지표 관리
"""
import logging
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.models.stock import Stock

logger = logging.getLogger(__name__)


class ValuationService:
    """
    밸류에이션 지표 계산 및 캐시 관리

    지표:
    - PER: 주가 / EPS
    - PBR: 주가 / BPS
    - ROE: 재무제표에서 가져옴
    - 배당수익률: 배당금 / 현재가
    """

    def update_valuation_for_ticker(
            self,
            db: Session,
            ticker: str
    ) -> Dict[str, Any]:
        """
        단일 종목 밸류에이션 갱신

        Args:
            db: 데이터베이스 세션
            ticker: 종목코드

        Returns:
            갱신 결과
        """
        try:
            # MySQL 프로시저 호출
            db.execute(
                text("CALL update_valuation_for_ticker(:ticker)"),
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

    def update_all_valuations(
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
                    result = self.update_valuation_for_ticker(db, stock.ticker)
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
                db.execute(text("CALL update_all_valuations()"))
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