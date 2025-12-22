"""
배당 정보 모델 (KIS API 응답 필드명 사용)
"""
from sqlalchemy import Column, String, BIGINT, DECIMAL, Date, TIMESTAMP, ForeignKey, Index
from sqlalchemy.sql import func
from app.core.database import Base


class Dividend(Base):
    """
    배당 정보 테이블

    KIS API: 예탁원정보(배당일정) [국내주식-145]
    Endpoint: /uapi/domestic-stock/v1/ksdinfo/dividend
    TR_ID: HHKDB669102C0

    결산배당, 중간배당 모두 저장

    Request Parameters:
    - CTS: 연속조회 키 (공백)
    - GB1: 조회구분 (0:전체, 1:결산배당, 2:중간배당)
    - F_DT: 조회시작일 (YYYYMMDD)
    - T_DT: 조회종료일 (YYYYMMDD)
    - SHT_CD: 종목코드 (9자리)
    - HIGH_GB: 고배당여부 (공백)
    """

    __tablename__ = "dividends"

    # Primary Key
    id = Column(BIGINT, primary_key=True, autoincrement=True)

    # Foreign Key
    ticker = Column(
        String(20),
        ForeignKey("stocks.ticker", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="종목코드"
    )

    # ============================================================
    # 배당 기본 정보
    # ============================================================
    record_date = Column(Date, nullable=False, index=True, comment="기준일")
    divi_kind = Column(String(20), nullable=True, comment="배당종류 (결산배당/중간배당)")

    # ============================================================
    # 배당 금액 정보
    # ============================================================
    face_val = Column(BIGINT, nullable=True, comment="액면가")
    per_sto_divi_amt = Column(DECIMAL(15, 2), nullable=True, comment="현금배당금 (주당)")
    divi_rate = Column(DECIMAL(10, 4), nullable=True, comment="현금배당률 (%)")
    stk_divi_rate = Column(DECIMAL(10, 4), nullable=True, comment="주식배당률 (%)")

    # ============================================================
    # 배당 지급일
    # ============================================================
    divi_pay_dt = Column(Date, nullable=True, comment="배당금지급일")
    stk_div_pay_dt = Column(Date, nullable=True, comment="주식배당지급일")

    # ============================================================
    # 기타 정보
    # ============================================================
    stk_kind = Column(String(20), nullable=True, comment="주식종류 (보통주/우선주)")
    high_divi_gb = Column(String(1), nullable=True, comment="고배당종목여부 (Y/N)")

    # ============================================================
    # 타임스탬프
    # ============================================================
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    # ============================================================
    # 복합 유니크 인덱스
    # 종목코드 + 기준일 + 배당종류 = 유니크
    # ============================================================
    __table_args__ = (
        Index('idx_ticker_record_kind', 'ticker', 'record_date', 'divi_kind', unique=True),
    )

    def __repr__(self):
        return (
            f"<Dividend("
            f"ticker={self.ticker}, "
            f"record_date={self.record_date}, "
            f"divi_kind={self.divi_kind}, "
            f"per_sto_divi_amt={self.per_sto_divi_amt})>"
        )

    def to_dict(self):
        """딕셔너리로 변환"""
        return {
            "id": self.id,
            "ticker": self.ticker,
            "record_date": self.record_date.isoformat() if self.record_date else None,
            "divi_kind": self.divi_kind,
            "face_val": int(self.face_val) if self.face_val else None,
            "per_sto_divi_amt": float(self.per_sto_divi_amt) if self.per_sto_divi_amt else None,
            "divi_rate": float(self.divi_rate) if self.divi_rate else None,
            "stk_divi_rate": float(self.stk_divi_rate) if self.stk_divi_rate else None,
            "divi_pay_dt": self.divi_pay_dt.isoformat() if self.divi_pay_dt else None,
            "stk_div_pay_dt": self.stk_div_pay_dt.isoformat() if self.stk_div_pay_dt else None,
            "stk_kind": self.stk_kind,
            "high_divi_gb": self.high_divi_gb,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }