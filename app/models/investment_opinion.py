"""
Investment Opinion (투자의견 컨센서스) 모델
"""
from sqlalchemy import Column, String, DateTime, PrimaryKeyConstraint
from sqlalchemy.sql import func
from app.core.database import Base


class InvestmentOpinion(Base):
    """투자의견 컨센서스 (증권사별 종목 투자의견)"""
    __tablename__ = "investment_opinions"

    # Composite Primary Key
    ticker = Column(String(12), nullable=False, comment="종목코드")
    mbcr_name = Column(String(100), nullable=False, comment="회원사명(증권사)")

    # 투자의견 정보
    stck_bsop_date = Column(String(8), nullable=False, comment="주식영업일자")
    invt_opnn = Column(String(50), comment="투자의견")
    invt_opnn_cls_code = Column(String(10), comment="투자의견구분코드")
    rgbf_invt_opnn = Column(String(50), comment="직전투자의견")
    rgbf_invt_opnn_cls_code = Column(String(10), comment="직전투자의견구분코드")
    hts_goal_prc = Column(String(20), comment="HTS목표가격")

    # 메타 정보
    created_at = Column(DateTime, server_default=func.now(), comment="생성일시")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), comment="수정일시")

    __table_args__ = (
        PrimaryKeyConstraint('ticker', 'mbcr_name', name='pk_investment_opinions'),
        {'comment': '투자의견 컨센서스 (증권사별 종목 투자의견)'}
    )

    def __repr__(self):
        return f"<InvestmentOpinion(ticker={self.ticker}, mbcr={self.mbcr_name}, opinion={self.invt_opnn})>"