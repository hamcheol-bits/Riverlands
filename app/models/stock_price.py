"""
주가 데이터 모델 (KIS API 응답 필드명 사용)
"""
from sqlalchemy import Column, String, Date, DECIMAL, BIGINT, CHAR, TIMESTAMP, ForeignKey
from sqlalchemy.sql import func
from app.core.database import Base


class StockPrice(Base):
    """
    일별 주가 데이터 테이블
    
    KIS API: 주식현재가 일자별 (FHKST01010400)
    또는: 국내주식기간별시세 (FHKST03010100)
    
    응답 output 필드명을 그대로 사용
    """

    __tablename__ = "stock_prices"

    # Primary Key
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    
    # Foreign Key
    ticker = Column(String(20), ForeignKey("stocks.ticker", ondelete="CASCADE"), nullable=False, comment="종목코드")
    
    # KIS API output 필드명 (주식현재가 일자별 응답)
    stck_bsop_date = Column(Date, nullable=False, comment="주식영업일자 (거래일)")
    stck_oprc = Column(DECIMAL(20, 2), nullable=True, comment="주식시가")
    stck_hgpr = Column(DECIMAL(20, 2), nullable=True, comment="주식최고가")
    stck_lwpr = Column(DECIMAL(20, 2), nullable=True, comment="주식최저가")
    stck_clpr = Column(DECIMAL(20, 2), nullable=False, comment="주식종가")
    acml_vol = Column(BIGINT, nullable=True, comment="누적거래량")
    acml_tr_pbmn = Column(BIGINT, nullable=True, comment="누적거래대금")
    
    # 전일대비 정보
    prdy_vrss = Column(DECIMAL(15, 2), nullable=True, comment="전일대비")
    prdy_vrss_sign = Column(CHAR(1), nullable=True, comment="전일대비부호 (1:상한 2:상승 3:보합 4:하한 5:하락)")
    prdy_ctrt = Column(DECIMAL(10, 4), nullable=True, comment="전일대비율")
    
    # 투자자 정보
    hts_frgn_ehrt = Column(DECIMAL(10, 4), nullable=True, comment="HTS외국인소진율")
    frgn_ntby_qty = Column(BIGINT, nullable=True, comment="외국인순매수수량")
    
    # 타임스탬프
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<StockPrice(ticker={self.ticker}, stck_bsop_date={self.stck_bsop_date}, stck_clpr={self.stck_clpr})>"

    def to_dict(self):
        """딕셔너리로 변환 (KIS API 응답 형식)"""
        return {
            "id": self.id,
            "ticker": self.ticker,
            "stck_bsop_date": self.stck_bsop_date.isoformat() if self.stck_bsop_date else None,
            "stck_oprc": float(self.stck_oprc) if self.stck_oprc else None,
            "stck_hgpr": float(self.stck_hgpr) if self.stck_hgpr else None,
            "stck_lwpr": float(self.stck_lwpr) if self.stck_lwpr else None,
            "stck_clpr": float(self.stck_clpr) if self.stck_clpr else None,
            "acml_vol": self.acml_vol,
            "acml_tr_pbmn": self.acml_tr_pbmn,
            "prdy_vrss": float(self.prdy_vrss) if self.prdy_vrss else None,
            "prdy_vrss_sign": self.prdy_vrss_sign,
            "prdy_ctrt": float(self.prdy_ctrt) if self.prdy_ctrt else None,
            "hts_frgn_ehrt": float(self.hts_frgn_ehrt) if self.hts_frgn_ehrt else None,
            "frgn_ntby_qty": self.frgn_ntby_qty,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
