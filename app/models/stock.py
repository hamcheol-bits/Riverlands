"""
주식 기본 정보 모델 (KIS API 응답 필드명 사용)
"""
from sqlalchemy import Column, String, Boolean, Date, TIMESTAMP
from sqlalchemy.sql import func
from app.core.database import Base


class Stock(Base):
    """
    주식 기본 정보 테이블
    
    KIS API: 주식현재가 시세 (FHKST01010100)
    - 응답 필드명을 그대로 사용
    """

    __tablename__ = "stocks"

    # Primary Key
    ticker = Column(String(20), primary_key=True, comment="종목코드 (FID_INPUT_ISCD)")
    
    # KIS API output 필드
    hts_kor_isnm = Column(String(200), nullable=False, comment="HTS한글종목명")
    name_en = Column(String(200), nullable=True, comment="영문명")
    mrkt_ctg_cls_code = Column(String(20), nullable=False, comment="시장범주구분코드 (KOSPI/KOSDAQ)")
    bstp_kor_isnm = Column(String(100), nullable=True, comment="업종한글종목명")
    sector = Column(String(100), nullable=True, comment="섹터 (추가 필드)")
    listed_date = Column(Date, nullable=True, comment="상장일 (추가 필드)")
    
    # 상태 관리
    is_active = Column(Boolean, nullable=False, default=True, comment="활성 여부")
    
    # 타임스탬프
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<Stock(ticker={self.ticker}, hts_kor_isnm={self.hts_kor_isnm}, mrkt_ctg_cls_code={self.mrkt_ctg_cls_code})>"

    def to_dict(self):
        """딕셔너리로 변환 (KIS API 응답 형식)"""
        return {
            "ticker": self.ticker,
            "hts_kor_isnm": self.hts_kor_isnm,
            "name_en": self.name_en,
            "mrkt_ctg_cls_code": self.mrkt_ctg_cls_code,
            "bstp_kor_isnm": self.bstp_kor_isnm,
            "sector": self.sector,
            "listed_date": self.listed_date.isoformat() if self.listed_date else None,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
