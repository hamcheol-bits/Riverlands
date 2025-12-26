"""
개선된 리서치 리포트 모델 (다대다 관계 지원)
"""
from sqlalchemy import Column, String, Integer, Date, Text, Boolean, TIMESTAMP, ForeignKey, Index, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.database import Base


class ResearchReport(Base):
    """
    리서치 리포트 기본 정보

    종목 정보는 별도 테이블(ReportStockRelation)에서 다대다로 관리
    """

    __tablename__ = "research_reports"

    # Primary Key
    id = Column(String(100), primary_key=True, comment="리포트 고유 ID")

    # 기본 정보
    broker = Column(String(50), nullable=False, comment="증권사명")
    source = Column(String(20), nullable=False, default="naver", comment="수집 출처")

    # 리포트 정보
    title = Column(String(500), nullable=False, comment="리포트 제목")
    report_type = Column(String(20), nullable=False, comment="리포트 유형")
    category = Column(String(20), nullable=True, comment="카테고리")
    author = Column(String(100), nullable=True, comment="애널리스트명")
    published_date = Column(Date, nullable=False, comment="발행일")

    # 파일 정보
    pdf_url = Column(String(1000), nullable=True, comment="원본 PDF URL")
    pdf_local_path = Column(String(500), nullable=True, comment="로컬 저장 경로")
    pdf_s3_key = Column(String(500), nullable=True, comment="S3 저장 키")
    file_size_bytes = Column(Integer, nullable=True, comment="파일 크기")

    # 내용
    summary = Column(Text, nullable=True, comment="요약")
    full_text = Column(Text, nullable=True, comment="전체 텍스트")

    # 처리 상태
    download_status = Column(String(20), nullable=False, default="pending")
    text_extracted = Column(Boolean, default=False)
    vectorized = Column(Boolean, default=False)
    chroma_collection_id = Column(String(100), nullable=True)

    # 타임스탬프
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    # Relationships
    stock_relations = relationship(
        "ReportStockRelation",
        back_populates="report",
        cascade="all, delete-orphan"
    )

    industry_relations = relationship(
        "ReportIndustry",
        back_populates="report",
        cascade="all, delete-orphan"
    )

    # 인덱스
    __table_args__ = (
        Index('idx_broker_date', 'broker', 'published_date'),
        Index('idx_report_type', 'report_type'),
        Index('idx_category', 'category'),
        Index('idx_download_status', 'download_status'),
        Index('idx_published_date', 'published_date'),
    )

    def __repr__(self):
        return f"<ResearchReport(id={self.id}, title={self.title[:30]}...)>"

    def to_dict(self, include_relations=False):
        """딕셔너리로 변환"""
        result = {
            "id": self.id,
            "broker": self.broker,
            "source": self.source,
            "title": self.title,
            "report_type": self.report_type,
            "category": self.category,
            "author": self.author,
            "published_date": self.published_date.isoformat() if self.published_date else None,
            "pdf_url": self.pdf_url,
            "pdf_local_path": self.pdf_local_path,
            "summary": self.summary,
            "download_status": self.download_status,
            "text_extracted": self.text_extracted,
            "vectorized": self.vectorized,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }

        # 관련 종목 정보 포함
        if include_relations and self.stock_relations:
            result["related_stocks"] = [
                {
                    "ticker": rel.ticker,
                    "investment_opinion": rel.investment_opinion,
                    "target_price": rel.target_price,
                    "is_main_ticker": rel.is_main_ticker
                }
                for rel in self.stock_relations
            ]
            result["ticker_count"] = len(self.stock_relations)

        return result


class ReportStockRelation(Base):
    """
    리포트-종목 연결 테이블 (다대다)

    하나의 리포트가 여러 종목을 언급할 수 있고,
    하나의 종목이 여러 리포트에 언급될 수 있음
    """

    __tablename__ = "report_stock_relations"

    # Primary Key
    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Foreign Keys
    report_id = Column(
        String(100),
        ForeignKey("research_reports.id", ondelete="CASCADE"),
        nullable=False,
        comment="리포트 ID"
    )
    ticker = Column(
        String(20),
        ForeignKey("stocks.ticker", ondelete="CASCADE"),
        nullable=False,
        comment="종목코드"
    )

    # 종목별 세부 정보
    investment_opinion = Column(String(20), nullable=True, comment="투자의견")
    target_price = Column(Integer, nullable=True, comment="목표주가")
    mention_count = Column(Integer, default=1, comment="언급 횟수")
    is_main_ticker = Column(Boolean, default=False, comment="주요 종목 여부")

    # 타임스탬프
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    # Relationships
    report = relationship("ResearchReport", back_populates="stock_relations")

    # 인덱스
    __table_args__ = (
        Index('idx_report_ticker', 'report_id', 'ticker', unique=True),
        Index('idx_ticker', 'ticker'),
        Index('idx_report', 'report_id'),
        Index('idx_opinion', 'investment_opinion'),
        Index('idx_is_main', 'is_main_ticker'),
    )

    def __repr__(self):
        return f"<ReportStockRelation(report={self.report_id}, ticker={self.ticker})>"

    def to_dict(self):
        return {
            "report_id": self.report_id,
            "ticker": self.ticker,
            "investment_opinion": self.investment_opinion,
            "target_price": self.target_price,
            "mention_count": self.mention_count,
            "is_main_ticker": self.is_main_ticker
        }


class ReportIndustry(Base):
    """
    리포트 산업 분류

    산업분석 리포트의 경우 특정 산업에 대한 분석
    """

    __tablename__ = "report_industries"

    # Primary Key
    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Foreign Key
    report_id = Column(
        String(100),
        ForeignKey("research_reports.id", ondelete="CASCADE"),
        nullable=False,
        comment="리포트 ID"
    )

    # 산업 정보
    industry_name = Column(String(100), nullable=False, comment="산업명")
    industry_code = Column(String(20), nullable=True, comment="산업코드")

    # 타임스탬프
    created_at = Column(TIMESTAMP, server_default=func.now())

    # Relationships
    report = relationship("ResearchReport", back_populates="industry_relations")

    # 인덱스
    __table_args__ = (
        Index('idx_report_industry', 'report_id', 'industry_name', unique=True),
        Index('idx_industry_name', 'industry_name'),
    )

    def __repr__(self):
        return f"<ReportIndustry(report={self.report_id}, industry={self.industry_name})>"