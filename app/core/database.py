"""
데이터베이스 연결 및 세션 관리
"""
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator
import logging

from app.config.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# SQLAlchemy 엔진 생성
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,  # 연결 유효성 체크
    pool_size=10,  # 기본 연결 풀 크기
    max_overflow=20,  # 최대 추가 연결 수
    echo=False  # SQL 로그 출력 (개발 시 True)
)

# 세션 팩토리
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Base 클래스 (모델 상속용)
Base = declarative_base()


def get_db() -> Generator[Session, None, None]:
    """
    데이터베이스 세션 의존성

    Usage:
        @app.get("/items")
        async def read_items(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_db_connection() -> bool:
    """데이터베이스 연결 확인"""
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))  # text() 함수로 래핑
        db.close()
        logger.info("Database connection successful")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False