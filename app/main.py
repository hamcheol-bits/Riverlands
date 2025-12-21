"""
Riverlands FastAPI 메인 애플리케이션
"""
from fastapi import FastAPI, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
import logging
from contextlib import asynccontextmanager

from app.config.config import get_settings
from app.core.database import get_db, check_db_connection
from app.core.kis_auth import get_auth_manager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 실행"""
    logger.info(f"Starting {settings.PROJECT_NAME} v{settings.VERSION}")

    # 데이터베이스 연결 확인
    if check_db_connection():
        logger.info("Database connection successful")
    else:
        logger.error("Database connection failed")

    # KIS 인증 매니저 초기화
    try:
        auth_manager = get_auth_manager()
        logger.info("KIS Auth manager initialized")
    except Exception as e:
        logger.error(f"Auth manager initialization failed: {e}")

    yield

    logger.info("Shutting down application")


app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version=settings.VERSION,
    lifespan=lifespan
)


@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {
        "project": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "description": settings.DESCRIPTION,
        "status": "running"
    }


@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """헬스 체크 엔드포인트"""
    try:
        db.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_status = "unhealthy"

    return {
        "status": "ok",
        "database": db_status,
        "version": settings.VERSION
    }


# ============================================================
# 라우터 등록 (모듈화된 구조)
# ============================================================
from app.routers import stocks, stock_prices, financials, batch

app.include_router(stocks.router)
app.include_router(stock_prices.router)
app.include_router(financials.router)
app.include_router(batch.router)

logger.info("All routers registered successfully")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )