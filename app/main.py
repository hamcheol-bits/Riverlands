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

    # KIS 인증 매니저 초기화 및 토큰 발급
    try:
        auth_manager = get_auth_manager()
        logger.info("KIS Auth manager initialized")

        # ✅ 서버 시작 시 토큰 미리 발급하여 Redis에 저장
        token = await auth_manager.get_access_token()
        logger.info(f"KIS access token obtained and cached in Redis (valid for 24h)")

    except Exception as e:
        logger.error(f"Failed to initialize auth or obtain token: {e}")
        # 토큰 발급 실패해도 서버는 시작 (첫 API 요청 시 재시도)

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

    # Redis 토큰 확인 (없으면 빈 문자열)
    redis_token = ""
    redis_ttl = None
    redis_status = "unavailable"

    try:
        from app.core.redis_client import get_redis_client
        redis_client = get_redis_client()

        if redis_client:
            redis_status = "connected"
            # 토큰 조회 (새로 발급하지 않고 캐시만 확인)
            token = redis_client.get(settings.REDIS_TOKEN_KEY)
            if token:
                redis_token = token
                redis_ttl = redis_client.ttl(settings.REDIS_TOKEN_KEY)
    except Exception as e:
        logger.warning(f"Redis health check failed: {e}")
        redis_status = "error"

    return {
        "status": "ok",
        "database": db_status,
        "redis": {
            "status": redis_status,
            "token": redis_token,
            "ttl_seconds": redis_ttl,
            "ttl_hours": round(redis_ttl / 3600, 2) if redis_ttl and redis_ttl > 0 else None
        },
        "version": settings.VERSION
    }


# ============================================================
# 라우터 등록 (모듈화된 구조)
# ============================================================
from app.routers import stocks, stock_prices, financials, batch, dividends, investment_opinion, naver_research, \
    valuation

app.include_router(stocks.router)
app.include_router(stock_prices.router)
app.include_router(financials.router)
app.include_router(batch.router)
app.include_router(dividends.router)
app.include_router(investment_opinion.router)
app.include_router(naver_research.router)
app.include_router(valuation.router)

logger.info("All routers registered successfully")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8100,
        reload=True
    )