"""
Riverlands FastAPI 메인 애플리케이션
"""
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import logging
from contextlib import asynccontextmanager

from app.config.config import get_settings
from app.core.database import get_db, check_db_connection, init_db
from app.core.kis_auth import get_auth_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 실행"""
    # 시작 시
    logger.info(f"Starting {settings.PROJECT_NAME} v{settings.VERSION}")

    # 데이터베이스 연결 확인
    if check_db_connection():
        logger.info("Database connection successful")
    else:
        logger.error("Database connection failed")

    # OAuth 토큰 초기화 (옵션)
    try:
        auth_manager = get_auth_manager()
        logger.info("Auth manager initialized")
    except Exception as e:
        logger.error(f"Auth manager initialization failed: {e}")

    yield

    # 종료 시
    logger.info("Shutting down application")


# FastAPI 앱 생성
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
        # DB 연결 확인
        db.execute("SELECT 1")
        db_status = "healthy"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_status = "unhealthy"

    return {
        "status": "ok",
        "database": db_status,
        "version": settings.VERSION
    }


@app.get("/api/token/verify")
async def verify_token(db: Session = Depends(get_db)):
    """KIS API 토큰 확인 엔드포인트"""
    try:
        auth_manager = get_auth_manager()
        token = await auth_manager.get_access_token(db)

        return {
            "status": "ok",
            "token_exists": bool(token),
            "token_valid": auth_manager._is_token_valid(),
            "expires_at": auth_manager._token_expires_at.isoformat() if auth_manager._token_expires_at else None
        }
    except Exception as e:
        logger.error(f"Token verification failed: {e}")
        return {
            "status": "error",
            "message": str(e)
        }


# 라우터 등록 (추후 추가)
# from app.routers import stocks, collection
# app.include_router(stocks.router, prefix="/api/stocks", tags=["stocks"])
# app.include_router(collection.router, prefix="/api/collection", tags=["collection"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )