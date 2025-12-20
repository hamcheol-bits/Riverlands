"""
Redis 연결 및 관리
토큰 캐싱용
"""
import redis
from redis import Redis
from typing import Optional
import logging

from app.config.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Redis 클라이언트 인스턴스
_redis_client: Optional[Redis] = None


def get_redis_client() -> Redis:
    """
    Redis 클라이언트 싱글톤 반환
    
    Returns:
        Redis 클라이언트
    """
    global _redis_client
    
    if _redis_client is None:
        try:
            _redis_client = redis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,  # 문자열로 자동 디코딩
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            # 연결 테스트
            _redis_client.ping()
            logger.info(f"Redis connected: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
            
        except redis.RedisError as e:
            logger.error(f"Redis connection failed: {e}")
            logger.warning("Proceeding without Redis cache")
            _redis_client = None
    
    return _redis_client


def check_redis_connection() -> bool:
    """
    Redis 연결 확인
    
    Returns:
        연결 성공 여부
    """
    try:
        client = get_redis_client()
        if client:
            client.ping()
            return True
        return False
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        return False


def close_redis_connection():
    """Redis 연결 종료"""
    global _redis_client
    
    if _redis_client:
        try:
            _redis_client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")
        finally:
            _redis_client = None
