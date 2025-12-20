"""
한국투자증권 KIS API OAuth 인증 모듈 (Redis 기반)
"""
import httpx
import logging
from typing import Optional
from app.config.config import get_settings
from app.core.redis_client import get_redis_client

logger = logging.getLogger(__name__)
settings = get_settings()


class KISAuthManager:
    """KIS API OAuth 인증 관리자 (Redis 캐시)"""

    def __init__(self):
        self.app_key = settings.KIS_APP_KEY
        self.app_secret = settings.KIS_APP_SECRET
        self.token_url = settings.kis_token_url
        self.redis_token_key = settings.REDIS_TOKEN_KEY
        
    async def get_access_token(self, force_refresh: bool = False) -> str:
        """
        액세스 토큰 반환 (Redis 캐시 우선)
        
        토큰 관리 전략:
        1. Redis에서 토큰 조회
        2. 토큰 있으면 → 반환 (Redis TTL로 자동 만료 관리)
        3. 토큰 없으면 → KIS API 호출하여 새 토큰 발급
        4. 새 토큰을 expires_in 값으로 Redis에 저장

        Args:
            force_refresh: 강제 갱신 여부

        Returns:
            액세스 토큰 문자열
        """
        # 강제 갱신이 아니면 Redis에서 먼저 조회
        if not force_refresh:
            cached_token = self._get_token_from_redis()
            if cached_token:
                logger.debug("Using cached access token from Redis")
                return cached_token
        
        # Redis에 토큰 없음 → 새로 발급
        logger.info("No cached token found. Requesting new token from KIS API")
        token, expires_in = await self._request_new_token()
        
        # Redis에 저장 (TTL: expires_in)
        self._save_token_to_redis(token, expires_in)
        
        return token

    def _get_token_from_redis(self) -> Optional[str]:
        """
        Redis에서 토큰 조회
        
        Returns:
            토큰 문자열 또는 None
        """
        try:
            redis_client = get_redis_client()
            if not redis_client:
                logger.warning("Redis not available. Skipping cache.")
                return None
            
            token = redis_client.get(self.redis_token_key)
            
            if token:
                # Redis TTL 확인 (디버깅용)
                ttl = redis_client.ttl(self.redis_token_key)
                logger.info(f"Token found in Redis. TTL: {ttl}s ({ttl/3600:.2f}h)")
                return token
            
            logger.info("No token in Redis")
            return None
            
        except Exception as e:
            logger.error(f"Error reading token from Redis: {e}")
            return None

    def _save_token_to_redis(self, token: str, expires_in: int) -> bool:
        """
        Redis에 토큰 저장 (TTL 설정)
        
        Args:
            token: 액세스 토큰
            expires_in: 유효 시간 (초)
        
        Returns:
            저장 성공 여부
        """
        try:
            redis_client = get_redis_client()
            if not redis_client:
                logger.warning("Redis not available. Token not cached.")
                return False
            
            # Redis에 토큰 저장 (TTL: expires_in)
            # KIS API가 자동으로 만료 관리 → Redis TTL과 동기화
            redis_client.setex(
                name=self.redis_token_key,
                time=expires_in,  # TTL (초)
                value=token
            )
            
            logger.info(f"Token saved to Redis with TTL: {expires_in}s ({expires_in/3600:.2f}h)")
            return True
            
        except Exception as e:
            logger.error(f"Error saving token to Redis: {e}")
            return False

    async def _request_new_token(self) -> tuple[str, int]:
        """
        KIS API에서 새로운 액세스 토큰 요청
        
        Returns:
            (access_token, expires_in) 튜플
        
        Raises:
            httpx.HTTPError: API 요청 실패시
        """
        headers = {"content-type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }

        async with httpx.AsyncClient() as client:
            try:
                logger.info(f"Requesting new token from: {self.token_url}")
                response = await client.post(
                    self.token_url,
                    headers=headers,
                    json=body,
                    timeout=30.0
                )
                response.raise_for_status()

                data = response.json()
                
                # 응답 검증
                if "access_token" not in data:
                    raise ValueError(f"Invalid token response: {data}")

                access_token = data["access_token"]
                expires_in = int(data.get("expires_in", 86400))  # 기본 24시간
                
                # 추가 정보 로깅
                if "access_token_token_expired" in data:
                    logger.info(f"Token expires at: {data['access_token_token_expired']}")
                
                logger.info(f"New token obtained. Expires in: {expires_in}s ({expires_in/3600:.2f}h)")
                
                return access_token, expires_in

            except httpx.HTTPError as e:
                logger.error(f"Failed to obtain access token: {e}")
                raise
            except (ValueError, KeyError) as e:
                logger.error(f"Invalid token response format: {e}")
                raise

    def get_auth_headers(self) -> dict:
        """
        API 요청용 인증 헤더 반환 (비동기 아님 - 주의)
        
        Note: 
            이 메서드는 동기 함수이므로 토큰을 미리 발급받아야 함
            사용 전에 await get_access_token()을 먼저 호출할 것
        
        Returns:
            인증 헤더 딕셔너리
        """
        # Redis에서 토큰 조회 (동기)
        token = self._get_token_from_redis()
        
        if not token:
            raise ValueError(
                "No access token available. "
                "Call 'await get_access_token()' first."
            )

        return {
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "content-type": "application/json; charset=utf-8"
        }
    
    def invalidate_token(self) -> bool:
        """
        토큰 강제 무효화 (Redis에서 삭제)
        
        Returns:
            삭제 성공 여부
        """
        try:
            redis_client = get_redis_client()
            if not redis_client:
                return False
            
            deleted = redis_client.delete(self.redis_token_key)
            if deleted:
                logger.info("Token invalidated (deleted from Redis)")
                return True
            
            logger.warning("No token to invalidate")
            return False
            
        except Exception as e:
            logger.error(f"Error invalidating token: {e}")
            return False


# 싱글톤 인스턴스
_auth_manager: Optional[KISAuthManager] = None


def get_auth_manager() -> KISAuthManager:
    """KIS 인증 매니저 싱글톤 반환"""
    global _auth_manager
    if _auth_manager is None:
        _auth_manager = KISAuthManager()
    return _auth_manager
