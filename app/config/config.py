"""
Riverlands 프로젝트 환경 설정
한국투자증권 KIS API 및 데이터베이스 연결 설정
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """애플리케이션 설정"""

    # 프로젝트 기본 정보
    PROJECT_NAME: str = "Riverlands"
    VERSION: str = "1.0.0"
    DESCRIPTION: str = "KIS API 기반 주식 데이터 수집 시스템"

    # KIS API 설정
    KIS_APP_KEY: str
    KIS_APP_SECRET: str
    KIS_BASE_URL: str = "https://openapi.koreainvestment.com:9443"
    KIS_VIRTUAL_MODE: bool = False  # False: 실전투자, True: 모의투자

    # 데이터베이스 설정
    DATABASE_URL: str  # 전체 DB URL (환경변수에서 직접 사용)

    # Redis 설정 (토큰 캐시)
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""  # Optional
    REDIS_TOKEN_KEY: str = "kis_access_token"  # Redis key for token

    # API 호출 제한 설정
    API_RATE_LIMIT_PER_SECOND: int = 20  # 초당 요청 제한
    API_RETRY_COUNT: int = 3  # 재시도 횟수
    API_RETRY_DELAY: int = 1  # 재시도 대기 시간(초)

    # 데이터 수집 설정
    COLLECTION_BATCH_SIZE: int = 100  # 배치 처리 크기
    COLLECTION_START_YEAR: int = 2020  # 과거 데이터 수집 시작 년도

    # 로깅 설정
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/riverlands.log"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "ignore"  # 정의되지 않은 환경변수 무시

    @property
    def database_url(self) -> str:
        """
        데이터베이스 연결 URL
        환경변수 DATABASE_URL을 그대로 사용
        """
        return self.DATABASE_URL

    @property
    def redis_url(self) -> str:
        """Redis 연결 URL"""
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    @property
    def kis_token_url(self) -> str:
        """KIS OAuth 토큰 발급 URL"""
        return f"{self.KIS_BASE_URL}/oauth2/tokenP"

    @property
    def kis_api_url(self) -> str:
        """KIS API 기본 URL"""
        return f"{self.KIS_BASE_URL}/uapi"


@lru_cache()
def get_settings() -> Settings:
    """설정 객체 반환 (캐싱)"""
    return Settings()