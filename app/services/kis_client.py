"""
한국투자증권 KIS API 클라이언트
"""
import httpx
import logging
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.config.config import get_settings
from app.core.kis_auth import get_auth_manager

logger = logging.getLogger(__name__)
settings = get_settings()


class KISAPIClient:
    """KIS API 호출 클라이언트"""

    def __init__(self):
        self.base_url = settings.kis_api_url
        self.auth_manager = get_auth_manager()
        self.rate_limit_delay = 1.0 / settings.API_RATE_LIMIT_PER_SECOND
        self.last_request_time = datetime.now()

    async def _wait_for_rate_limit(self):
        """API 호출 제한 준수"""
        elapsed = (datetime.now() - self.last_request_time).total_seconds()
        if elapsed < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = datetime.now()

    async def _request(
        self,
        method: str,
        endpoint: str,
        tr_id: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """
        KIS API 요청 실행

        Args:
            method: HTTP 메서드 (GET, POST 등)
            endpoint: API 엔드포인트
            tr_id: 거래ID (KIS API에서 요구)
            params: 쿼리 파라미터
            headers: 추가 헤더
            retry_count: 현재 재시도 횟수

        Returns:
            API 응답 데이터

        Raises:
            httpx.HTTPError: API 요청 실패시
        """
        await self._wait_for_rate_limit()

        # Redis에서 토큰 조회 (자동 갱신)
        token = await self.auth_manager.get_access_token()
        
        url = f"{self.base_url}{endpoint}"
        request_headers = {
            "authorization": f"Bearer {token}",
            "appkey": self.auth_manager.app_key,
            "appsecret": self.auth_manager.app_secret,
            "tr_id": tr_id,
            "content-type": "application/json; charset=utf-8"
        }

        if headers:
            request_headers.update(headers)

        async with httpx.AsyncClient() as client:
            try:
                if method.upper() == "GET":
                    response = await client.get(
                        url,
                        headers=request_headers,
                        params=params,
                        timeout=30.0
                    )
                else:
                    response = await client.post(
                        url,
                        headers=request_headers,
                        json=params,
                        timeout=30.0
                    )

                response.raise_for_status()
                data = response.json()

                # KIS API 응답 코드 확인
                rt_cd = data.get("rt_cd", "1")
                if rt_cd != "0":
                    msg = data.get("msg1", "Unknown error")
                    logger.warning(f"KIS API returned error: {rt_cd} - {msg}")

                return data

            except httpx.HTTPError as e:
                logger.error(f"HTTP error on {url}: {e}")

                # 재시도 로직
                if retry_count < settings.API_RETRY_COUNT:
                    logger.info(f"Retrying... (attempt {retry_count + 1}/{settings.API_RETRY_COUNT})")
                    await asyncio.sleep(settings.API_RETRY_DELAY)
                    return await self._request(method, endpoint, tr_id, params, headers, retry_count + 1)

                raise

    # ============================================================
    # 종목 정보 조회
    # ============================================================

    async def get_stock_price(self, ticker: str) -> Dict[str, Any]:
        """
        주식 현재가 시세 조회

        Args:
            ticker: 종목코드 (예: 005930)

        Returns:
            현재가 시세 데이터
        """
        endpoint = "/domestic-stock/v1/quotations/inquire-price"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # 시장구분 (J: 주식)
            "FID_INPUT_ISCD": ticker
        }
        return await self._request("GET", endpoint, "FHKST01010100", params)

    async def get_daily_price(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        period: str = "D"
    ) -> Dict[str, Any]:
        """
        주식 기간별 시세 조회

        Args:
            ticker: 종목코드
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)
            period: 기간구분 (D: 일, W: 주, M: 월)

        Returns:
            기간별 시세 데이터
        """
        endpoint = "/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": period,
            "FID_ORG_ADJ_PRC": "0"  # 수정주가 (0: 수정주가, 1: 원주가)
        }
        return await self._request("GET", endpoint, "FHKST03010100", params)

    # ============================================================
    # 투자자 매매 동향
    # ============================================================

    async def get_investor_trend(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        종목별 투자자 매매 동향 조회

        Args:
            ticker: 종목코드
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)

        Returns:
            투자자별 매매 동향 데이터
        """
        endpoint = "/domestic-stock/v1/quotations/inquire-investor-trend"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_ETC_CLS_CODE": ""
        }
        return await self._request("GET", endpoint, "FHKST01010900", params)

    # ============================================================
    # 프로그램 매매
    # ============================================================

    async def get_program_trading_trend(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        종목별 프로그램 매매 추이 조회 (일별)

        Args:
            ticker: 종목코드
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)

        Returns:
            프로그램 매매 추이 데이터
        """
        endpoint = "/domestic-stock/v1/quotations/program-trade-by-stock"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_ETC_CLS_CODE": ""
        }
        return await self._request("GET", endpoint, "FHPST01060000", params)

    # ============================================================
    # 유틸리티 메서드
    # ============================================================

    async def get_all_stock_codes(self, market: str = "ALL") -> List[Dict[str, str]]:
        """
        전체 종목 코드 조회

        Args:
            market: 시장구분 (KOSPI, KOSDAQ, ALL)

        Returns:
            종목 코드 리스트 [{"ticker": "005930", "name": "삼성전자", "market": "KOSPI"}, ...]

        Note:
            KIS API에는 전체 종목 코드 조회 API가 없으므로,
            pykrx 등 다른 소스를 활용하거나 별도 구현 필요
        """
        # TODO: 전체 종목 코드 조회 구현
        # Option 1: pykrx 라이브러리 활용
        # Option 2: KRX 파일 다운로드
        # Option 3: 수동으로 관리하는 종목 리스트
        logger.warning("get_all_stock_codes not implemented yet")
        return []

    def format_date(self, date: datetime) -> str:
        """날짜를 KIS API 형식(YYYYMMDD)으로 변환"""
        return date.strftime("%Y%m%d")


# 싱글톤 인스턴스
_kis_client: Optional[KISAPIClient] = None


def get_kis_client() -> KISAPIClient:
    """KIS API 클라이언트 싱글톤 반환"""
    global _kis_client
    if _kis_client is None:
        _kis_client = KISAPIClient()
    return _kis_client
