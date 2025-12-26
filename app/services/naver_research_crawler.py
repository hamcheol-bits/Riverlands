"""
네이버증권 리서치 크롤러 (다중 종목 추출 지원)
app/services/naver_research_crawler.py
"""
import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, Page
import asyncio

logger = logging.getLogger(__name__)


class NaverResearchCrawler:
    """
    네이버증권 리서치 크롤러

    개선 사항:
    - 다중 종목 추출
    - 주요 종목 식별
    - 종목별 투자의견/목표주가 추출
    - 산업 분류
    """

    # 리서치 카테고리별 URL
    RESEARCH_URLS = {
        "market": "https://finance.naver.com/research/market_info_list.naver",
        "invest": "https://finance.naver.com/research/invest_list.naver",
        "company": "https://finance.naver.com/research/company_list.naver",
        "industry": "https://finance.naver.com/research/industry_list.naver",
        "economy": "https://finance.naver.com/research/economy_list.naver",
        "debenture": "https://finance.naver.com/research/debenture_list.naver",
    }

    REPORT_TYPE_MAP = {
        "market": "시장전망",
        "invest": "투자전략",
        "company": "기업분석",
        "industry": "산업분석",
        "economy": "경제분석",
        "debenture": "채권분석"
    }

    def __init__(self):
        self.base_url = "https://finance.naver.com"

    async def crawl_category(
        self,
        category: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_pages: int = 10,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """카테고리별 크롤링"""
        if category not in self.RESEARCH_URLS:
            raise ValueError(f"Invalid category: {category}")

        url = self.RESEARCH_URLS[category]
        report_type = self.REPORT_TYPE_MAP[category]

        logger.info(f"Crawling {category} from Naver Research (max_pages={max_pages})")

        reports = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                for page_num in range(1, max_pages + 1):
                    logger.info(f"Processing page {page_num}/{max_pages}")

                    page_url = f"{url}?page={page_num}"
                    await page.goto(page_url, wait_until="networkidle")

                    page_reports = await self._parse_report_list(
                        page, category, report_type, start_date, end_date
                    )

                    if not page_reports:
                        logger.info(f"No more reports on page {page_num}")
                        break

                    reports.extend(page_reports)

                    if limit and len(reports) >= limit:
                        reports = reports[:limit]
                        break

                    if start_date and page_reports:
                        last_date = page_reports[-1].get("published_date")
                        if last_date:
                            last_datetime = datetime.strptime(last_date, "%Y-%m-%d")
                            if last_datetime < start_date:
                                logger.info(f"Reached start_date cutoff at page {page_num}")
                                break

                    await asyncio.sleep(1)

            finally:
                await browser.close()

        logger.info(f"Collected {len(reports)} reports from {category}")
        return reports

    async def _parse_report_list(
        self,
        page: Page,
        category: str,
        report_type: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """페이지에서 리포트 목록 파싱"""
        reports = []

        try:
            rows = await page.query_selector_all("table.type_1 tbody tr")

            for row in rows:
                try:
                    # PDF 링크 찾기
                    pdf_cell = await row.query_selector("td.file a")
                    if not pdf_cell:
                        continue

                    pdf_link = await pdf_cell.get_attribute("href")
                    if not pdf_link or not pdf_link.endswith(".pdf"):
                        continue

                    # 절대 URL
                    if pdf_link.startswith("//"):
                        pdf_url = f"https:{pdf_link}"
                    elif pdf_link.startswith("/"):
                        pdf_url = f"{self.base_url}{pdf_link}"
                    else:
                        pdf_url = pdf_link

                    # 제목 링크 찾기
                    title_link = await row.query_selector("a[href*='company_read'], a[href*='market_read'], a[href*='invest_read'], a[href*='industry_read'], a[href*='economy_read'], a[href*='debenture_read']")
                    if not title_link:
                        continue

                    title = await title_link.inner_text()
                    title = title.strip()

                    # 모든 td 셀 가져오기
                    cells = await row.query_selector_all("td")
                    if len(cells) < 4:
                        continue

                    # 증권사 찾기 (td 중에서 "증권" 포함된 텍스트)
                    broker = None
                    for cell in cells:
                        text = await cell.inner_text()
                        text = text.strip()
                        if "증권" in text or "투자" in text or "자산" in text:
                            broker = text
                            break

                    if not broker:
                        # 증권사가 없으면 건너뛰기
                        continue

                    # 날짜 찾기 (td.date 클래스)
                    date_cell = await row.query_selector("td.date")
                    if not date_cell:
                        continue

                    date_str = await date_cell.inner_text()
                    date_str = date_str.strip()

                    published_date = self._parse_date(date_str)
                    if not published_date:
                        continue

                    # 날짜 필터
                    if start_date and published_date < start_date:
                        continue
                    if end_date and published_date > end_date:
                        continue

                    # ========== 다중 종목 추출 ==========
                    ticker_details = self._extract_tickers_with_details(title, category)

                    # ========== 산업명 추출 ==========
                    industries = self._extract_industries(title, category)

                    # ========== 애널리스트명 ==========
                    author = self._extract_author(title)

                    # 리포트 ID 생성
                    report_id = self._generate_report_id(
                        broker, published_date, category
                    )

                    report = {
                        "id": report_id,
                        "broker": broker,
                        "source": "naver",
                        "title": title,
                        "report_type": report_type,
                        "category": category,
                        "author": author,
                        "published_date": published_date.strftime("%Y-%m-%d"),
                        "pdf_url": pdf_url,
                        "summary": None,

                        # 다중 종목 정보
                        "related_tickers": ticker_details,

                        # 산업 정보
                        "related_industries": industries,
                    }

                    reports.append(report)

                except Exception as e:
                    logger.error(f"Failed to parse row: {e}")
                    continue

        except Exception as e:
            logger.error(f"Failed to parse page: {e}")

        return reports

    async def crawl_all_categories(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_pages_per_category: int = 5,
        categories: Optional[List[str]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """모든 카테고리 크롤링"""
        if categories is None:
            categories = list(self.RESEARCH_URLS.keys())

        results = {}

        for category in categories:
            try:
                reports = await self.crawl_category(
                    category, start_date, end_date, max_pages_per_category
                )
                results[category] = reports
                logger.info(f"{category}: {len(reports)} reports")
            except Exception as e:
                logger.error(f"Failed to crawl {category}: {e}")
                results[category] = []

        total = sum(len(reports) for reports in results.values())
        logger.info(f"Total collected: {total} reports")

        return results

    async def download_pdf(self, pdf_url: str, save_path: str) -> bool:
        """PDF 다운로드"""
        import httpx

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(pdf_url, follow_redirects=True)
                response.raise_for_status()

                with open(save_path, "wb") as f:
                    f.write(response.content)

                logger.info(f"Downloaded: {save_path}")
                return True
        except Exception as e:
            logger.error(f"Failed to download PDF from {pdf_url}: {e}")
            return False

    # ============================================================
    # 헬퍼 메서드
    # ============================================================

    def _extract_tickers_with_details(
        self, title: str, category: str
    ) -> List[Dict[str, Any]]:
        """제목에서 모든 종목 추출 (상세 정보 포함)"""
        tickers = []
        found_tickers = set()

        # (종목명(XXXXXX)) 패턴
        pattern = r'([^\(]+)\((\d{6})\)'
        matches = re.finditer(pattern, title)

        for i, match in enumerate(matches):
            stock_name = match.group(1).strip()
            ticker = match.group(2)

            if ticker in found_tickers:
                continue
            found_tickers.add(ticker)

            is_main = (i == 0 and category == "company")

            opinion = self._extract_opinion_for_ticker(title, stock_name, ticker)
            target = self._extract_target_price_for_ticker(title, stock_name, ticker)

            tickers.append({
                "ticker": ticker,
                "stock_name": stock_name,
                "is_main_ticker": is_main,
                "investment_opinion": opinion,
                "target_price": target
            })

        # (XXXXXX) 패턴만
        if not tickers:
            pattern2 = r'\((\d{6})\)'
            matches2 = re.findall(pattern2, title)

            for i, ticker in enumerate(matches2):
                if ticker in found_tickers:
                    continue
                found_tickers.add(ticker)

                tickers.append({
                    "ticker": ticker,
                    "stock_name": None,
                    "is_main_ticker": (i == 0 and category == "company"),
                    "investment_opinion": self._extract_investment_opinion(title),
                    "target_price": self._extract_target_price(title)
                })

        # 6자리 숫자만
        if not tickers:
            pattern3 = r'\b(\d{6})\b'
            matches3 = re.findall(pattern3, title)

            for i, ticker in enumerate(matches3):
                if ticker in found_tickers:
                    continue
                found_tickers.add(ticker)

                tickers.append({
                    "ticker": ticker,
                    "stock_name": None,
                    "is_main_ticker": (i == 0 and category == "company"),
                    "investment_opinion": None,
                    "target_price": None
                })

        return tickers

    def _extract_opinion_for_ticker(
        self, title: str, stock_name: str, ticker: str
    ) -> Optional[str]:
        """특정 종목의 투자의견 추출"""
        opinions = ["BUY", "SELL", "HOLD", "매수", "매도", "중립",
                   "비중확대", "비중축소", "Outperform", "Underperform"]

        if stock_name:
            idx = title.find(stock_name)
            if idx != -1:
                context = title[idx:idx+50]
                for opinion in opinions:
                    if opinion in context.upper():
                        return opinion

        idx = title.find(ticker)
        if idx != -1:
            context = title[idx:idx+50]
            for opinion in opinions:
                if opinion in context.upper():
                    return opinion

        return None

    def _extract_target_price_for_ticker(
        self, title: str, stock_name: str, ticker: str
    ) -> Optional[int]:
        """특정 종목의 목표주가 추출"""
        if stock_name:
            idx = title.find(stock_name)
            if idx != -1:
                context = title[idx:idx+100]
                price = self._extract_price_from_text(context)
                if price:
                    return price

        idx = title.find(ticker)
        if idx != -1:
            context = title[idx:idx+100]
            price = self._extract_price_from_text(context)
            if price:
                return price

        return None

    def _extract_price_from_text(self, text: str) -> Optional[int]:
        """텍스트에서 가격 추출"""
        match = re.search(r'목표주?가\s*([0-9,]+)원?', text)
        if match:
            try:
                return int(match.group(1).replace(",", ""))
            except ValueError:
                pass

        match = re.search(r'TP\s*([0-9,]+)K?', text, re.IGNORECASE)
        if match:
            try:
                price = int(match.group(1).replace(",", ""))
                if "K" in text.upper():
                    price *= 1000
                return price
            except ValueError:
                pass

        return None

    def _extract_industries(self, title: str, category: str) -> List[str]:
        """제목에서 산업명 추출"""
        if category != "industry":
            return []

        industries = []
        industry_keywords = {
            "반도체": ["반도체", "메모리", "파운드리"],
            "2차전지": ["2차전지", "배터리", "전지"],
            "자동차": ["자동차", "전기차", "EV"],
            "조선": ["조선", "해운"],
            "철강": ["철강", "제철"],
            "화학": ["화학", "석유화학"],
            "바이오": ["바이오", "제약"],
            "금융": ["금융", "은행", "증권"],
        }

        for industry, keywords in industry_keywords.items():
            if any(kw in title for kw in keywords):
                industries.append(industry)

        return industries

    def _extract_investment_opinion(self, title: str) -> Optional[str]:
        """제목에서 투자의견 추출"""
        opinions = ["BUY", "SELL", "HOLD", "매수", "매도", "중립"]
        for opinion in opinions:
            if opinion in title.upper():
                return opinion
        return None

    def _extract_target_price(self, title: str) -> Optional[int]:
        """제목에서 목표주가 추출"""
        return self._extract_price_from_text(title)

    def _extract_author(self, title: str) -> Optional[str]:
        """제목에서 애널리스트명 추출"""
        match = re.search(r'\[([^\]]+)\]', title)
        if match:
            return match.group(1).strip()
        return None

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """날짜 파싱 (YY.MM.DD)"""
        try:
            parts = date_str.strip().split(".")
            if len(parts) == 3:
                year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])

                if year < 100:
                    year += 2000

                return datetime(year, month, day)
            return None
        except Exception as e:
            logger.warning(f"Failed to parse date: {date_str} - {e}")
            return None

    def _generate_report_id(
        self, broker: str, date: datetime, category: str
    ) -> str:
        """리포트 ID 생성"""
        import hashlib

        broker_clean = broker.replace(" ", "").replace("증권", "")
        date_str = date.strftime("%Y%m%d")

        # 충돌 방지를 위해 해시 추가
        hash_input = f"{broker}{date_str}{category}"
        hash_suffix = hashlib.md5(hash_input.encode()).hexdigest()[:8]

        return f"naver_{broker_clean}_{date_str}_{category}_{hash_suffix}"