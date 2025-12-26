"""
간단한 네이버증권 페이지 확인 (동기 버전)
"""
from playwright.sync_api import sync_playwright
import time


def check_naver_page():
    """네이버증권 리서치 페이지 구조 확인"""

    url = "https://finance.naver.com/research/company_list.naver"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        print(f"Opening: {url}\n")
        page.goto(url, wait_until="networkidle")

        time.sleep(2)

        # 1. 페이지 제목
        title = page.title()
        print(f"Page title: {title}\n")

        # 2. 테이블 확인
        print("=== Table Check ===")
        tables = page.query_selector_all("table")
        print(f"Total tables: {len(tables)}")

        for i, table in enumerate(tables[:5]):
            class_name = table.get_attribute("class") or "no-class"
            id_name = table.get_attribute("id") or "no-id"
            print(f"  Table {i}: class='{class_name}', id='{id_name}'")

        # 3. table.type_1 확인
        print("\n=== table.type_1 Check ===")
        type1_table = page.query_selector("table.type_1")

        if type1_table:
            print("✓ table.type_1 EXISTS")
            rows = type1_table.query_selector_all("tr")
            print(f"  Total rows: {len(rows)}")

            # 첫 번째 데이터 행 출력
            if len(rows) > 1:
                first_data_row = rows[1]  # 0은 헤더일 가능성
                cells = first_data_row.query_selector_all("td")
                print(f"  First data row has {len(cells)} cells:")

                for i, cell in enumerate(cells):
                    text = cell.inner_text().strip()
                    print(f"    Cell {i}: {text[:50]}")
        else:
            print("✗ table.type_1 NOT FOUND")

            # 리스트 영역 확인
            print("\n  Checking alternative selectors...")

            # ul.list 확인
            list_ul = page.query_selector("ul.list")
            if list_ul:
                print("  ✓ Found ul.list")
                items = list_ul.query_selector_all("li")
                print(f"    Items: {len(items)}")

            # div.box_type_m 확인
            box = page.query_selector("div.box_type_m")
            if box:
                print("  ✓ Found div.box_type_m")

        # 4. 리포트 링크 확인
        print("\n=== Report Links ===")

        # 제목 링크
        title_links = page.query_selector_all("a[href*='company_read']")
        print(f"Title links: {len(title_links)}")

        if title_links:
            first_link = title_links[0]
            print(f"  First: {first_link.inner_text()[:50]}")
            print(f"  href: {first_link.get_attribute('href')}")

        # PDF 링크
        pdf_links = page.query_selector_all("a[href$='.pdf']")
        print(f"\nPDF links: {len(pdf_links)}")

        if pdf_links:
            first_pdf = pdf_links[0]
            print(f"  First PDF: {first_pdf.get_attribute('href')}")

        # 5. 스크린샷
        page.screenshot(path="naver_debug.png")
        print("\n✓ Screenshot saved: naver_debug.png")

        # 6. HTML 저장
        html = page.content()
        with open("naver_debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("✓ HTML saved: naver_debug.html")

        browser.close()

        print("\n=== 완료 ===")
        print("파일을 확인하고 HTML 구조를 분석하세요.")


if __name__ == "__main__":
    check_naver_page()