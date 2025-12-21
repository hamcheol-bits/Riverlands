"""
KIS API 재무제표 필드명 확인 스크립트
실제 API 응답을 확인하여 올바른 필드명 매핑
"""
import asyncio
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# 프로젝트 루트 경로 찾기
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# .env 파일 로드
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(env_path)
    print(f"✓ .env 파일 로드: {env_path}")
else:
    print(f"✗ .env 파일 없음: {env_path}")
    sys.exit(1)

from app.services.financial_service import get_financial_service


async def debug_financial_fields(ticker: str = "005930"):
    """재무제표 API 응답 필드명 확인"""

    service = get_financial_service()

    print(f"\n{'='*80}")
    print(f"KIS API 재무제표 필드명 확인 - {ticker}")
    print(f"{'='*80}\n")

    # 1. 대차대조표
    print("1. 대차대조표 (Balance Sheet)")
    print("-" * 80)
    balance_sheets = await service.collect_balance_sheet(ticker, "0")
    if balance_sheets:
        print(f"응답 레코드 수: {len(balance_sheets)}")
        print(f"필드 목록: {list(balance_sheets[0].keys())}")
        print(f"\n첫 번째 레코드 샘플:")
        for key, value in list(balance_sheets[0].items())[:10]:
            print(f"  {key}: {value}")
    else:
        print("데이터 없음")

    # 2. 손익계산서
    print(f"\n2. 손익계산서 (Income Statement)")
    print("-" * 80)
    income_statements = await service.collect_income_statement(ticker, "0")
    if income_statements:
        print(f"응답 레코드 수: {len(income_statements)}")
        print(f"필드 목록: {list(income_statements[0].keys())}")
        print(f"\n첫 번째 레코드 샘플:")
        for key, value in list(income_statements[0].items())[:10]:
            print(f"  {key}: {value}")
    else:
        print("데이터 없음")

    # 3. 재무비율
    print(f"\n3. 재무비율 (Financial Ratios)")
    print("-" * 80)
    financial_ratios = await service.collect_financial_ratios(ticker, "0")
    if financial_ratios:
        print(f"응답 레코드 수: {len(financial_ratios)}")
        print(f"필드 목록: {list(financial_ratios[0].keys())}")
        print(f"\n첫 번째 레코드 샘플:")
        for key, value in list(financial_ratios[0].items())[:10]:
            print(f"  {key}: {value}")
    else:
        print("데이터 없음")

    # 4. 수익성비율
    print(f"\n4. 수익성비율 (Profit Ratios)")
    print("-" * 80)
    profit_ratios = await service.collect_profit_ratios(ticker, "0")
    if profit_ratios:
        print(f"응답 레코드 수: {len(profit_ratios)}")
        print(f"필드 목록: {list(profit_ratios[0].keys())}")
        print(f"\n첫 번째 레코드 샘플:")
        for key, value in list(profit_ratios[0].items())[:10]:
            print(f"  {key}: {value}")
    else:
        print("데이터 없음")

    # 5. 기타주요비율
    print(f"\n5. 기타주요비율 (Other Major Ratios)")
    print("-" * 80)
    other_ratios = await service.collect_other_major_ratios(ticker, "0")
    if other_ratios:
        print(f"응답 레코드 수: {len(other_ratios)}")
        print(f"필드 목록: {list(other_ratios[0].keys())}")
        print(f"\n첫 번째 레코드 샘플:")
        for key, value in list(other_ratios[0].items())[:10]:
            print(f"  {key}: {value}")
    else:
        print("데이터 없음")

    # 6. 성장성비율
    print(f"\n6. 성장성비율 (Growth Ratios)")
    print("-" * 80)
    growth_ratios = await service.collect_growth_ratios(ticker, "0")
    if growth_ratios:
        print(f"응답 레코드 수: {len(growth_ratios)}")
        print(f"필드 목록: {list(growth_ratios[0].keys())}")
        print(f"\n첫 번째 레코드 샘플:")
        for key, value in list(growth_ratios[0].items())[:10]:
            print(f"  {key}: {value}")
    else:
        print("데이터 없음")

    print(f"\n{'='*80}")
    print("완료!")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    ticker = sys.argv[1] if len(sys.argv) > 1 else "005930"
    asyncio.run(debug_financial_fields(ticker))