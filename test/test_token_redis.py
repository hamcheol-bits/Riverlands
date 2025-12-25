# test_token_redis.py
import asyncio
import logging
from app.core.kis_auth import get_auth_manager
from app.core.redis_client import get_redis_client

logging.basicConfig(level=logging.INFO)


async def test():
    print("=== Testing KIS Token Redis Storage ===\n")

    # 1. Redis 연결 확인
    redis_client = get_redis_client()
    if not redis_client:
        print("❌ Redis client is None!")
        return

    print("✅ Redis client connected")

    # 2. 기존 토큰 삭제
    auth_manager = get_auth_manager()
    auth_manager.invalidate_token()
    print("🗑️  Cleared existing token")

    # 3. 새 토큰 발급 (강제)
    print("\n📡 Requesting new token...")
    token = await auth_manager.get_access_token(force_refresh=True)
    print(f"✅ Token received: {token[:30]}...")

    # 4. Redis에서 직접 확인
    print("\n🔍 Checking Redis...")
    redis_token = redis_client.get(auth_manager.redis_token_key)
    ttl = redis_client.ttl(auth_manager.redis_token_key)

    if redis_token:
        print(f"✅ Token in Redis: {redis_token[:30]}...")
        print(f"✅ TTL: {ttl}s ({ttl / 3600:.2f}h)")
    else:
        print("❌ Token NOT in Redis!")
        print(f"   Key used: {auth_manager.redis_token_key}")

        # 모든 키 확인
        all_keys = redis_client.keys("*")
        print(f"   All keys in Redis: {all_keys}")

    # 5. 다시 조회해보기 (캐시 사용)
    print("\n🔄 Getting token again (should use cache)...")
    cached_token = await auth_manager.get_access_token()
    print(f"✅ Token from cache: {cached_token[:30]}...")


if __name__ == "__main__":
    asyncio.run(test())