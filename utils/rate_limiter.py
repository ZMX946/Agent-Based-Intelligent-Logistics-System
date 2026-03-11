import time
import threading
import uuid
from enum import Enum
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass
from functools import wraps
import logging

from utils.redis_client import get_redis_client

logger = logging.getLogger(__name__)


class RateLimitStrategy(Enum):
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitConfig:
    max_tokens: int
    refill_rate: float
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET
    block_duration: float = 0.0
    burst_multiplier: float = 1.0


@dataclass
class RateLimitResult:
    allowed: bool
    remaining_tokens: int
    reset_time: float
    limit: int
    retry_after: Optional[float] = None


class TokenBucketRateLimiter:
    def __init__(self, config: RateLimitConfig, key: str, redis_key_prefix: str = "rate_limit:"):
        self.config = config
        self.key = key
        self.redis_key_prefix = redis_key_prefix
        self._lock = threading.RLock()

    def _get_redis_key(self) -> str:
        return f"{self.redis_key_prefix}token_bucket:{self.key}"

    def _get_redis_client(self):
        return get_redis_client()

    def _consume_tokens_redis(self, tokens: int = 1) -> RateLimitResult:
        client = self._get_redis_client()
        if not client.is_healthy:
            return self._consume_tokens_local(tokens)

        try:
            # 检测客户端是否为异步客户端
            is_async_client = hasattr(client, '_pool') and hasattr(client, '_client') and hasattr(client, '_init_pool')
            
            if is_async_client:
                # 对于异步客户端，直接使用本地限流
                logger.warning("异步Redis客户端不支持同步操作，使用本地限流")
                return self._consume_tokens_local(tokens)
            
            # 同步客户端处理逻辑
            key = self._get_redis_key()
            now = time.time()
            refill_time = 1.0 / self.config.refill_rate if self.config.refill_rate > 0 else float('inf')

            pipeline = client.pipeline()

            current = client.hgetall(key)

            if current:
                last_update = float(current.get("last_update", now))
                tokens = float(current.get("tokens", self.config.max_tokens))
            else:
                last_update = now
                tokens = self.config.max_tokens

            elapsed = now - last_update
            new_tokens = min(
                tokens + elapsed * self.config.refill_rate,
                self.config.max_tokens * self.config.burst_multiplier
            )

            if new_tokens >= tokens:
                remaining = new_tokens - tokens
                pipeline.hset(key, mapping={
                    "tokens": str(remaining),
                    "last_update": str(now)
                })
                pipeline.expire(key, int(refill_time * 2))
                pipeline.execute()

                return RateLimitResult(
                    allowed=True,
                    remaining_tokens=int(remaining),
                    reset_time=now + (remaining / self.config.refill_rate) if self.config.refill_rate > 0 else now,
                    limit=self.config.max_tokens
                )
            else:
                wait_time = (tokens - new_tokens) / self.config.refill_rate if self.config.refill_rate > 0 else 0
                retry_after = max(wait_time, 0)

                return RateLimitResult(
                    allowed=False,
                    remaining_tokens=int(new_tokens),
                    reset_time=now + retry_after,
                    limit=self.config.max_tokens,
                    retry_after=retry_after
                )
        except Exception as e:
            logger.warning(f"Redis rate limit error, falling back to local: {e}")
            return self._consume_tokens_local(tokens)

    def _consume_tokens_local(self, tokens: int = 1) -> RateLimitResult:
        now = time.time()

        with self._lock:
            if not hasattr(self, "_local_tokens"):
                self._local_tokens = self.config.max_tokens
                self._local_last_update = now

            elapsed = now - self._local_last_update
            self._local_tokens = min(
                self._local_tokens + elapsed * self.config.refill_rate,
                self.config.max_tokens * self.config.burst_multiplier
            )
            self._local_last_update = now

            if self._local_tokens >= tokens:
                self._local_tokens -= tokens
                return RateLimitResult(
                    allowed=True,
                    remaining_tokens=int(self._local_tokens),
                    reset_time=now + (self._local_tokens / self.config.refill_rate) if self.config.refill_rate > 0 else now,
                    limit=self.config.max_tokens
                )
            else:
                wait_time = (tokens - self._local_tokens) / self.config.refill_rate if self.config.refill_rate > 0 else 0
                return RateLimitResult(
                    allowed=False,
                    remaining_tokens=int(self._local_tokens),
                    reset_time=now + wait_time,
                    limit=self.config.max_tokens,
                    retry_after=wait_time
                )

    def consume(self, tokens: int = 1) -> RateLimitResult:
        return self._consume_tokens_redis(tokens)

    def get_current_tokens(self) -> float:
        client = self._get_redis_client()
        if not client.is_healthy:
            with self._lock:
                if hasattr(self, "_local_tokens"):
                    return self._local_tokens
                return self.config.max_tokens

        try:
            key = self._get_redis_key()
            current = client.hgetall(key)
            if current:
                last_update = float(current.get("last_update", time.time()))
                tokens = float(current.get("tokens", self.config.max_tokens))
                elapsed = time.time() - last_update
                return min(
                    tokens + elapsed * self.config.refill_rate,
                    self.config.max_tokens * self.config.burst_multiplier
                )
            return self.config.max_tokens
        except Exception as e:
            logger.warning(f"Failed to get current tokens: {e}")
            return self.config.max_tokens

    def reset(self):
        client = self._get_redis_client()
        try:
            client.delete(self._get_redis_key())
        except Exception as e:
            logger.warning(f"Failed to reset rate limiter: {e}")
        with self._lock:
            if hasattr(self, "_local_tokens"):
                del self._local_tokens
                del self._local_last_update


class SlidingWindowRateLimiter:
    def __init__(self, config: RateLimitConfig, key: str, redis_key_prefix: str = "rate_limit:"):
        self.config = config
        self.key = key
        self.redis_key_prefix = redis_key_prefix

    def _get_redis_key(self) -> str:
        return f"{self.redis_key_prefix}sliding:{self.key}"

    def _get_redis_client(self):
        return get_redis_client()

    def consume(self, tokens: int = 1) -> RateLimitResult:
        client = self._get_redis_client()
        if not client.is_healthy:
            return self._consume_local(tokens)

        try:
            # 检测客户端是否为异步客户端
            is_async_client = hasattr(client, '_pool') and hasattr(client, '_client') and hasattr(client, '_init_pool')
            
            if is_async_client:
                # 对于异步客户端，直接使用本地限流
                logger.warning("异步Redis客户端不支持同步操作，使用本地限流")
                return self._consume_local(tokens)
            
            # 同步客户端处理逻辑
            key = self._get_redis_key()
            now = time.time()
            window_size = self.config.max_tokens / self.config.refill_rate if self.config.refill_rate > 0 else 1
            window_start = now - window_size

            pipeline = client.pipeline()

            pipeline.zremrangebyscore(key, 0, window_start)
            pipeline.zcard(key)  # 将zcard操作添加到管道中

            # 执行管道获取结果
            results = pipeline.execute()
            
            # 第二个结果是zcard的结果
            current_count = results[1]

            if current_count + tokens <= self.config.max_tokens:
                members = [(str(uuid.uuid4()), now + i * 0.001) for i in range(tokens)]
                
                pipeline = client.pipeline()  # 创建新的管道
                # zadd的正确格式是 {member: score}
                pipeline.zadd(key, {member[0]: member[1] for member in members})
                pipeline.expire(key, int(window_size * 2))
                pipeline.execute()

                remaining = self.config.max_tokens - (current_count + tokens)
                reset_time = now + window_size

                return RateLimitResult(
                    allowed=True,
                    remaining_tokens=int(remaining),
                    reset_time=reset_time,
                    limit=self.config.max_tokens
                )
            else:
                wait_time = window_size
                retry_after = wait_time

                return RateLimitResult(
                    allowed=False,
                    remaining_tokens=0,
                    reset_time=now + wait_time,
                    limit=self.config.max_tokens,
                    retry_after=retry_after
                )
        except Exception as e:
            logger.warning(f"Redis sliding window error, falling back to local: {e}")
            return self._consume_local(tokens)

    def _consume_local(self, tokens: int = 1) -> RateLimitResult:
        now = time.time()

        if not hasattr(self, "_local_requests"):
            self._local_requests = []
            self._local_window_start = now

        window_size = self.config.max_tokens / self.config.refill_rate if self.config.refill_rate > 0 else 1

        self._local_requests = [t for t in self._local_requests if t >= now - window_size]

        if len(self._local_requests) + tokens <= self.config.max_tokens:
            self._local_requests.extend([now + i * 0.001 for i in range(tokens)])
            remaining = self.config.max_tokens - len(self._local_requests)
            return RateLimitResult(
                allowed=True,
                remaining_tokens=remaining,
                reset_time=now + window_size,
                limit=self.config.max_tokens
            )
        else:
            wait_time = window_size
            return RateLimitResult(
                allowed=False,
                remaining_tokens=0,
                reset_time=now + wait_time,
                limit=self.config.max_tokens,
                retry_after=wait_time
            )

    def get_current_count(self) -> int:
        client = self._get_redis_client()
        if not client.is_healthy:
            if hasattr(self, "_local_requests"):
                return len(self._local_requests)
            return 0

        try:
            key = self._get_redis_key()
            window_size = self.config.max_tokens / self.config.refill_rate if self.config.refill_rate > 0 else 1
            window_start = time.time() - window_size
            return client.zcount(key, window_start, "+inf")
        except Exception as e:
            logger.warning(f"Failed to get current count: {e}")
            return 0

    def reset(self):
        client = self._get_redis_client()
        try:
            client.delete(self._get_redis_key())
        except Exception as e:
            logger.warning(f"Failed to reset rate limiter: {e}")
        if hasattr(self, "_local_requests"):
            del self._local_requests


class FixedWindowRateLimiter:
    def __init__(self, config: RateLimitConfig, key: str, redis_key_prefix: str = "rate_limit:"):
        self.config = config
        self.key = key
        self.redis_key_prefix = redis_key_prefix

    def _get_redis_key(self) -> str:
        return f"{self.redis_key_prefix}fixed:{self.key}"

    def _get_redis_client(self):
        return get_redis_client()

    def _get_window_key(self) -> str:
        now = time.time()
        window_size = self.config.max_tokens / self.config.refill_rate if self.config.refill_rate > 0 else 1
        window_start = int(now / window_size) * window_size
        return f"{self._get_redis_key()}:{window_start}"

    def consume(self, tokens: int = 1) -> RateLimitResult:
        client = self._get_redis_client()
        if not client.is_healthy:
            return self._consume_local(tokens)

        try:
            # 检测客户端是否为异步客户端
            is_async_client = hasattr(client, '_pool') and hasattr(client, '_client') and hasattr(client, '_init_pool')
            
            if is_async_client:
                # 对于异步客户端，直接使用本地限流
                logger.warning("异步Redis客户端不支持同步操作，使用本地限流")
                return self._consume_local(tokens)
            
            # 同步客户端处理逻辑
            window_key = self._get_window_key()
            window_size = self.config.max_tokens / self.config.refill_rate if self.config.refill_rate > 0 else 1

            current = client.get(window_key)
            current_count = int(current) if current else 0

            if current_count + tokens <= self.config.max_tokens:
                client.incrby(window_key, tokens)
                client.expire(window_key, int(window_size * 2))

                remaining = self.config.max_tokens - (current_count + tokens)
                now = time.time()
                reset_time = int(now / window_size) * window_size + window_size

                return RateLimitResult(
                    allowed=True,
                    remaining_tokens=int(remaining),
                    reset_time=reset_time,
                    limit=self.config.max_tokens
                )
            else:
                wait_time = window_size
                now = time.time()
                reset_time = int(now / window_size) * window_size + window_size

                return RateLimitResult(
                    allowed=False,
                    remaining_tokens=0,
                    reset_time=reset_time,
                    limit=self.config.max_tokens,
                    retry_after=wait_time
                )
        except Exception as e:
            logger.warning(f"Redis fixed window error, falling back to local: {e}")
            return self._consume_local(tokens)

    def _consume_local(self, tokens: int = 1) -> RateLimitResult:
        now = time.time()
        window_size = self.config.max_tokens / self.config.refill_rate if self.config.refill_rate > 0 else 1
        window_start = int(now / window_size) * window_size

        if not hasattr(self, "_local_windows"):
            self._local_windows = {}

        if window_start not in self._local_windows:
            self._local_windows = {window_start: 0}

        current_count = self._local_windows.get(window_start, 0)

        if current_count + tokens <= self.config.max_tokens:
            self._local_windows[window_start] = current_count + tokens
            remaining = self.config.max_tokens - (current_count + tokens)
            reset_time = window_start + window_size

            return RateLimitResult(
                allowed=True,
                remaining_tokens=int(remaining),
                reset_time=reset_time,
                limit=self.config.max_tokens
            )
        else:
            wait_time = window_size
            reset_time = window_start + window_size

            return RateLimitResult(
                allowed=False,
                remaining_tokens=0,
                reset_time=reset_time,
                limit=self.config.max_tokens,
                retry_after=wait_time
            )

    def get_current_count(self) -> int:
        client = self._get_redis_client()
        if not client.is_healthy:
            if hasattr(self, "_local_windows"):
                now = time.time()
                window_size = self.config.max_tokens / self.config.refill_rate if self.config.refill_rate > 0 else 1
                window_start = int(now / window_size) * window_size
                return self._local_windows.get(window_start, 0)
            return 0

        try:
            window_key = self._get_window_key()
            current = client.get(window_key)
            return int(current) if current else 0
        except Exception as e:
            logger.warning(f"Failed to get current count: {e}")
            return 0

    def reset(self):
        client = self._get_redis_client()
        pattern = f"{self._get_redis_key()}:*"
        try:
            keys = client.keys(pattern)
            if keys:
                client.delete(*keys)
        except Exception as e:
            logger.warning(f"Failed to reset rate limiter: {e}")
        if hasattr(self, "_local_windows"):
            del self._local_windows


class RateLimiterFactory:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._limiters: Dict[str, Tuple[RateLimitConfig, RateLimitStrategy]] = {}
        self._lock = threading.RLock()
        self._initialized = True

    def create_limiter(
        self,
        key: str,
        max_tokens: int,
        refill_rate: float,
        strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET,
        block_duration: float = 0.0,
        burst_multiplier: float = 1.0
    ) -> Any:
        config = RateLimitConfig(
            max_tokens=max_tokens,
            refill_rate=refill_rate,
            strategy=strategy,
            block_duration=block_duration,
            burst_multiplier=burst_multiplier
        )

        with self._lock:
            self._limiters[key] = (config, strategy)

        if strategy == RateLimitStrategy.TOKEN_BUCKET:
            return TokenBucketRateLimiter(config, key)
        elif strategy == RateLimitStrategy.SLIDING_WINDOW:
            return SlidingWindowRateLimiter(config, key)
        else:
            return FixedWindowRateLimiter(config, key)

    def get_limiter(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._limiters:
                return None
            config, strategy = self._limiters[key]

        if strategy == RateLimitStrategy.TOKEN_BUCKET:
            return TokenBucketRateLimiter(config, key)
        elif strategy == RateLimitStrategy.SLIDING_WINDOW:
            return SlidingWindowRateLimiter(config, key)
        else:
            return FixedWindowRateLimiter(config, key)

    def get_config(self, key: str) -> Optional[RateLimitConfig]:
        with self._lock:
            if key in self._limiters:
                return self._limiters[key][0]
            return None

    def remove(self, key: str):
        with self._lock:
            if key in self._limiters:
                del self._limiters[key]

    def get_all_configs(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                key: {
                    "max_tokens": config.max_tokens,
                    "refill_rate": config.refill_rate,
                    "strategy": config.strategy.value,
                    "block_duration": config.block_duration,
                    "burst_multiplier": config.burst_multiplier
                }
                for key, (config, _) in self._limiters.items()
            }


# 单例实例
_rate_limiter_factory_instance = None


def get_rate_limiter_factory() -> RateLimiterFactory:
    """
    获取速率限制器工厂的单例实例
    
    Returns:
        RateLimiterFactory: 速率限制器工厂的单例实例
    """
    global _rate_limiter_factory_instance
    if _rate_limiter_factory_instance is None:
        _rate_limiter_factory_instance = RateLimiterFactory()
    return _rate_limiter_factory_instance


def rate_limit(
    key: str,
    max_tokens: int,
    refill_rate: float,
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET,
    fallback: Optional[callable] = None
):
    factory = get_rate_limiter_factory()
    limiter = factory.create_limiter(
        key=key,
        max_tokens=max_tokens,
        refill_rate=refill_rate,
        strategy=strategy
    )

    def decorator(func: callable) -> callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = limiter.consume()
            if not result.allowed:
                if fallback:
                    return fallback(*args, **kwargs)
                raise RateLimitExceededError(
                    f"Rate limit exceeded for key '{key}'. Retry after {result.retry_after:.2f}s"
                )
            return func(*args, **kwargs)
        return wrapper
    return decorator


class RateLimitExceededError(Exception):
    pass


def create_api_rate_limiter(api_key: str = "global") -> Any:
    factory = get_rate_limiter_factory()
    return factory.create_limiter(
        key=f"api:{api_key}",
        max_tokens=100,
        refill_rate=10,
        strategy=RateLimitStrategy.TOKEN_BUCKET
    )


def create_llm_rate_limiter(provider: str, model: str) -> Any:
    factory = get_rate_limiter_factory()
    return factory.create_limiter(
        key=f"llm:{provider}:{model}",
        max_tokens=50,
        refill_rate=5,
        strategy=RateLimitStrategy.TOKEN_BUCKET
    )


def create_user_rate_limiter(user_id: str) -> Any:
    factory = get_rate_limiter_factory()
    return factory.create_limiter(
        key=f"user:{user_id}",
        max_tokens=1000,
        refill_rate=100,
        strategy=RateLimitStrategy.SLIDING_WINDOW
    )
