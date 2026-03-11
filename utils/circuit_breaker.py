import time
import threading
from enum import Enum
from typing import Optional, Callable, Any, Dict
from dataclasses import dataclass
from functools import wraps
import logging

from utils.redis_client import get_redis_client

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitMetrics:
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 3,
        timeout: float = 60.0,
        half_open_max_calls: int = 3,
        redis_key_prefix: str = "circuit_breaker:",
        fallback: Optional[Callable] = None
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout = timeout
        self.half_open_max_calls = half_open_max_calls
        self.redis_key_prefix = redis_key_prefix
        self.fallback = fallback

        self._state = CircuitState.CLOSED
        self._metrics = CircuitMetrics()
        self._lock = threading.RLock()
        self._last_state_change = time.time()
        self._half_open_calls = 0

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def metrics(self) -> CircuitMetrics:
        return self._metrics

    def _get_redis_key(self) -> str:
        return f"{self.redis_key_prefix}{self.name}"

    def _get_state_from_redis(self, redis_client) -> Optional[CircuitState]:
        try:
            key = self._get_redis_key()
            
            # 检查是否是异步Redis客户端
            is_async = hasattr(redis_client, '_pool') and hasattr(redis_client, '_init_pool')
            
            if is_async:
                # 异步Redis客户端需要特殊处理
                # 由于当前方法是同步的，我们无法直接使用await
                # 所以这里我们直接返回None，让本地状态优先
                logger.debug("Skipping Redis state sync for async client in sync method")
                return None
            else:
                # 同步Redis客户端
                data = redis_client.hgetall(key)
                if data:
                    state_str = data.get("state")
                    if state_str:
                        return CircuitState(state_str)
                return None
        except Exception as e:
            logger.warning(f"Failed to get circuit breaker state from Redis: {e}")
            return None

    def _save_state_to_redis(self, redis_client, state: CircuitState):
        try:
            key = self._get_redis_key()
            
            # 检查是否是异步Redis客户端
            is_async = hasattr(redis_client, '_pool') and hasattr(redis_client, '_init_pool')
            
            if is_async:
                # 异步Redis客户端需要特殊处理
                # 由于当前方法是同步的，我们无法直接使用await
                # 所以这里我们记录一个警告，但不执行实际操作
                logger.debug("Skipping Redis state save for async client in sync method")
            else:
                # 同步Redis客户端
                data = {
                    "state": state.value,
                    "last_state_change": str(self._last_state_change),
                    "half_open_calls": str(self._half_open_calls)
                }
                redis_client.hset(key, mapping=data)
                redis_client.expire(key, self.timeout * 2)
        except Exception as e:
            logger.warning(f"Failed to save circuit breaker state to Redis: {e}")

    def _sync_state(self):
        try:
            redis_client = get_redis_client()
            if redis_client.is_healthy:
                redis_state = self._get_state_from_redis(redis_client)
                if redis_state:
                    if self._should_transition_from_redis(redis_state):
                        self._state = redis_state
                        self._last_state_change = time.time()
                        self._half_open_calls = 0
                        logger.info(f"Circuit breaker '{self.name}' synced state from Redis: {self._state.value}")
        except Exception as e:
            logger.warning(f"Failed to sync circuit breaker state: {e}")

    def _should_transition_from_redis(self, redis_state: CircuitState) -> bool:
        if redis_state == CircuitState.OPEN and self._state == CircuitState.CLOSED:
            return True
        if redis_state == CircuitState.HALF_OPEN and self._state == CircuitState.CLOSED:
            return True
        return False

    def _transition_to(self, new_state: CircuitState):
        with self._lock:
            old_state = self._state
            self._state = new_state
            self._last_state_change = time.time()

            if new_state == CircuitState.HALF_OPEN:
                self._half_open_calls = 0

            try:
                redis_client = get_redis_client()
                if redis_client.is_healthy:
                    self._save_state_to_redis(redis_client, new_state)
            except Exception as e:
                logger.warning(f"Failed to save state transition to Redis: {e}")

            logger.info(f"Circuit breaker '{self.name}' transitioned from {old_state.value} to {new_state.value}")

    def _check_timeout(self) -> bool:
        if self._state == CircuitState.OPEN:
            elapsed = time.time() - self._last_state_change
            if elapsed >= self.timeout:
                self._transition_to(CircuitState.HALF_OPEN)
                return True
        return False

    def _record_success(self):
        self._metrics.total_calls += 1
        self._metrics.successful_calls += 1
        self._metrics.consecutive_successes += 1
        self._metrics.consecutive_failures = 0
        self._metrics.last_success_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._half_open_calls += 1
            if self._half_open_calls >= self.half_open_max_calls:
                if self._metrics.consecutive_successes >= self.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
                    self._metrics.consecutive_successes = 0

    def _record_failure(self, error: Exception):
        self._metrics.total_calls += 1
        self._metrics.failed_calls += 1
        self._metrics.consecutive_failures += 1
        self._metrics.consecutive_successes = 0
        self._metrics.last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._transition_to(CircuitState.OPEN)
        elif self._state == CircuitState.CLOSED:
            if self._metrics.consecutive_failures >= self.failure_threshold:
                self._transition_to(CircuitState.OPEN)

    def call(self, func: Callable, *args, **kwargs) -> Any:
        self._sync_state()
        self._check_timeout()

        if self._state == CircuitState.OPEN:
            if self.fallback:
                return self.fallback(*args, **kwargs)
            raise CircuitBreakerOpenError(f"Circuit breaker '{self.name}' is open")

        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure(e)
            if self.fallback:
                return self.fallback(*args, **kwargs)
            raise

    def reset(self):
        with self._lock:
            self._state = CircuitState.CLOSED
            self._metrics = CircuitMetrics()
            self._last_state_change = time.time()
            self._half_open_calls = 0

            try:
                redis_client = get_redis_client()
                if redis_client.is_healthy:
                    redis_client.delete(self._get_redis_key())
            except Exception as e:
                logger.warning(f"Failed to reset circuit breaker in Redis: {e}")

            logger.info(f"Circuit breaker '{self.name}' has been reset")

    def get_info(self) -> Dict[str, Any]:
        self._sync_state()
        return {
            "name": self.name,
            "state": self._state.value,
            "failure_threshold": self.failure_threshold,
            "success_threshold": self.success_threshold,
            "timeout": self.timeout,
            "metrics": {
                "total_calls": self._metrics.total_calls,
                "successful_calls": self._metrics.successful_calls,
                "failed_calls": self._metrics.failed_calls,
                "consecutive_failures": self._metrics.consecutive_failures,
                "consecutive_successes": self._metrics.consecutive_successes
            },
            "last_state_change": self._last_state_change
        }


class CircuitBreakerOpenError(Exception):
    pass


class CircuitBreakerRegistry:
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
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()
        self._initialized = True

    def register(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 3,
        timeout: float = 60.0,
        half_open_max_calls: int = 3,
        fallback: Optional[Callable] = None
    ) -> CircuitBreaker:
        with self._lock:
            if name not in self._circuit_breakers:
                self._circuit_breakers[name] = CircuitBreaker(
                    name=name,
                    failure_threshold=failure_threshold,
                    success_threshold=success_threshold,
                    timeout=timeout,
                    half_open_max_calls=half_open_max_calls,
                    fallback=fallback
                )
            return self._circuit_breakers[name]

    def get(self, name: str) -> Optional[CircuitBreaker]:
        with self._lock:
            return self._circuit_breakers.get(name)

    def remove(self, name: str):
        with self._lock:
            if name in self._circuit_breakers:
                del self._circuit_breakers[name]

    def get_all(self) -> Dict[str, CircuitBreaker]:
        with self._lock:
            return dict(self._circuit_breakers)

    def get_all_info(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {name: cb.get_info() for name, cb in self._circuit_breakers.items()}

    def reset_all(self):
        with self._lock:
            for cb in self._circuit_breakers.values():
                cb.reset()


def get_circuit_breaker_registry() -> CircuitBreakerRegistry:
    return CircuitBreakerRegistry()


def get_circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    success_threshold: int = 3,
    timeout: float = 60.0,
    half_open_max_calls: int = 3,
    fallback: Optional[Callable] = None
) -> CircuitBreaker:
    """
    获取或创建熔断器实例
    
    Args:
        name: 熔断器名称
        failure_threshold: 失败阈值
        success_threshold: 成功阈值
        timeout: 超时时间（秒）
        half_open_max_calls: 半开状态下的最大调用次数
        fallback: 降级函数
        
    Returns:
        CircuitBreaker 实例
    """
    registry = get_circuit_breaker_registry()
    return registry.register(
        name=name,
        failure_threshold=failure_threshold,
        success_threshold=success_threshold,
        timeout=timeout,
        half_open_max_calls=half_open_max_calls,
        fallback=fallback
    )


def circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    success_threshold: int = 3,
    timeout: float = 60.0,
    fallback: Optional[Callable] = None
):
    registry = get_circuit_breaker_registry()

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cb = registry.register(
                name=name,
                failure_threshold=failure_threshold,
                success_threshold=success_threshold,
                timeout=timeout,
                fallback=fallback
            )
            return cb.call(func, *args, **kwargs)
        return wrapper
    return decorator


def create_llm_circuit_breaker(provider: str, model: str) -> CircuitBreaker:
    registry = get_circuit_breaker_registry()
    return registry.register(
        name=f"llm:{provider}:{model}",
        failure_threshold=5,
        success_threshold=3,
        timeout=60.0
    )


def create_external_service_circuit_breaker(service_name: str) -> CircuitBreaker:
    registry = get_circuit_breaker_registry()
    return registry.register(
        name=f"service:{service_name}",
        failure_threshold=3,
        success_threshold=2,
        timeout=30.0
    )
