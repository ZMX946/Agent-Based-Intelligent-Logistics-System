import time
import random
import threading
from enum import Enum
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field

import logging

from utils.redis_client import get_redis_client
from utils.circuit_breaker import (
    get_circuit_breaker_registry
)
from utils.rate_limiter import (
    get_rate_limiter_factory,
    RateLimitStrategy
)

logger = logging.getLogger(__name__)


class RouterStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    WEIGHTED = "weighted"
    FAILOVER = "failover"
    LOAD_BALANCE = "load_balance"
    COST_OPTIMIZED = "cost_optimized"
    LATENCY_OPTIMIZED = "latency_optimized"


class ProviderStatus(Enum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    MAINTENANCE = "maintenance"


@dataclass
class LLMProvider:
    name: str
    api_base: str
    api_key: str
    model: str
    weight: float = 1.0
    cost_per_token: float = 0.001
    avg_latency: float = 1.0
    timeout: float = 30.0
    max_tokens: int = 4096
    status: ProviderStatus = ProviderStatus.ACTIVE
    priority: int = 0
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class RouterConfig:
    strategy: RouterStrategy = RouterStrategy.FAILOVER
    health_check_interval: float = 30.0
    failure_count_threshold: int = 3
    recovery_count_threshold: int = 2
    max_retries: int = 3
    retry_delay: float = 1.0
    enable_fallback: bool = True
    default_provider: Optional[str] = None
    timeout_multiplier: float = 1.5


@dataclass
class RequestMetrics:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency: float = 0.0
    total_tokens: int = 0
    total_cost: float = 0.0
    last_request_time: Optional[float] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class LLMProviderManager:
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
        self._initialized = True
        self._providers: Dict[str, LLMProvider] = {}
        self._metrics: Dict[str, RequestMetrics] = {}
        self._lock = threading.RLock()
        self._health_check_thread: Optional[threading.Thread] = None
        self._stop_health_check = threading.Event()
        self._initialized = True

    def register_provider(
        self,
        name: str,
        api_base: str,
        api_key: str,
        model: str,
        weight: float = 1.0,
        cost_per_token: float = 0.001,
        avg_latency: float = 1.0,
        timeout: float = 30.0,
        max_tokens: int = 4096,
        priority: int = 0,
        tags: Optional[Dict[str, str]] = None
    ) -> LLMProvider:
        with self._lock:
            provider = LLMProvider(
                name=name,
                api_base=api_base,
                api_key=api_key,
                model=model,
                weight=weight,
                cost_per_token=cost_per_token,
                avg_latency=avg_latency,
                timeout=timeout,
                max_tokens=max_tokens,
                priority=priority,
                tags=tags or {}
            )
            self._providers[name] = provider
            self._metrics[name] = RequestMetrics()
            return provider

    def get_provider(self, name: str) -> Optional[LLMProvider]:
        with self._lock:
            return self._providers.get(name)

    def get_all_providers(self) -> Dict[str, LLMProvider]:
        with self._lock:
            return dict(self._providers)

    def get_active_providers(self) -> List[LLMProvider]:
        with self._lock:
            return [
                p for p in self._providers.values()
                if p.status in (ProviderStatus.ACTIVE, ProviderStatus.DEGRADED)
            ]

    def update_provider_status(self, name: str, status: ProviderStatus):
        with self._lock:
            if name in self._providers:
                self._providers[name].status = status
                logger.info(f"Provider '{name}' status updated to {status.value}")

    def record_request(self, name: str, success: bool, latency: float, tokens: int = 0, cost: float = 0.0):
        with self._lock:
            if name not in self._metrics:
                self._metrics[name] = RequestMetrics()

            metrics = self._metrics[name]
            metrics.total_requests += 1
            metrics.total_latency += latency
            metrics.total_tokens += tokens
            metrics.total_cost += cost
            metrics.last_request_time = time.time()

            if success:
                metrics.successful_requests += 1
                metrics.consecutive_successes += 1
                metrics.consecutive_failures = 0
            else:
                metrics.failed_requests += 1
                metrics.consecutive_failures += 1
                metrics.consecutive_successes = 0

                if metrics.consecutive_failures >= 3:
                    self.update_provider_status(name, ProviderStatus.DEGRADED)
            if metrics.consecutive_successes >= 2 and self._providers.get(name, {}).status == ProviderStatus.DEGRADED:
                self.update_provider_status(name, ProviderStatus.ACTIVE)

    def get_provider_metrics(self, name: str) -> Optional[RequestMetrics]:
        with self._lock:
            return self._metrics.get(name)

    def get_provider_stats(self, name: str) -> Dict[str, Any]:
        with self._lock:
            metrics = self._metrics.get(name)
            provider = self._providers.get(name)
            if not metrics or not provider:
                return {}

            avg_latency = metrics.total_latency / metrics.total_requests if metrics.total_requests > 0 else 0
            success_rate = metrics.successful_requests / metrics.total_requests if metrics.total_requests > 0 else 0

            return {
                "name": name,
                "model": provider.model,
                "status": provider.status.value,
                "total_requests": metrics.total_requests,
                "successful_requests": metrics.successful_requests,
                "failed_requests": metrics.failed_requests,
                "success_rate": f"{success_rate * 100:.2f}%",
                "avg_latency": f"{avg_latency:.3f}s",
                "total_tokens": metrics.total_tokens,
                "total_cost": f"${metrics.total_cost:.4f}",
                "consecutive_failures": metrics.consecutive_failures,
                "consecutive_successes": metrics.consecutive_successes
            }

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {name: self.get_provider_stats(name) for name in self._providers}

    def remove_provider(self, name: str):
        with self._lock:
            if name in self._providers:
                del self._providers[name]
            if name in self._metrics:
                del self._metrics[name]

    def start_health_check(self, interval: float = 30.0):
        def health_check_loop():
            while not self._stop_health_check.is_set():
                self._perform_health_check()
                self._stop_health_check.wait(interval)

        self._health_check_thread = threading.Thread(target=health_check_loop, daemon=True)
        self._health_check_thread.start()
        logger.info("Provider health check started")

    def stop_health_check(self):
        self._stop_health_check.set()
        if self._health_check_thread:
            self._health_check_thread.join(timeout=5)
        logger.info("Provider health check stopped")

    def _perform_health_check(self):
        with self._lock:
            providers = list(self._providers.values())

        for provider in providers:
            try:
                is_healthy = self._check_provider_health(provider)
                status = ProviderStatus.ACTIVE if is_healthy else ProviderStatus.UNHEALTHY
                self.update_provider_status(provider.name, status)
            except Exception as e:
                logger.warning(f"Health check failed for provider '{provider.name}': {e}")
                self.update_provider_status(provider.name, ProviderStatus.UNHEALTHY)

    def _check_provider_health(self, provider: LLMProvider) -> bool:
        try:
            redis_client = get_redis_client()
            if redis_client.is_healthy:
                latency_start = time.time()
                success = redis_client.ping()
                latency = time.time() - latency_start

                if success:
                    return True

            fallback_check = self._simple_health_check(provider)
            return fallback_check
        except Exception as e:
            logger.warning(f"Health check error for '{provider.name}': {e}")
            return False

    def _simple_health_check(self, provider: LLMProvider) -> bool:
        try:
            import httpx
            timeout = min(provider.timeout, 5.0)
            with httpx.Client(timeout=timeout) as client:
                response = client.get(provider.api_base, headers={"Authorization": f"Bearer {provider.api_key}"})
                return response.status_code in (200, 401, 403)
        except Exception:
            return True

    def calculate_weights(self) -> Dict[str, float]:
        with self._lock:
            providers = [
                (name, p, m)
                for name, p in self._providers.items()
                for m in [self._metrics.get(name)]
                if m and p.status == ProviderStatus.ACTIVE
            ]

        if not providers:
            return {}

        total_weight = 0.0
        for name, provider, metrics in providers:
            health_factor = metrics.successful_requests / max(metrics.total_requests, 1)
            latency_factor = 1.0 / max(provider.avg_latency, 0.1)
            weight = provider.weight * health_factor * latency_factor
            total_weight += weight

        if total_weight == 0:
            return {name: 1.0 / len(providers) for name, _, _ in providers}

        return {
            name: (provider.weight * health_factor * latency_factor) / total_weight
            for name, provider, metrics in providers
        }


def get_provider_manager() -> LLMProviderManager:
    return LLMProviderManager()


class LLMRouter:
    def __init__(self, config: Optional[RouterConfig] = None):
        self.config = config or RouterConfig()
        self._manager = get_provider_manager()
        self._circuit_breaker_registry = get_circuit_breaker_registry()
        self._rate_limiter_factory = get_rate_limiter_factory()
        self._lock = threading.RLock()
        self._round_robin_index: Dict[str, int] = {}

    def add_provider(
        self,
        name: str,
        api_base: str,
        api_key: str,
        model: str,
        weight: float = 1.0,
        cost_per_token: float = 0.001,
        avg_latency: float = 1.0,
        timeout: float = 30.0,
        max_tokens: int = 4096,
        priority: int = 0,
        tags: Optional[Dict[str, str]] = None
    ) -> LLMProvider:
        provider = self._manager.register_provider(
            name=name,
            api_base=api_base,
            api_key=api_key,
            model=model,
            weight=weight,
            cost_per_token=cost_per_token,
            avg_latency=avg_latency,
            timeout=timeout,
            max_tokens=max_tokens,
            priority=priority,
            tags=tags
        )

        self._circuit_breaker_registry.register(
            name=f"llm:{name}",
            failure_threshold=self.config.failure_count_threshold,
            success_threshold=self.config.recovery_count_threshold,
            timeout=60.0
        )

        self._rate_limiter_factory.create_limiter(
            key=f"llm:{name}",
            max_tokens=100,
            refill_rate=10,
            strategy=RateLimitStrategy.TOKEN_BUCKET
        )

        return provider

    def _select_provider_round_robin(self, exclude: Optional[List[str]] = None) -> Optional[LLMProvider]:
        exclude = exclude or []
        providers = [
            p for p in self._manager.get_active_providers()
            if p.name not in exclude
        ]

        if not providers:
            return None

        with self._lock:
            key = ",".join(sorted([p.name for p in providers]))
            if key not in self._round_robin_index:
                self._round_robin_index[key] = 0

            index = self._round_robin_index[key]
            selected = providers[index % len(providers)]
            self._round_robin_index[key] = (index + 1) % len(providers)

            return selected

    def _select_provider_weighted(self, exclude: Optional[List[str]] = None) -> Optional[LLMProvider]:
        exclude = exclude or []
        weights = self._manager.calculate_weights()

        providers = [
            p for p in self._manager.get_active_providers()
            if p.name not in exclude and p.name in weights
        ]

        if not providers:
            return None

        total = sum(weights.get(p.name, 0) for p in providers)
        if total == 0:
            return providers[0]

        random_value = random.uniform(0, total)
        cumulative = 0

        for provider in providers:
            cumulative += weights.get(provider.name, 0)
            if random_value <= cumulative:
                return provider

        return providers[-1]

    def _select_provider_failover(self, exclude: Optional[List[str]] = None) -> Optional[LLMProvider]:
        exclude = exclude or []
        providers = sorted(
            [p for p in self._manager.get_active_providers() if p.name not in exclude],
            key=lambda p: (-p.priority, p.avg_latency)
        )

        return providers[0] if providers else None

    def _select_provider_load_balance(self, exclude: Optional[List[str]] = None) -> Optional[LLMProvider]:
        weights = self._manager.calculate_weights()
        return self._select_provider_weighted(exclude)

    def _select_provider_cost_optimized(self, exclude: Optional[List[str]] = None) -> Optional[LLMProvider]:
        exclude = exclude or []
        providers = [
            p for p in self._manager.get_active_providers()
            if p.name not in exclude
        ]

        if not providers:
            return None

        providers.sort(key=lambda p: p.cost_per_token)
        return providers[0]

    def _select_provider_latency_optimized(self, exclude: Optional[List[str]] = None) -> Optional[LLMProvider]:
        exclude = exclude or []
        providers = [
            p for p in self._manager.get_active_providers()
            if p.name not in exclude
        ]

        if not providers:
            return None

        providers.sort(key=lambda p: p.avg_latency)
        return providers[0]

    def select_provider(self, exclude: Optional[List[str]] = None) -> Optional[LLMProvider]:
        strategies = {
            RouterStrategy.ROUND_ROBIN: self._select_provider_round_robin,
            RouterStrategy.WEIGHTED: self._select_provider_weighted,
            RouterStrategy.FAILOVER: self._select_provider_failover,
            RouterStrategy.LOAD_BALANCE: self._select_provider_load_balance,
            RouterStrategy.COST_OPTIMIZED: self._select_provider_cost_optimized,
            RouterStrategy.LATENCY_OPTIMIZED: self._select_provider_latency_optimized
        }

        strategy_func = strategies.get(self.config.strategy, self._select_provider_failover)
        return strategy_func(exclude)

    def _execute_with_circuit_breaker(
        self,
        provider: LLMProvider,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        circuit_breaker = self._circuit_breaker_registry.get(f"llm:{provider.name}")

        if not circuit_breaker:
            circuit_breaker = self._circuit_breaker_registry.register(
                name=f"llm:{provider.name}",
                failure_threshold=self.config.failure_count_threshold,
                success_threshold=self.config.recovery_count_threshold,
                timeout=60.0
            )

        def circuit_breaker_fallback(*f_args, **f_kwargs):
            logger.warning(f"Circuit breaker open for provider '{provider.name}', using fallback")
            raise ProviderCircuitOpenError(f"Circuit breaker open for provider '{provider.name}'")

        circuit_breaker.fallback = circuit_breaker_fallback

        return circuit_breaker.call(func, *args, **kwargs)

    def _execute_with_rate_limit(
        self,
        provider: LLMProvider,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        limiter = self._rate_limiter_factory.get_limiter(f"llm:{provider.name}")

        if limiter:
            result = limiter.consume()
            if not result.allowed:
                raise RateLimitExceededError(
                    f"Rate limit exceeded for provider '{provider.name}'. Retry after {result.retry_after:.2f}s"
                )

        return func(*args, **kwargs)

    def route_and_execute(
        self,
        func: Callable,
        *args,
        provider_name: Optional[str] = None,
        **kwargs
    ) -> Any:
        last_error = None
        excluded_providers = []

        for attempt in range(self.config.max_retries):
            if provider_name:
                provider = self._manager.get_provider(provider_name)
                if not provider:
                    raise ProviderNotFoundError(f"Provider '{provider_name}' not found")
                providers = [provider]
            else:
                provider = self.select_provider(exclude=excluded_providers)
                providers = [provider] if provider else []

            if not providers and self.config.default_provider:
                provider = self._manager.get_provider(self.config.default_provider)
                providers = [provider] if provider else []

            if not providers:
                if self.config.enable_fallback and self._manager.get_provider("fallback"):
                    provider = self._manager.get_provider("fallback")
                    providers = [provider] if provider else []

            for provider in providers:
                try:
                    self._execute_with_rate_limit(provider, func, *args, **kwargs)

                    start_time = time.time()
                    result = self._execute_with_circuit_breaker(provider, func, *args, **kwargs)
                    latency = time.time() - start_time

                    self._manager.record_request(
                        name=provider.name,
                        success=True,
                        latency=latency
                    )

                    return result

                except ProviderCircuitOpenError as e:
                    logger.warning(f"Circuit breaker open for provider '{provider.name}': {e}")
                    excluded_providers.append(provider.name)
                    last_error = e
                    continue

                except RateLimitExceededError as e:
                    logger.warning(f"Rate limit exceeded for provider '{provider.name}': {e}")
                    excluded_providers.append(provider.name)
                    last_error = e
                    continue

                except Exception as e:
                    latency = time.time() - getattr(self, '_last_start_time', time.time())
                    self._manager.record_request(
                        name=provider.name,
                        success=False,
                        latency=latency
                    )
                    last_error = e

                    if attempt < self.config.max_retries - 1:
                        time.sleep(self.config.retry_delay * (attempt + 1))

                    continue

            break

        if last_error:
            raise RouterExecutionError(f"All providers failed: {last_error}")

        raise RouterExecutionError("No available providers")

    def get_router_stats(self) -> Dict[str, Any]:
        return {
            "config": {
                "strategy": self.config.strategy.value,
                "health_check_interval": self.config.health_check_interval,
                "failure_count_threshold": self.config.failure_count_threshold,
                "max_retries": self.config.max_retries
            },
            "providers": self._manager.get_all_stats()
        }


class ProviderNotFoundError(Exception):
    pass


class ProviderCircuitOpenError(Exception):
    pass


class RateLimitExceededError(Exception):
    pass


class RouterExecutionError(Exception):
    pass


def get_llm_router(config: Optional[RouterConfig] = None) -> LLMRouter:
    return LLMRouter(config)


def create_default_router() -> LLMRouter:
    config = RouterConfig(
        strategy=RouterStrategy.FAILOVER,
        health_check_interval=30.0,
        failure_count_threshold=3,
        recovery_count_threshold=2,
        max_retries=3,
        retry_delay=1.0,
        enable_fallback=True
    )
    return LLMRouter(config)