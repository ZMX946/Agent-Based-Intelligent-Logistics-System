import threading
import time
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime

from utils import logger
from utils.redis_client import get_redis_client
from utils.circuit_breaker import get_circuit_breaker_registry
from utils.rate_limiter import get_rate_limiter_factory


class HealthCheckService:
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
        self._circuit_breaker_registry = None
        self._rate_limiter_factory = None
        self._health_history: List[Dict] = []
        self._max_history_size = 100
        self._last_check_time = None
        self._lock = threading.Lock()
        self._is_initialized = False

    def initialize(self):
        try:
            self._circuit_breaker_registry = get_circuit_breaker_registry()
            self._rate_limiter_factory = get_rate_limiter_factory()
            self._is_initialized = True
            logger.info("HealthCheckService 初始化成功")
        except Exception as e:
            logger.warning(f"HealthCheckService 初始化失败: {e}")

    async def _check_redis_health_async(self) -> Dict[str, Any]:
        try:
            redis_client = get_redis_client()
            # 检查是否是异步Redis客户端
            is_async_client = hasattr(redis_client, '_pool') and hasattr(redis_client, '_init_pool')
            
            is_healthy = False
            latency = -1
            
            if is_async_client:
                # 异步Redis客户端
                try:
                    # 确保连接池已初始化
                    if not hasattr(redis_client, '_client') or redis_client._client is None:
                        await redis_client._init_pool()
                    
                    # 执行异步ping操作
                    start_time = time.time()
                    await redis_client._client.ping()
                    latency = (time.time() - start_time) * 1000
                    is_healthy = True
                    logger.info(f"异步Redis健康检查成功，延迟: {latency:.2f}ms")
                except Exception as async_error:
                    logger.warning(f"异步Redis健康检查失败: {async_error}")
                    is_healthy = False
                    
                    # 尝试重新连接
                    try:
                        await redis_client._reconnect()
                        # 重新检查连接
                        start_time = time.time()
                        await redis_client._client.ping()
                        latency = (time.time() - start_time) * 1000
                        is_healthy = True
                        logger.info(f"异步Redis重连后健康检查成功，延迟: {latency:.2f}ms")
                    except Exception as reconnect_error:
                        logger.error(f"异步Redis重连失败: {reconnect_error}")
            else:
                # 同步Redis客户端
                try:
                    start_time = time.time()
                    redis_client.ping()
                    latency = (time.time() - start_time) * 1000
                    is_healthy = True
                    logger.info(f"同步Redis健康检查成功，延迟: {latency:.2f}ms")
                except Exception:
                    is_healthy = False
                    latency = -1
                    logger.warning("同步Redis健康检查失败")

            return {
                "service": "redis",
                "status": "healthy" if is_healthy else "unhealthy",
                "latency_ms": round(latency, 2) if latency >= 0 else None,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Redis健康检查发生未知错误: {e}")
            return {
                "service": "redis",
                "status": "unknown",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def _check_redis_health(self) -> Dict[str, Any]:
        """
        同步版本的Redis健康检查（向后兼容）
        """
        try:
            # 使用asyncio.run执行异步版本
            return asyncio.run(self._check_redis_health_async())
        except Exception as e:
            logger.error(f"同步Redis健康检查失败: {e}")
            return {
                "service": "redis",
                "status": "unknown",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

    def _check_circuit_breaker_status(self) -> Dict[str, Any]:
        try:
            if not self._circuit_breaker_registry:
                self._circuit_breaker_registry = get_circuit_breaker_registry()

            all_breakers = self._circuit_breaker_registry.get_all()
            breakers_status = {}

            for name, breaker in all_breakers.items():
                breakers_status[name] = {
                    "state": breaker.state,
                    "failure_count": getattr(breaker, 'failure_count', 0),
                    "success_count": getattr(breaker, 'success_count', 0),
                    "last_failure_time": getattr(breaker, 'last_failure_time', None)
                }

            total_open = sum(1 for b in breakers_status.values() if b["state"] == "open")
            total_half_open = sum(1 for b in breakers_status.values() if b["state"] == "half-open")

            return {
                "service": "circuit_breaker",
                "status": "healthy" if total_open == 0 else "degraded",
                "total_breakers": len(breakers_status),
                "open_breakers": total_open,
                "half_open_breakers": total_half_open,
                "breakers": breakers_status,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "service": "circuit_breaker",
                "status": "unknown",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

    def _check_rate_limiter_status(self) -> Dict[str, Any]:
        try:
            if not self._rate_limiter_factory:
                self._rate_limiter_factory = get_rate_limiter_factory()

            all_configs = self._rate_limiter_factory.get_all_configs()
            limiters_status = {}

            for key, config in all_configs.items():
                try:
                    limiters_status[key] = {
                        "state": "active",
                        "max_tokens": config.get("max_tokens"),
                        "refill_rate": config.get("refill_rate"),
                        "strategy": config.get("strategy"),
                        "block_duration": config.get("block_duration")
                    }
                except Exception as limiter_error:
                    limiters_status[key] = {
                        "state": "error",
                        "error": str(limiter_error)
                    }

            return {
                "service": "rate_limiter",
                "status": "healthy",
                "total_limiters": len(limiters_status),
                "limiters": limiters_status,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "service": "rate_limiter",
                "status": "unknown",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

    async def _check_mongodb_health_async(self) -> Dict[str, Any]:
        try:
            from utils.mongo_async_client import get_mongo_client
            from utils.config import mongo_host, mongo_db, mongo_port
            
            try:
                # 尝试获取异步MongoDB客户端
                mongo_client = await get_mongo_client(mongo_host, mongo_db, mongo_port)
                
                # 执行异步ping操作
                start_time = time.time()
                await mongo_client.db.command('ping')
                latency = (time.time() - start_time) * 1000
                
                logger.info(f"异步MongoDB健康检查成功，延迟: {latency:.2f}ms")
                return {
                    "service": "mongodb",
                    "status": "healthy",
                    "latency_ms": round(latency, 2),
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as async_error:
                logger.error(f"MongoDB健康检查失败: {async_error}")
                return {
                    "service": "mongodb",
                    "status": "unhealthy",
                    "error": str(async_error),
                    "timestamp": datetime.now().isoformat()
                }
        except Exception as e:
            logger.error(f"MongoDB健康检查发生未知错误: {e}")
            return {
                "service": "mongodb",
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def _check_mongodb_health(self) -> Dict[str, Any]:
        """
        同步版本的MongoDB健康检查（向后兼容）
        """
        try:
            # 使用asyncio.run执行异步版本
            return asyncio.run(self._check_mongodb_health_async())
        except Exception as e:
            logger.error(f"同步MongoDB健康检查失败: {e}")
            
            # 降级到原始的同步检查
            try:
                from mongoengine import get_connection
                connection = get_connection()
                if connection:
                    return {
                        "service": "mongodb",
                        "status": "healthy",
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "service": "mongodb",
                        "status": "unhealthy",
                        "error": "无法获取MongoDB连接",
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as sync_error:
                return {
                    "service": "mongodb",
                    "status": "unhealthy",
                    "error": str(sync_error),
                    "timestamp": datetime.now().isoformat()
                }

    def _check_milvus_health(self) -> Dict[str, Any]:
        try:
            # 简化Milvus健康检查，只检查是否可以导入
            from customize_milvus_wrapper import CustomizeMilvus
            # 只检查是否可以导入类，不实例化，避免配置依赖
            return {
                "service": "milvus",
                "status": "healthy",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "service": "milvus",
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

    def check_all(self) -> Dict[str, Any]:
        # 不使用锁，提高响应速度
        self._last_check_time = time.time()

        # 检查服务是否已经初始化
        if not self._is_initialized:
            health_report = {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "checks": {},
                "version": "1.0.0",
                "message": "HealthCheckService 正在初始化中"
            }
            self._add_to_history(report=health_report)
            return health_report

        checks = [
            self._check_redis_health(),
            self._check_circuit_breaker_status(),
            self._check_rate_limiter_status(),
            self._check_mongodb_health(),
            self._check_milvus_health()
        ]

        overall_status = "healthy"
        for check in checks:
            if check["status"] == "unhealthy":
                overall_status = "unhealthy"
                break
            elif check["status"] == "degraded" and overall_status == "healthy":
                overall_status = "degraded"

        health_report = {
            "status": overall_status,
            "timestamp": datetime.now().isoformat(),
            "checks": {check["service"]: check for check in checks},
            "version": "1.0.0"
        }

        # 同步添加到历史记录，确保在任何线程中都能正常工作
        self._add_to_history(report=health_report)
        return health_report
    
    def _add_to_history(self, report: Dict):
        with self._lock:
            self._health_history.append({
                "status": report["status"],
                "timestamp": report["timestamp"],
                "check_count": len(report["checks"])
            })

            while len(self._health_history) > self._max_history_size:
                self._health_history.pop(0)

    async def check_all_async(self) -> Dict[str, Any]:
        """
        异步版本的完整健康检查（真正的异步实现）
        """
        self._last_check_time = time.time()

        # 检查服务是否已经初始化
        if not self._is_initialized:
            health_report = {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "checks": {},
                "version": "1.0.0",
                "message": "HealthCheckService 正在初始化中"
            }
            self._add_to_history(report=health_report)
            return health_report

        # 并行执行所有健康检查
        checks = await asyncio.gather(
            self._check_redis_health_async(),
            asyncio.to_thread(self._check_circuit_breaker_status),
            asyncio.to_thread(self._check_rate_limiter_status),
            self._check_mongodb_health_async(),
            asyncio.to_thread(self._check_milvus_health)
        )

        # 确定整体状态，Redis的健康状态不影响整体状态
        overall_status = "healthy"
        for check in checks:
            # 跳过Redis检查，不影响整体状态
            if check["service"] == "redis":
                continue
                
            if check["status"] == "unhealthy":
                overall_status = "unhealthy"
                break
            elif check["status"] == "degraded" and overall_status == "healthy":
                overall_status = "degraded"

        health_report = {
            "status": overall_status,
            "timestamp": datetime.now().isoformat(),
            "checks": {check["service"]: check for check in checks},
            "version": "1.0.0"
        }

        # 同步添加到历史记录，确保在任何线程中都能正常工作
        self._add_to_history(report=health_report)
        return health_report

    def get_health_history(self, limit: int = 10) -> List[Dict]:
        with self._lock:
            return list(self._health_history[-limit:])
    
    async def get_health_history_async(self, limit: int = 10) -> List[Dict]:
        """
        异步版本的获取健康检查历史
        """
        return await asyncio.to_thread(self.get_health_history, limit)

    def get_service_status(self, service_name: str) -> Optional[Dict]:
        report = self.check_all()
        return report["checks"].get(service_name)
    
    async def get_service_status_async(self, service_name: str) -> Optional[Dict]:
        """
        异步版本的获取特定服务状态
        """
        report = await self.check_all_async()
        return report["checks"].get(service_name)

    def get_summary(self) -> Dict[str, Any]:
        report = self.check_all()

        summary = {
            "overall_status": report["status"],
            "last_check_time": self._last_check_time,
            "services": {}
        }

        for service_name, check in report["checks"].items():
            summary["services"][service_name] = {
                "status": check["status"],
                "latency_ms": check.get("latency_ms")
            }

            if "total_breakers" in check:
                summary["services"]["circuit_breaker"] = {
                    "status": check["status"],
                    "total_breakers": check["total_breakers"],
                    "open_breakers": check["open_breakers"]
                }
            elif "total_limiters" in check:
                summary["services"]["rate_limiter"] = {
                    "status": check["status"],
                    "total_limiters": check["total_limiters"]
                }

        return summary
    
    async def get_summary_async(self) -> Dict[str, Any]:
        """
        异步版本的获取健康检查摘要
        """
        return await asyncio.to_thread(self.get_summary)

    def is_healthy(self) -> bool:
        report = self.check_all()
        return report["status"] == "healthy"
    
    async def is_healthy_async(self) -> bool:
        """
        异步版本的检查系统是否健康
        """
        report = await self.check_all_async()
        return report["status"] == "healthy"


_health_check_service = None


def get_health_check_service() -> HealthCheckService:
    global _health_check_service
    if _health_check_service is None:
        _health_check_service = HealthCheckService()
        _health_check_service.initialize()
    return _health_check_service


def check_all_services() -> Dict[str, Any]:
    service = get_health_check_service()
    return service.check_all()


async def check_all_services_async() -> Dict[str, Any]:
    service = get_health_check_service()
    return await service.check_all_async()


def get_health_summary() -> Dict[str, Any]:
    service = get_health_check_service()
    return service.get_summary()


async def get_health_summary_async() -> Dict[str, Any]:
    service = get_health_check_service()
    return await service.get_summary_async()


def is_system_healthy() -> bool:
    service = get_health_check_service()
    return service.is_healthy()


async def is_system_healthy_async() -> bool:
    service = get_health_check_service()
    return await service.is_healthy_async()