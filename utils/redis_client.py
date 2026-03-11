# -*- coding: utf-8 -*-
"""
Redis 连接池管理器
提供高可用的 Redis 连接，支持连接池、健康检查、自动重连
"""

import redis
from redis.connection import ConnectionPool
from redis.exceptions import (
    RedisError
)
from typing import Optional, List, Dict
from datetime import datetime
import threading
import logging
import asyncio
import builtins

# Hack to fix aioredis TimeoutError issue in Python 3.14
# In Python 3.14+, asyncio.TimeoutError and builtins.TimeoutError are the same class
# This causes a "duplicate base class" error in aioredis.exceptions
if asyncio.TimeoutError is builtins.TimeoutError:
    # Create a dummy TimeoutError class for asyncio
    class _DummyTimeoutError(Exception):
        pass
    
    # Replace asyncio.TimeoutError temporarily
    asyncio.TimeoutError = _DummyTimeoutError

import aioredis
from utils.logger_config import logger
from utils.config import async_mode

logger = logging.getLogger(__name__)


class RedisClient:
    """
    Redis 客户端封装类
    提供连接池管理、自动重连、健康检查、读写分离等功能
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, host='localhost', port=6379, db=0, 
               max_connections=50, password=None, **kwargs):
        """
        单例模式确保全局只有一个连接池
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, host='localhost', port=6379, db=0,
                 max_connections=50, password=None, **kwargs):
        """
        初始化 Redis 连接池
        
        Args:
            host: Redis 服务器地址
            port: Redis 端口
            db: 数据库编号
            max_connections: 最大连接数
            password: 密码
            **kwargs: 其他 Redis 连接参数
        """
        if self._initialized:
            return
        
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        
        # 连接池配置
        self.max_connections = max_connections
        self.pool_config = {
            'host': host,
            'port': port,
            'db': db,
            'password': password,
            'max_connections': max_connections,
            'socket_timeout': 5.0,
            'socket_connect_timeout': 5.0,
            'retry_on_timeout': True,
            'decode_responses': True,
            **kwargs
        }
        
        # 创建连接池
        self._pool = ConnectionPool(**self.pool_config)
        
        # 创建 Redis 客户端
        self._client = redis.Redis(connection_pool=self._pool)
        
        # 健康检查相关
        self._last_health_check = None
        self._health_check_interval = 30
        self._is_healthy = False
        
        # 统计信息
        self._stats = {
            'total_connections': 0,
            'failed_connections': 0,
            'operations_success': 0,
            'operations_failed': 0
        }
        
        # 锁
        self._lock = threading.Lock()
        
        # 启动健康检查
        self._initialized = True
        self._start_health_check()
        
        # 初始化时进行一次同步健康检查
        self._health_check()
        
        logger.info(f"Redis 连接池初始化成功: {host}:{port}/{db}, 健康状态: {'健康' if self._is_healthy else '不健康'}")
    
    def _start_health_check(self):
        """启动后台健康检查线程"""
        def health_check_loop():
            while True:
                try:
                    self._health_check()
                except Exception as e:
                    logger.error(f"Redis 健康检查失败: {e}")
                finally:
                    import time
                    time.sleep(self._health_check_interval)
        
        thread = threading.Thread(target=health_check_loop, daemon=True, name="RedisHealthCheck")
        thread.start()
    
    def _health_check(self):
        """执行健康检查"""
        try:
            self._client.ping()
            self._last_health_check = datetime.now()
            self._is_healthy = True
        except RedisError as e:
            self._is_healthy = False
            logger.warning(f"Redis 健康检查失败: {e}")
            self._reconnect()
    
    def _reconnect(self):
        """重新连接 Redis"""
        logger.info("尝试重新连接 Redis...")
        try:
            self._pool.disconnect()
            self._pool = ConnectionPool(**self.pool_config)
            self._client = redis.Redis(connection_pool=self._pool)
            self._client.ping()
            self._is_healthy = True
            logger.info("Redis 重连成功")
        except Exception as e:
            self._is_healthy = False
            logger.error(f"Redis 重连失败: {e}")
    
    @property
    def client(self) -> redis.Redis:
        """获取 Redis 客户端实例"""
        return self._client
    
    @property
    def is_healthy(self) -> bool:
        """检查 Redis 是否健康"""
        return self._is_healthy
    
    @property
    def stats(self) -> Dict:
        """获取操作统计"""
        with self._lock:
            return self._stats.copy()
    
    def reset_stats(self):
        """重置统计信息"""
        with self._lock:
            self._stats = {
                'total_connections': 0,
                'failed_connections': 0,
                'operations_success': 0,
                'operations_failed': 0
            }
    
    def get(self, key: str) -> Optional[str]:
        """获取值"""
        try:
            result = self._client.get(key)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis GET 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """设置值
        
        Args:
            key: 键
            value: 值
            ex: 过期时间（秒）
        """
        try:
            result = self._client.set(key, value, ex=ex)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis SET 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def delete(self, *keys: str) -> int:
        """删除键"""
        try:
            result = self._client.delete(*keys)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis DELETE 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def exists(self, *keys: str) -> int:
        """检查键是否存在"""
        try:
            result = self._client.exists(*keys)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis EXISTS 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def incr(self, key: str) -> int:
        """原子自增"""
        try:
            result = self._client.incr(key)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis INCR 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def hset(self, name: str, *args, **kwargs) -> int:
        """设置哈希表字段
        
        支持多种调用方式：
        1. hset(name, key, value) - 设置单个字段
        2. hset(name, mapping) - 设置多个字段
        3. hset(name, field1, value1, field2, value2, ...) - 设置多个字段
        """
        try:
            if len(args) == 1 and isinstance(args[0], dict):
                # hset(name, mapping) - 设置多个字段
                result = self._client.hset(name, mapping=args[0])
            elif len(args) >= 2 and len(args) % 2 == 0:
                # hset(name, field1, value1, field2, value2, ...) - 设置多个字段
                # 对于redis-py 4.6.0，需要将多个字段值对转换为mapping字典
                mapping = {args[i]: args[i+1] for i in range(0, len(args), 2)}
                result = self._client.hset(name, mapping=mapping)
            elif len(args) == 2:
                # hset(name, key, value) - 设置单个字段
                result = self._client.hset(name, args[0], args[1])
            elif 'mapping' in kwargs:
                # hset(name, mapping=mapping) - 设置多个字段
                result = self._client.hset(name, mapping=kwargs['mapping'])
            elif 'key' in kwargs and 'value' in kwargs:
                # hset(name, key=key, value=value) - 设置单个字段
                result = self._client.hset(name, kwargs['key'], kwargs['value'])
            else:
                raise RedisError(f"Invalid arguments for hset: {args}, {kwargs}")
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis HSET 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def hget(self, name: str, key: str) -> Optional[str]:
        """获取哈希表字段"""
        try:
            result = self._client.hget(name, key)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis HGET 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def hgetall(self, name: str) -> Dict:
        """获取哈希表所有字段"""
        try:
            result = self._client.hgetall(name)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis HGETALL 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def lpush(self, name: str, *values: str) -> int:
        """从左侧插入列表"""
        try:
            result = self._client.lpush(name, *values)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis LPUSH 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def rpop(self, name: str) -> Optional[str]:
        """从右侧弹出列表"""
        try:
            result = self._client.rpop(name)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis RPOP 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def lrange(self, name: str, start: int, end: int) -> List[str]:
        """获取列表范围"""
        try:
            result = self._client.lrange(name, start, end)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis LRANGE 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def zadd(self, name: str, mapping: Dict[str, float]) -> int:
        """添加有序集合成员"""
        try:
            result = self._client.zadd(name, mapping)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis ZADD 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def zrangebyscore(self, name: str, min_: float, max_: float) -> List[str]:
        """按分数范围获取有序集合成员"""
        try:
            result = self._client.zrangebyscore(name, min_, max_)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis ZRANGEBYSCORE 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def expire(self, key: str, seconds: int) -> bool:
        """设置过期时间"""
        try:
            result = self._client.expire(key, seconds)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis EXPIRE 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def ttl(self, key: str) -> int:
        """获取剩余过期时间"""
        try:
            result = self._client.ttl(key)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except RedisError as e:
            logger.error(f"Redis TTL 失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    def close(self):
        """关闭连接池"""
        try:
            self._pool.disconnect()
            logger.info("Redis 连接池已关闭")
        except Exception as e:
            logger.error(f"关闭 Redis 连接池失败: {e}")
    
    def flush_db(self):
        """清除当前数据库中的所有数据"""
        try:
            result = self._client.flushdb()
            logger.info(f"Redis 数据库已清空: {result}")
            return result
        except RedisError as e:
            logger.error(f"Redis 清空数据库失败: {e}")
            raise
    
    def clear_all_keys(self, pattern: str = "*"):
        """清除匹配模式的所有键"""
        try:
            keys = self._client.keys(pattern)
            if keys:
                result = self._client.delete(*keys)
                logger.info(f"已清除 {result} 个 Redis 键")
                return result
            else:
                logger.info("没有找到匹配的 Redis 键")
                return 0
        except RedisError as e:
            logger.error(f"清除 Redis 键失败: {e}")
            raise


from utils.config import redis_host, redis_port, redis_db, redis_password, async_mode, async_redis_pool_size

def get_redis_client(host=None, port=None, db=None, 
                    max_connections=50, password=None, **kwargs):
    """
    获取 Redis 客户端实例的工厂函数
    根据async_mode配置返回同步或异步Redis客户端
    
    Args:
        host: Redis 服务器地址
        port: Redis 端口
        db: 数据库编号
        max_connections: 最大连接数
        password: 密码
        **kwargs: 其他参数
        
    Returns:
        RedisClient或AsyncRedisClient实例
    """
    # 使用配置文件中的参数，如果没有提供的话
    host = host or redis_host
    port = port or redis_port
    db = db or redis_db
    password = password or redis_password
    
    if async_mode:
        # 使用异步Redis客户端
        return AsyncRedisClient(
            host=host,
            port=port,
            db=db,
            max_connections=async_redis_pool_size,
            password=password,
            **kwargs
        )
    else:
        # 使用同步Redis客户端
        return RedisClient(
            host=host,
            port=port,
            db=db,
            max_connections=max_connections,
            password=password,
            **kwargs
        )


# Redis 键前缀常量
class RedisKeys:
    """Redis 键前缀常量类"""
    
    # 限流相关
    RATE_LIMIT_PREFIX = "ratelimit:"
    
    # 熔断器相关
    CIRCUIT_BREAKER_PREFIX = "circuit:"
    
    # 会话/认证相关
    SESSION_PREFIX = "session:"
    
    # 任务队列相关
    TASK_QUEUE_PREFIX = "taskqueue:"
    TASK_STATUS_PREFIX = "taskstatus:"
    
    # 工具缓存
    TOOL_CACHE_PREFIX = "toolcache:"
    
    # LLM 提供商状态
    LLM_PROVIDER_PREFIX = "llmprovider:"
    
    # 工具调用频率限制
    TOOL_RATE_PREFIX = "toolrate:"
    
    # 工具使用统计
    TOOL_STATS_PREFIX = "toolstats:"
    
    # 缓存前缀
    CACHE_PREFIX = "cache:"
    
    @classmethod
    def rate_limit_key(cls, identifier: str) -> str:
        return f"{cls.RATE_LIMIT_PREFIX}{identifier}"
    
    @classmethod
    def circuit_breaker_key(cls, name: str) -> str:
        return f"{cls.CIRCUIT_BREAKER_PREFIX}{name}"
    
    @classmethod
    def session_key(cls, token: str) -> str:
        return f"{cls.SESSION_PREFIX}{token}"
    
    @classmethod
    def task_queue_key(cls, queue_name: str) -> str:
        return f"{cls.TASK_QUEUE_PREFIX}{queue_name}"
    
    @classmethod
    def task_status_key(cls, task_id: str) -> str:
        return f"{cls.TASK_STATUS_PREFIX}{task_id}"
    
    @classmethod
    def tool_cache_key(cls, tool_id: int) -> str:
        return f"{cls.TOOL_CACHE_PREFIX}{tool_id}"
    
    @classmethod
    def llm_provider_key(cls, provider_name: str) -> str:
        return f"{cls.LLM_PROVIDER_PREFIX}{provider_name}"
    
    @classmethod
    def tool_rate_key(cls, tool_id: int, window: str) -> str:
        return f"{cls.TOOL_RATE_PREFIX}{tool_id}:{window}"
    
    @classmethod
    def tool_stats_key(cls, tool_id: int) -> str:
        return f"{cls.TOOL_STATS_PREFIX}{tool_id}"
    
    @classmethod
    def cache_key(cls, key: str) -> str:
        return f"{cls.CACHE_PREFIX}{key}"


class AsyncRedisClient:
    """
    异步Redis客户端封装类
    提供异步连接池管理、自动重连、健康检查等功能
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, host='localhost', port=6379, db=0, 
               max_connections=50, password=None, **kwargs):
        """
        单例模式确保全局只有一个连接池
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    # 初始化实例
                    cls._instance._initialize(host, port, db, max_connections, password, **kwargs)
        return cls._instance
    
    def _initialize(self, host='localhost', port=6379, db=0,
                   max_connections=50, password=None, **kwargs):
        """
        初始化异步Redis连接池
        
        Args:
            host: Redis服务器地址
            port: Redis端口
            db: 数据库编号
            max_connections: 最大连接数
            password: 密码
            **kwargs: 其他Redis连接参数
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.max_connections = max_connections
        
        # 连接池配置
        self.pool_config = {
            'host': host,
            'port': port,
            'db': db,
            'password': password,
            'max_connections': max_connections,
            'socket_timeout': 5.0,
            'socket_connect_timeout': 5.0,
            'retry_on_timeout': True,
            'decode_responses': True,
            **kwargs
        }
        
        # 健康检查相关
        self._last_health_check = None
        self._health_check_interval = 30
        self._is_healthy = False
        
        # 统计信息
        self._stats = {
            'total_connections': 0,
            'failed_connections': 0,
            'operations_success': 0,
            'operations_failed': 0
        }
        
        # 锁
        self._lock = threading.Lock()
        
        # 连接池和客户端
        self._pool = None
        self._client = None
        
        # 记录初始化日志
        logger.info(f"异步Redis连接池初始化配置: {host}:{port}/{db}")
    
    def __init__(self, host='localhost', port=6379, db=0,
                 max_connections=50, password=None, **kwargs):
        """
        初始化方法（保持向后兼容）
        """
        # 单例模式已经在__new__方法中初始化了实例，这里不需要重复初始化
        pass
    
    async def _init_pool(self):
        """初始化异步连接池"""
        if not self._pool:
            logger.info(f"正在初始化异步Redis连接池: host={self.host}, port={self.port}, db={self.db}")
            try:
                self._pool = aioredis.ConnectionPool(**self.pool_config)
                logger.info("异步Redis连接池创建成功")
                self._client = aioredis.Redis(connection_pool=self._pool)
                logger.info("异步Redis客户端创建成功")
                await self._health_check()
                logger.info("异步Redis健康检查完成")
            except Exception as e:
                logger.error(f"异步Redis连接池初始化失败: {e}")
                raise
    
    async def _start_health_check(self):
        """启动异步健康检查"""
        async def health_check_loop():
            while True:
                try:
                    await self._health_check()
                except Exception as e:
                    logger.error(f"异步Redis健康检查失败: {e}")
                finally:
                    await asyncio.sleep(self._health_check_interval)
        
        # 启动健康检查协程
        asyncio.create_task(health_check_loop(), name="AsyncRedisHealthCheck")
    
    async def _health_check(self):
        """执行异步健康检查"""
        if not self._client:
            await self._init_pool()
            
        try:
            await self._client.ping()
            self._last_health_check = datetime.now()
            self._is_healthy = True
        except Exception as e:
            self._is_healthy = False
            logger.warning(f"异步Redis健康检查失败: {e}")
            await self._reconnect()
    
    async def _reconnect(self):
        """异步重新连接Redis"""
        logger.info("尝试重新连接异步Redis...")
        try:
            if self._pool:
                await self._pool.disconnect()
            
            self._pool = aioredis.ConnectionPool(**self.pool_config)
            self._client = aioredis.Redis(connection_pool=self._pool)
            await self._client.ping()
            self._is_healthy = True
            logger.info("异步Redis重连成功")
        except Exception as e:
            self._is_healthy = False
            logger.error(f"异步Redis重连失败: {e}")
    
    @property
    def client(self):
        """获取Redis客户端实例"""
        return self._client
    
    @property
    def is_healthy(self) -> bool:
        """检查Redis是否健康"""
        return self._is_healthy
    
    @property
    def stats(self) -> Dict:
        """获取操作统计"""
        with self._lock:
            return self._stats.copy()
    
    def reset_stats(self):
        """重置统计信息"""
        with self._lock:
            self._stats = {
                'total_connections': 0,
                'failed_connections': 0,
                'operations_success': 0,
                'operations_failed': 0
            }
    
    async def get(self, key: str) -> Optional[str]:
        """异步获取值"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.get(key)
            with self._lock:
                self._stats['operations_success'] += 1
            return result  # 连接池已经配置了decode_responses=True，所以直接返回
        except Exception as e:
            logger.error(f"异步Redis GET失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """异步设置值"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.set(key, value, ex=ex)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except Exception as e:
            logger.error(f"异步Redis SET失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def delete(self, *keys: str) -> int:
        """异步删除键"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.delete(*keys)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except Exception as e:
            logger.error(f"异步Redis DELETE失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def exists(self, *keys: str) -> int:
        """异步检查键是否存在"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.exists(*keys)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except Exception as e:
            logger.error(f"异步Redis EXISTS失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def incr(self, key: str) -> int:
        """异步原子自增"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.incr(key)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except Exception as e:
            logger.error(f"异步Redis INCR失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def hset(self, name: str, *args, **kwargs) -> int:
        """异步设置哈希表字段
        
        支持多种调用方式：
        1. hset(name, key, value) - 设置单个字段
        2. hset(name, mapping) - 设置多个字段
        3. hset(name, field1, value1, field2, value2, ...) - 设置多个字段
        """
        if not self._client:
            await self._init_pool()
            
        try:
            if len(args) == 1 and isinstance(args[0], dict):
                # hset(name, mapping) - 设置多个字段
                result = await self._client.hset(name, args[0])
            elif len(args) >= 2 and len(args) % 2 == 0:
                # hset(name, field1, value1, field2, value2, ...) - 设置多个字段
                result = await self._client.hset(name, *args)
            elif len(args) == 2:
                # hset(name, key, value) - 设置单个字段
                result = await self._client.hset(name, args[0], args[1])
            elif 'mapping' in kwargs:
                # hset(name, mapping=mapping) - 设置多个字段
                result = await self._client.hset(name, kwargs['mapping'])
            elif 'key' in kwargs and 'value' in kwargs:
                # hset(name, key=key, value=value) - 设置单个字段
                result = await self._client.hset(name, kwargs['key'], kwargs['value'])
            else:
                raise Exception(f"Invalid arguments for hset: {args}, {kwargs}")
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except Exception as e:
            logger.error(f"异步Redis HSET失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def hget(self, name: str, key: str) -> Optional[str]:
        """异步获取哈希表字段"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.hget(name, key)
            with self._lock:
                self._stats['operations_success'] += 1
            return result.decode('utf-8') if result else None
        except Exception as e:
            logger.error(f"异步Redis HGET失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def hgetall(self, name: str) -> Dict:
        """异步获取哈希表所有字段"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.hgetall(name)
            with self._lock:
                self._stats['operations_success'] += 1
            return result  # 连接池已经配置了decode_responses=True，所以直接返回
        except Exception as e:
            logger.error(f"异步Redis HGETALL失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def lpush(self, name: str, *values: str) -> int:
        """异步从左侧插入列表"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.lpush(name, *values)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except Exception as e:
            logger.error(f"异步Redis LPUSH失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def rpop(self, name: str) -> Optional[str]:
        """异步从右侧弹出列表"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.rpop(name)
            with self._lock:
                self._stats['operations_success'] += 1
            return result.decode('utf-8') if result else None
        except Exception as e:
            logger.error(f"异步Redis RPOP失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def lrange(self, name: str, start: int, end: int) -> List[str]:
        """异步获取列表范围"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.lrange(name, start, end)
            with self._lock:
                self._stats['operations_success'] += 1
            return result  # 连接池已经配置了decode_responses=True，所以直接返回
        except Exception as e:
            logger.error(f"异步Redis LRANGE失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def zadd(self, name: str, mapping: Dict[str, float]) -> int:
        """异步添加有序集合成员"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.zadd(name, mapping)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except Exception as e:
            logger.error(f"异步Redis ZADD失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def zrangebyscore(self, name: str, min_: float, max_: float) -> List[str]:
        """异步按分数范围获取有序集合成员"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.zrangebyscore(name, min_, max_)
            with self._lock:
                self._stats['operations_success'] += 1
            return result  # 连接池已经配置了decode_responses=True，所以直接返回
        except Exception as e:
            logger.error(f"异步Redis ZRANGEBYSCORE失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def pipeline(self):
        """获取异步Redis管道"""
        if not self._client:
            await self._init_pool()
            
        return self._client.pipeline()
    
    async def expire(self, key: str, seconds: int) -> bool:
        """异步设置过期时间"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.expire(key, seconds)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except Exception as e:
            logger.error(f"异步Redis EXPIRE失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def ttl(self, key: str) -> int:
        """异步获取剩余过期时间"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.ttl(key)
            with self._lock:
                self._stats['operations_success'] += 1
            return result
        except Exception as e:
            logger.error(f"异步Redis TTL失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def keys(self, pattern: str) -> List[str]:
        """异步获取匹配的键"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.keys(pattern)
            with self._lock:
                self._stats['operations_success'] += 1
            return result  # 连接池已经配置了decode_responses=True，所以直接返回
        except Exception as e:
            logger.error(f"异步Redis KEYS失败: {e}")
            with self._lock:
                self._stats['operations_failed'] += 1
            raise
    
    async def close(self):
        """异步关闭连接池"""
        try:
            if self._pool:
                await self._pool.disconnect()
            logger.info("异步Redis连接池已关闭")
        except Exception as e:
            logger.error(f"关闭异步Redis连接池失败: {e}")
    
    async def flush_db(self):
        """异步清除当前数据库中的所有数据"""
        if not self._client:
            await self._init_pool()
            
        try:
            result = await self._client.flushdb()
            logger.info(f"异步Redis数据库已清空: {result}")
            return result
        except Exception as e:
            logger.error(f"异步Redis清空数据库失败: {e}")
            raise
    
    async def clear_all_keys(self, pattern: str = "*"):
        """异步清除匹配模式的所有键"""
        if not self._client:
            await self._init_pool()
            
        try:
            keys = await self._client.keys(pattern)
            if keys:
                result = await self._client.delete(*keys)
                logger.info(f"已清除 {result} 个异步Redis键")
                return result
            else:
                logger.info("没有找到匹配的异步Redis键")
                return 0
        except Exception as e:
            logger.error(f"清除异步Redis键失败: {e}")
            raise


# 限流器实现
class RedisRateLimiter:
    """
    基于 Redis 的分布式限流器
    支持滑动窗口、令牌桶等多种限流算法
    """
    
    def __init__(self, redis_client: RedisClient):
        """
        初始化限流器
        
        Args:
            redis_client: Redis 客户端实例
        """
        self.redis = redis_client
    
    def sliding_window(self, key: str, max_requests: int, window_seconds: int) -> tuple:
        """
        滑动窗口限流
        
        Args:
            key: 限流标识
            max_requests: 窗口期内最大请求数
            window_seconds: 窗口期（秒）
            
        Returns:
            (is_allowed, remaining, reset_time)
            is_allowed: 是否允许请求
            remaining: 剩余请求数
            reset_time: 窗口重置时间戳
        """
        now = datetime.now().timestamp()
        window_start = now - window_seconds
        
        pipe = self.redis.client.pipeline()
        
        # 删除窗口外的数据
        pipe.zremrangebyscore(key, 0, window_start)
        
        # 统计当前窗口内的请求数
        pipe.zcard(key)
        
        # 添加当前请求
        pipe.zadd(key, {f"{now}": now})
        
        # 设置过期时间（防止数据堆积）
        pipe.expire(key, window_seconds + 1)
        
        results = pipe.execute()
        
        current_count = results[1]
        remaining = max(0, max_requests - current_count - 1)
        reset_time = int(now + window_seconds)
        
        if current_count < max_requests:
            return True, remaining, reset_time
        else:
            return False, remaining, reset_time
    
    def token_bucket(self, key: str, rate: int, capacity: int) -> bool:
        """
        令牌桶限流
        
        Args:
            key: 限流标识
            rate: 令牌生成速率（令牌/秒）
            capacity: 桶容量
            
        Returns:
            是否获取到令牌
        """
        now = datetime.now().timestamp()
        
        pipe = self.redis.client.pipeline()
        
        # 获取上次更新时间戳和桶中令牌数
        pipe.hgetall(f"{key}:bucket")
        pipe.get(f"{key}:last_update")
        
        results = pipe.execute()
        bucket_data = results[0] or {}
        last_update = float(results[1] or now)
        
        # 计算应该增加的令牌数
        tokens_to_add = (now - last_update) * rate
        current_tokens = float(bucket_data.get('tokens', capacity))
        current_tokens = min(capacity, current_tokens + tokens_to_add)
        
        # 尝试获取令牌
        if current_tokens >= 1:
            current_tokens -= 1
            pipe.hset(f"{key}:bucket", 'tokens', str(current_tokens))
            pipe.set(f"{key}:last_update", now)
            pipe.execute()
            return True
        else:
            # 更新桶状态（不消耗令牌）
            pipe.hset(f"{key}:bucket", 'tokens', str(current_tokens))
            pipe.set(f"{key}:last_update", now)
            pipe.execute()
            return False
    
    def fixed_window(self, key: str, max_requests: int, window_seconds: int) -> tuple:
        """
        固定窗口限流
        
        Args:
            key: 限流标识
            max_requests: 窗口期内最大请求数
            window_seconds: 窗口期（秒）
            
        Returns:
            (is_allowed, remaining, reset_time)
        """
        now = datetime.now().timestamp()
        window_key = f"{key}:{int(now // window_seconds) * window_seconds}"
        
        pipe = self.redis.client.pipeline()
        pipe.incr(window_key)
        pipe.ttl(window_key)
        
        results = pipe.execute()
        current_count = results[0]
        ttl = results[1]
        
        if ttl == -1:
            self.redis.expire(window_key, window_seconds)
        
        remaining = max(0, max_requests - current_count)
        reset_time = int(now // window_seconds + 1) * window_seconds
        
        if current_count <= max_requests:
            return True, remaining, reset_time
        else:
            return False, remaining, reset_time


# 分布式锁实现
class RedisDistributedLock:
    """
    基于 Redis 的分布式锁实现
    支持自动过期、可重入等特性
    """
    
    def __init__(self, redis_client: RedisClient):
        """
        初始化分布式锁
        
        Args:
            redis_client: Redis 客户端实例
        """
        self.redis = redis_client
    
    async def acquire(self, key: str, expire: int = 30, timeout: int = 10) -> bool:
        """
        获取分布式锁
        
        Args:
            key: 锁的唯一标识
            expire: 锁的过期时间（秒）
            timeout: 获取锁的超时时间（秒）
            
        Returns:
            是否成功获取锁
        """
        lock_key = f"lock:{key}"
        start_time = datetime.now().timestamp()
        
        while datetime.now().timestamp() - start_time < timeout:
            try:
                # 使用 SETNX 命令尝试获取锁
                # 同时设置过期时间，防止死锁
                result = False
                if hasattr(self.redis, '_client') and hasattr(self.redis._client, 'set'):
                    # 检查是否是异步Redis客户端
                    if asyncio.iscoroutinefunction(self.redis._client.set):
                        result = await self.redis._client.set(lock_key, "1", nx=True, ex=expire)
                    else:
                        result = self.redis._client.set(lock_key, "1", nx=True, ex=expire)
                else:
                    # 兼容旧版本
                    if hasattr(self.redis, 'client') and hasattr(self.redis.client, 'set'):
                        result = self.redis.client.set(lock_key, "1", nx=True, ex=expire)
                if result:
                    return True
                
                # 短暂休眠后重试
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"获取分布式锁失败: {e}")
                await asyncio.sleep(0.1)
        
        return False
    
    async def release(self, key: str) -> bool:
        """
        释放分布式锁
        
        Args:
            key: 锁的唯一标识
            
        Returns:
            是否成功释放锁
        """
        lock_key = f"lock:{key}"
        try:
            result = 0
            if hasattr(self.redis, '_client') and hasattr(self.redis._client, 'delete'):
                # 检查是否是异步Redis客户端
                if asyncio.iscoroutinefunction(self.redis._client.delete):
                    result = await self.redis._client.delete(lock_key)
                else:
                    result = self.redis._client.delete(lock_key)
            else:
                # 兼容旧版本
                if hasattr(self.redis, 'client') and hasattr(self.redis.client, 'delete'):
                    result = self.redis.client.delete(lock_key)
            # 确保result是整数类型
            if isinstance(result, int):
                return result > 0
            else:
                logger.error(f"释放分布式锁返回值类型错误: {type(result)}")
                return False
        except Exception as e:
            logger.error(f"释放分布式锁失败: {e}")
            return False
    
    async def is_locked(self, key: str) -> bool:
        """
        检查锁是否存在
        
        Args:
            key: 锁的唯一标识
            
        Returns:
            锁是否存在
        """
        lock_key = f"lock:{key}"
        try:
            result = 0
            if hasattr(self.redis, '_client') and hasattr(self.redis._client, 'exists'):
                # 检查是否是异步Redis客户端
                if asyncio.iscoroutinefunction(self.redis._client.exists):
                    result = await self.redis._client.exists(lock_key)
                else:
                    result = self.redis._client.exists(lock_key)
            else:
                # 兼容旧版本
                if hasattr(self.redis, 'client') and hasattr(self.redis.client, 'exists'):
                    result = self.redis.client.exists(lock_key)
            return result > 0
        except Exception as e:
            logger.error(f"检查分布式锁状态失败: {e}")
            return False


# 幂等性处理器
class RedisIdempotentProcessor:
    """
    基于 Redis 的幂等性处理器
    用于防止重复请求导致的副作用
    """
    
    def __init__(self, redis_client: RedisClient):
        """
        初始化幂等性处理器
        
        Args:
            redis_client: Redis 客户端实例
        """
        self.redis = redis_client
    
    async def process(self, key: str, expire: int = 3600) -> bool:
        """
        处理幂等性请求
        
        Args:
            key: 幂等性键（如 requestId）
            expire: 键的过期时间（秒）
            
        Returns:
            是否是首次请求
        """
        idempotent_key = f"idempotent:{key}"
        try:
            # 使用 SETNX 命令检查是否已处理过
            if hasattr(self.redis, '_client') and hasattr(self.redis._client, 'set'):
                # 检查是否是异步Redis客户端
                if asyncio.iscoroutinefunction(self.redis._client.set):
                    result = await self.redis._client.set(idempotent_key, "1", nx=True, ex=expire)
                else:
                    result = self.redis._client.set(idempotent_key, "1", nx=True, ex=expire)
            else:
                # 兼容旧版本
                result = self.redis.client.set(idempotent_key, "1", nx=True, ex=expire)
            return result is not None
        except Exception as e:
            logger.error(f"处理幂等性请求失败: {e}")
            # 出错时默认允许请求，避免影响正常业务
            return True
    
    async def exists(self, key: str) -> bool:
        """
        检查幂等性键是否存在
        
        Args:
            key: 幂等性键
            
        Returns:
            键是否存在
        """
        idempotent_key = f"idempotent:{key}"
        try:
            if hasattr(self.redis, '_client') and hasattr(self.redis._client, 'exists'):
                # 检查是否是异步Redis客户端
                if asyncio.iscoroutinefunction(self.redis._client.exists):
                    result = await self.redis._client.exists(idempotent_key)
                else:
                    result = self.redis._client.exists(idempotent_key)
            else:
                # 兼容旧版本
                result = self.redis.client.exists(idempotent_key)
            return result > 0
        except Exception as e:
            logger.error(f"检查幂等性键失败: {e}")
            return False
    
    async def remove(self, key: str) -> bool:
        """
        移除幂等性键
        
        Args:
            key: 幂等性键
            
        Returns:
            是否成功移除
        """
        idempotent_key = f"idempotent:{key}"
        try:
            if hasattr(self.redis, '_client') and hasattr(self.redis._client, 'delete'):
                # 检查是否是异步Redis客户端
                if asyncio.iscoroutinefunction(self.redis._client.delete):
                    result = await self.redis._client.delete(idempotent_key)
                else:
                    result = self.redis._client.delete(idempotent_key)
            else:
                # 兼容旧版本
                result = self.redis.client.delete(idempotent_key)
            return result > 0
        except Exception as e:
            logger.error(f"移除幂等性键失败: {e}")
            return False


# 熔断器状态存储
class RedisCircuitBreakerStore:
    """
    基于 Redis 的熔断器状态存储
    支持分布式环境下的熔断器状态同步
    """
    
    def __init__( redis_client: RedisClient):
        """
        初始化熔断器存储
        
        Args:
            redis_client: Redis 客户端实例
        """
        self.redis = redis_client
    
    def get_state(self, breaker_name: str) -> Dict:
        """获取熔断器状态"""
        data = self.redis.hgetall(f"circuit:{breaker_name}")
        if not data:
            return {
                'state': 'closed',  # closed, open, half_open
                'failure_count': 0,
                'last_failure_time': None,
                'success_count': 0,
                'last_success_time': None
            }
        return {
            'state': data.get('state', 'closed'),
            'failure_count': int(data.get('failure_count', 0)),
            'last_failure_time': data.get('last_failure_time'),
            'success_count': int(data.get('success_count', 0)),
            'last_success_time': data.get('last_success_time')
        }
    
    def set_state(self, breaker_name: str, state: str, 
                  failure_count: int = 0, success_count: int = 0):
        """设置熔断器状态"""
        now = datetime.now().isoformat()
        data = {
            'state': state,
            'failure_count': str(failure_count),
            'success_count': str(success_count),
            'last_update': now
        }
        
        if state == 'open':
            data['last_failure_time'] = now
        elif state == 'closed':
            data['last_success_time'] = now
        
        self.redis.hset(f"circuit:{breaker_name}", data)
        
        # 设置过期时间，防止僵尸数据
        self.redis.expire(f"circuit:{breaker_name}", 86400 * 7)  # 7天
    
    def record_failure(self, breaker_name: str, max_failures: int, 
                      reset_timeout: int) -> str:
        """
        记录失败，返回熔断器新状态
        
        Returns:
            新状态: 'closed' 或 'open'
        """
        state = self.get_state(breaker_name)
        state['failure_count'] += 1
        state['last_failure_time'] = datetime.now().isoformat()
        
        if state['failure_count'] >= max_failures:
            new_state = 'open'
            # 设置熔断恢复时间
            self.redis.set(f"circuit:{breaker_name}:retry_after", 
                          datetime.now().timestamp() + reset_timeout)
        else:
            new_state = 'closed'
        
        self.set_state(breaker_name, new_state, 
                      state['failure_count'], state['success_count'])
        return new_state
    
    def record_success(self, breaker_name: str) -> str:
        """
        记录成功，返回熔断器新状态
        
        Returns:
            新状态: 'closed' 或 'half_open'
        """
        state = self.get_state(breaker_name)
        state['success_count'] += 1
        state['last_success_time'] = datetime.now().isoformat()
        
        new_state = 'closed'
        self.set_state(breaker_name, new_state, 
                      state['failure_count'], state['success_count'])
        return new_state
    
    def can_retry(self, breaker_name: str) -> bool:
        """检查是否可以重试（熔断器是否应该进入半开状态）"""
        retry_after = self.redis.get(f"circuit:{breaker_name}:retry_after")
        if retry_after is None:
            return True
        return datetime.now().timestamp() > float(retry_after)
    
    def half_open(self, breaker_name: str) -> bool:
        """将熔断器置于半开状态"""
        retry_after = self.redis.get(f"circuit:{breaker_name}:retry_after")
        if retry_after and datetime.now().timestamp() <= float(retry_after):
            return False
        
        state = self.get_state(breaker_name)
        if state['state'] == 'open' and self.can_retry(breaker_name):
            self.set_state(breaker_name, 'half_open', 
                          state['failure_count'], state['success_count'])
            return True
        return False


# 任务队列实现
class RedisTaskQueue:
    """
    基于 Redis 的任务队列
    支持优先级、延迟任务、任务持久化
    """
    
    def __init__(self, redis_client: RedisClient, queue_name: str):
        """
        初始化任务队列
        
        Args:
            redis_client: Redis 客户端实例
            queue_name: 队列名称
        """
        self.redis = redis_client
        self.queue_name = queue_name
        self.pending_key = f"taskqueue:{queue_name}:pending"
        self.processing_key = f"taskqueue:{queue_name}:processing"
        self.delayed_key = f"taskqueue:{queue_name}:delayed"
    
    def enqueue(self, task_data: Dict, priority: int = 0, 
                delay_seconds: int = 0) -> str:
        """
        入队任务
        
        Args:
            task_data: 任务数据
            priority: 优先级（越大优先级越高）
            delay_seconds: 延迟执行时间（秒）
            
        Returns:
            任务 ID
        """
        import json
        import uuid
        
        task_id = str(uuid.uuid4())
        
        task = {
            'task_id': task_id,
            'data': json.dumps(task_data),
            'priority': priority,
            'created_at': datetime.now().isoformat(),
            'status': 'pending'
        }
        
        now = datetime.now().timestamp()
        
        if delay_seconds > 0:
            # 延迟任务，使用有序集合
            score = now + delay_seconds
            self.redis.zadd(self.delayed_key, {json.dumps(task): score})
        else:
            # 优先级队列，使用有序集合
            self.redis.zadd(self.pending_key, {json.dumps(task): -priority})
        
        return task_id
    
    def dequeue(self, count: int = 1) -> List[Dict]:
        """
        出队任务
        
        Args:
            count: 出队数量
            
        Returns:
            任务列表
        """
        import json
        
        tasks = self.redis.zrangebyscore(self.pending_key, '-inf', '+inf', 0, count)
        
        if not tasks:
            # 检查延迟队列
            now = datetime.now().timestamp()
            delayed_tasks = self.redis.zrangebyscore(self.delayed_key, '-inf', now, 0, count)
            
            for task_json in delayed_tasks:
                task = json.loads(task_json)
                self.redis.zrem(self.delayed_key, task_json)
                self.redis.zadd(self.pending_key, {task_json: -task['priority']})
            
            tasks = self.redis.zrangebyscore(self.pending_key, '-inf', '+inf', 0, count)
        
        if not tasks:
            return []
        
        # 移动到处理中队列
        for task_json in tasks:
            task = json.loads(task_json)
            task['status'] = 'processing'
            task['started_at'] = datetime.now().isoformat()
            
            self.redis.zrem(self.pending_key, task_json)
            self.redis.zadd(self.processing_key, 
                           {json.dumps(task): datetime.now().timestamp()})
        
        return [json.loads(t) for t in tasks]
    
    def complete(self, task_id: str):
        """完成任务"""
        import json
        
        # 从处理中队列移除
        tasks = self.redis.zrangebyscore(self.processing_key, '-inf', '+inf')
        
        for task_json in tasks:
            task = json.loads(task_json)
            if task.get('task_id') == task_id:
                self.redis.zrem(self.processing_key, task_json)
                break
    
    def fail(self, task_id: str, error: str):
        """任务失败"""
        import json
        
        # 从处理中队列移除
        tasks = self.redis.zrangebyscore(self.processing_key, '-inf', '+inf')
        
        for task_json in tasks:
            task = json.loads(task_json)
            if task.get('task_id') == task_id:
                task['status'] = 'failed'
                task['error'] = error
                task['failed_at'] = datetime.now().isoformat()
                
                # 移回等待队列（可重试）
                self.redis.zrem(self.processing_key, task_json)
                self.redis.zadd(self.pending_key, {task_json: -task['priority']})
                break
    
    def get_stats(self) -> Dict:
        """获取队列统计"""
        pending_count = self.redis.zcard(self.pending_key)
        processing_count = self.redis.zcard(self.processing_key)
        delayed_count = self.redis.zcard(self.delayed_key)
        
        return {
            'pending': pending_count,
            'processing': processing_count,
            'delayed': delayed_count,
            'total': pending_count + processing_count + delayed_count
        }
    
    def clear(self):
        """清空队列"""
        self.redis.delete(self.pending_key, self.processing_key, self.delayed_key)


# 分布式锁
class RedisLock:
    """
    基于 Redis 的分布式锁
    """
    
    def __init__(self, redis_client: RedisClient):
        """
        初始化分布式锁
        
        Args:
            redis_client: Redis 客户端实例
        """
        self.redis = redis_client
    
    def acquire(self, lock_name: str, timeout: int = 10, 
                retry_interval: float = 0.1) -> bool:
        """
        获取锁
        
        Args:
            lock_name: 锁名称
            timeout: 锁过期时间（秒）
            retry_interval: 重试间隔
            
        Returns:
            是否获取成功
        """
        import uuid
        import time
        
        lock_value = str(uuid.uuid4())
        lock_key = f"lock:{lock_name}"
        
        while True:
            # 使用 SET NX PX 原子操作
            result = self.redis.client.set(
                lock_key, lock_value, nx=True, px=timeout * 1000
            )
            
            if result:
                return lock_value
            
            time.sleep(retry_interval)
            
            # 防止无限循环
            if retry_interval > 5:
                break
        
        return False
    
    def release(self, lock_name: str, lock_value: str) -> bool:
        """
        释放锁
        
        Args:
            lock_name: 锁名称
            lock_value: 锁值（获取锁时返回的值）
            
        Returns:
            是否释放成功
        """
        import redis
        
        lock_key = f"lock:{lock_name}"
        
        # 使用 Lua 脚本原子释放锁
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        
        try:
            result = self.redis.client.eval(script, 1, lock_key, lock_value)
            return bool(result)
        except redis.RedisError:
            return False
    
    def extend(self, lock_name: str, lock_value: str, 
               timeout: int = 10) -> bool:
        """
        延长锁时间
        
        Args:
            lock_name: 锁名称
            lock_value: 锁值
            timeout: 新的过期时间
            
        Returns:
            是否延长成功
        """
        import redis
        
        lock_key = f"lock:{lock_name}"
        
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("pexpire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        
        try:
            result = self.redis.client.eval(
                script, 1, lock_key, lock_value, timeout * 1000
            )
            return bool(result)
        except redis.RedisError:
            return False


# 缓存管理器
class CacheManager:
    """
    基于 Redis 的缓存管理器
    支持缓存过期、缓存穿透防护、缓存击穿防护
    """
    
    def __init__(self, redis_client: RedisClient, prefix: str = "cache:"):
        """
        初始化缓存管理器
        
        Args:
            redis_client: Redis 客户端实例
            prefix: 键前缀
        """
        self.redis = redis_client
        self.prefix = prefix
    
    def get(self, key: str) -> Optional[str]:
        """获取缓存"""
        return self.redis.get(f"{self.prefix}{key}")
    
    def set(self, key: str, value: str, ttl: int = 300) -> bool:
        """设置缓存"""
        return self.redis.set(f"{self.prefix}{key}", value, ex=ttl)
    
    def delete(self, key: str) -> int:
        """删除缓存"""
        return self.redis.delete(f"{self.prefix}{key}")
    
    def get_or_set(self, key: str, fetch_func, ttl: int = 300) -> str:
        """
        获取缓存，如果不存在则调用 fetch_func 获取并缓存
        
        Args:
            key: 缓存键
            fetch_func: 获取数据的函数
            ttl: 过期时间
            
        Returns:
            缓存数据
        """
        value = self.get(key)
        if value is not None:
            return value
        
        # 使用锁防止缓存击穿
        lock = RedisLock(self.redis)
        lock_key = f"cachelock:{key}"
        lock_value = lock.acquire(lock_key, timeout=5)
        
        try:
            if lock_value:
                # 双重检查
                value = self.get(key)
                if value is not None:
                    return value
                
                # 获取数据并缓存
                value = fetch_func()
                self.set(key, value, ttl)
                return value
            else:
                # 等待锁释放后重新获取
                import time
                time.sleep(0.1)
                return self.get_or_set(key, fetch_func, ttl)
        finally:
            if lock_value:
                lock.release(lock_key, lock_value)
    
    def invalidate_pattern(self, pattern: str):
        """
        按模式批量删除缓存
        
        Args:
            pattern: 匹配模式，如 "tool:*"
        """
        keys = self.redis.client.keys(f"{self.prefix}{pattern}")
        if keys:
            self.redis.client.delete(*keys)
    
    def clear_all(self):
        """清空所有缓存"""
        keys = self.redis.client.keys(f"{self.prefix}*")
        if keys:
            self.redis.client.delete(*keys)


# 分布式锁实现
class DistributedLock:
    """
    基于 Redis 的分布式锁实现
    支持锁获取、释放、续期等功能
    """
    
    _lock_release_script = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """
    
    def __init__(self, redis_client: RedisClient):
        """
        初始化分布式锁
        
        Args:
            redis_client: Redis 客户端实例
        """
        self.redis = redis_client
        self._client = redis_client.client
        self._script_registered = False
    
    def _register_scripts(self):
        """注册 Lua 脚本"""
        if not self._script_registered:
            self._release_script_sha = self._client.script_load(self._lock_release_script)
            self._script_registered = True
    
    def acquire(self, lock_name: str, timeout: int = 10, 
                blocking: bool = False, blocking_timeout: int = 5) -> Optional[str]:
        """
        获取分布式锁
        
        Args:
            lock_name: 锁名称
            timeout: 锁过期时间（秒）
            blocking: 是否阻塞等待
            blocking_timeout: 阻塞超时时间（秒）
            
        Returns:
            锁标识（成功时），None（失败时）
        """
        import uuid
        lock_value = str(uuid.uuid4())
        
        if blocking:
            import time
            start_time = time.time()
            while time.time() - start_time < blocking_timeout:
                if self._client.set(
                    f"lock:{lock_name}",
                    lock_value,
                    nx=True,
                    ex=timeout
                ):
                    return lock_value
                time.sleep(0.1)
            return None
        else:
            if self._client.set(
                f"lock:{lock_name}",
                lock_value,
                nx=True,
                ex=timeout
            ):
                return lock_value
            return None
    
    def release(self, lock_name: str, lock_value: str) -> bool:
        """
        释放分布式锁
        
        Args:
            lock_name: 锁名称
            lock_value: 锁标识（获取锁时返回的值）
            
        Returns:
            是否释放成功
        """
        try:
            self._register_scripts()
            result = self._client.evalsha(
                self._release_script_sha,
                1,
                f"lock:{lock_name}",
                lock_value
            )
            return result == 1
        except redis.RedisError:
            return False
    
    def extend(self, lock_name: str, lock_value: str, timeout: int) -> bool:
        """
        延长锁的过期时间
        
        Args:
            lock_name: 锁名称
            lock_value: 锁标识
            timeout: 新的过期时间
            
        Returns:
            是否延长成功
        """
        current_value = self._client.get(f"lock:{lock_name}")
        if current_value == lock_value:
            return self._client.expire(f"lock:{lock_name}", timeout)
        return False
    
    def release_safe(self, lock_name: str, lock_value: str):
        """
        安全释放锁（不抛异常）
        
        Args:
            lock_name: 锁名称
            lock_value: 锁标识
        """
        try:
            self.release(lock_name, lock_value)
        except Exception:
            pass


class TaskQueue:
    """
    基于 Redis 的分布式任务队列
    支持任务添加、获取、确认、重试等功能
    """
    
    def __init__(self, redis_client: RedisClient, queue_name: str = "default"):
        """
        初始化任务队列
        
        Args:
            redis_client: Redis 客户端实例
            queue_name: 队列名称
        """
        self.redis = redis_client
        self.queue_name = queue_name
        self._processing_set = f"taskqueue:{queue_name}:processing"
        self._failed_set = f"taskqueue:{queue_name}:failed"
    
    def add(self, task_data: Dict, priority: int = 0, delay: int = 0) -> str:
        """
        添加任务到队列
        
        Args:
            task_data: 任务数据
            priority: 优先级（数值越大优先级越高）
            delay: 延迟执行时间（秒），0 表示立即执行
            
        Returns:
            任务ID
        """
        import json
        import uuid
        
        task_id = str(uuid.uuid4())
        task = {
            'id': task_id,
            'data': task_data,
            'priority': priority,
            'created_at': datetime.now().isoformat(),
            'retries': 0
        }
        
        if delay > 0:
            # 延迟任务，使用有序集合
            execute_at = datetime.now().timestamp() + delay
            self.redis.zadd(
                f"taskqueue:{self.queue_name}:delayed",
                {json.dumps(task): execute_at}
            )
        else:
            # 立即执行任务，使用列表
            self.redis.lpush(
                f"taskqueue:{self.queue_name}:ready",
                json.dumps(task)
            )
        
        return task_id
    
    def get(self, timeout: int = 0) -> Optional[Dict]:
        """
        从队列获取任务
        
        Args:
            timeout: 等待超时时间（秒），0 表示不等待
            
        Returns:
            任务数据（包含任务ID），None 表示无任务
        """
        import json
        
        if timeout > 0:
            result = self.redis.blpop(
                [f"taskqueue:{self.queue_name}:ready"],
                timeout
            )
            if result:
                task_data = json.loads(result[1])
                # 添加到处理中集合
                self.redis.zadd(
                    self._processing_set,
                    {json.dumps(task_data): datetime.now().timestamp()}
                )
                return task_data
        else:
            result = self.redis.rpop(f"taskqueue:{self.queue_name}:ready")
            if result:
                task_data = json.loads(result)
                # 添加到处理中集合
                self.redis.zadd(
                    self._processing_set,
                    {json.dumps(task_data): datetime.now().timestamp()}
                )
                return task_data
        return None
    
    def ack(self, task_id: str, task_data: Dict) -> bool:
        """
        确认任务完成
        
        Args:
            task_id: 任务ID
            task_data: 任务数据
            
        Returns:
            是否确认成功
        """
        import json
        
        try:
            self.redis.zrem(
                self._processing_set,
                json.dumps(task_data)
            )
            return True
        except Exception:
            return False
    
    def nack(self, task_id: str, task_data: Dict, requeue: bool = True) -> bool:
        """
        否定确认（任务处理失败）
        
        Args:
            task_id: 任务ID
            task_data: 任务数据
            requeue: 是否重新入队
            
        Returns:
            是否操作成功
        """
        import json
        
        try:
            self.redis.zrem(
                self._processing_set,
                json.dumps(task_data)
            )
            
            if requeue:
                # 增加重试次数后重新入队
                task_data['retries'] = task_data.get('retries', 0) + 1
                self.redis.lpush(
                    f"taskqueue:{self.queue_name}:ready",
                    json.dumps(task_data)
                )
            else:
                # 移到失败队列
                self.redis.lpush(
                    self._failed_set,
                    json.dumps(task_data)
                )
            return True
        except Exception:
            return False
    
    def move_delayed_tasks(self) -> int:
        """
        将到期的延迟任务移动到就绪队列
        
        Returns:
            移动的任务数量
        """
        import json
        
        now = datetime.now().timestamp()
        tasks = self.redis.zrangebyscore(
            f"taskqueue:{self.queue_name}:delayed",
            0,
            now
        )
        
        count = 0
        for task_json in tasks:
            task_data = json.loads(task_json)
            self.redis.lpush(
                f"taskqueue:{self.queue_name}:ready",
                task_json
            )
            count += 1
        
        if count > 0:
            self.redis.zremrangebyscore(
                f"taskqueue:{self.queue_name}:delayed",
                0,
                now
            )
        
        return count
    
    def get_queue_stats(self) -> Dict:
        """
        获取队列统计信息
        
        Returns:
            统计信息字典
        """
        import json
        
        ready_count = self.redis.llen(f"taskqueue:{self.queue_name}:ready")
        delayed_count = self.redis.zcard(f"taskqueue:{self.queue_name}:delayed")
        processing_count = self.redis.zcard(self._processing_set)
        failed_count = self.redis.llen(self._failed_set)
        
        return {
            'queue_name': self.queue_name,
            'ready': ready_count,
            'delayed': delayed_count,
            'processing': processing_count,
            'failed': failed_count
        }
    
    def clear(self, include_failed: bool = False):
        """
        清空队列
        
        Args:
            include_failed: 是否同时清空失败队列
        """
        self.redis.delete(f"taskqueue:{self.queue_name}:ready")
        self.redis.delete(f"taskqueue:{self.queue_name}:delayed")
        self.redis.delete(self._processing_set)
        
        if include_failed:
            self.redis.delete(self._failed_set)
    
    def retry_failed_tasks(self) -> int:
        """
        重试失败队列中的任务
        
        Returns:
            重试的任务数量
        """
        import json
        
        count = 0
        while True:
            task = self.redis.rpop(self._failed_set)
            if task is None:
                break
            task_data = json.loads(task)
            task_data['retries'] = 0
            self.redis.lpush(
                f"taskqueue:{self.queue_name}:ready",
                json.dumps(task_data)
            )
            count += 1
        
        return count
