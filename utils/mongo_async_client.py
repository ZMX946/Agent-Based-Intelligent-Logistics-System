import motor.motor_asyncio
import asyncio
from typing import Optional
from utils import logger


class AsyncMongoClient:
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # 同步初始化，只设置属性
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self._client = None
        self._db = None
        self._initialized = False
        self._host = None
        self._db_name = None
        self._port = None

    async def initialize(self, host: str = "localhost", db: str = "tools", port: int = 27017):
        if self._initialized:
            return
        
        try:
            self._client = motor.motor_asyncio.AsyncIOMotorClient(host, port)
            self._db = self._client[db]
            
            # 测试连接
            await self._client.server_info()
            
            self._initialized = True
            self._host = host
            self._db_name = db
            self._port = port
            
            logger.info(f"异步MongoDB客户端初始化成功: {host}:{port}/{db}")
        except Exception as e:
            logger.error(f"异步MongoDB客户端初始化失败: {e}")
            self._initialized = False
            raise

    @property
    def db(self):
        return self._db

    @property
    def client(self):
        return self._client

    async def close(self):
        if hasattr(self, '_client') and self._client:
            self._client.close()
            logger.info("异步MongoDB客户端已关闭")


_mongo_client = None


async def get_mongo_client(host: str = "localhost", db: str = "tools", port: int = 27017) -> AsyncMongoClient:
    global _mongo_client
    if _mongo_client is None:
        async with AsyncMongoClient._lock:
            if _mongo_client is None:
                _mongo_client = AsyncMongoClient()
                await _mongo_client.initialize(host, db, port)
    return _mongo_client


async def close_mongo_client():
    global _mongo_client
    if _mongo_client is not None:
        await _mongo_client.close()
        _mongo_client = None