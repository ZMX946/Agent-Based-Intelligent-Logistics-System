# coding = utf-8
"""
异步API服务
使用FastAPI实现的异步API服务，支持高可用、限流、熔断等特性
"""
import json
import time
import signal
import threading
import atexit
import asyncio
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
from apis.api_planning_hub import ApiPlanningHub
from entity import Parameter, Tool, Task, User
from tasks import GenerateTaskHub
from models import LargeLanguageModel
from tools.tool_manager import ToolManager
from tasks import TaskManager
from use_manager.user_manager import UserManagerHub
from utils import RESPONSE_AUTH_CODE_ERROR, RESPONSE_ALLOW_CODE_ERROR, RESPONSE_STATUS_CODE_SUCCESS, DEFAULT_PERMISSIONS
from utils import TASK_STATUS_FINISH, TASK_STATUS_RUNNING, TASK_STATUS_WAIT_CONFIRM, TASK_TYPE_UNKNOWN, \
    TASK_SYS_OUTPUT_STOP, GRAPH_TITLE_SUCESS, GRAPH_TITLE_FAILURE
from utils.logger_config import setup_logger, logger
import traceback
import os
import jwt
from datetime import datetime, timedelta
import uuid
from cachetools import TTLCache
from functools import wraps

from utils.redis_client import get_redis_client, RedisDistributedLock, RedisIdempotentProcessor
from utils.rate_limiter import (
    RateLimiterFactory,
    RateLimitStrategy,
    get_rate_limiter_factory,
    RateLimitExceededError
)
from utils.circuit_breaker import (
    CircuitBreakerRegistry,
    get_circuit_breaker_registry
)
from utils.llm_router import get_provider_manager
from utils.health_check import get_health_check_service
from utils.queue_manager import get_queue_manager
from utils.exception_handler import setup_exception_handlers
from utils.exceptions import (
    UserNotFoundException,
    UserAlreadyExistsException,
    InvalidPasswordException,
    InvalidCredentialsException,
    ToolNotFoundException,
    ToolOperationException,
    TaskNotFoundException,
    TaskTimeoutException,
    TaskFailedException,
    TokenExpiredException,
    TokenInvalidException,
    PermissionDeniedException,
    MissingParameterException,
    InvalidParameterException,
    DatabaseConnectionException,
    DatabaseQueryException,
    DatabaseWriteException,
    CacheConnectionException,
    CacheOperationException,
    ExternalServiceUnavailableException,
    ExternalServiceTimeoutException,
    ResourceNotFoundException,
    RateLimitException,
    DistributedLockException
)
from utils.config import (
    milvus_uri,
    model_path,
    milvus_db_name,
    model_name,
    model_temperature,
    model_top_p,
    mongo_host,
    mongo_db,
    mongo_port,
    topK,
    model_api_key,
    model_base_url, SECRET_KEY, JWT_ALGORITHM,
    async_mode, async_max_workers, local_mode
)

# JWT配置
JWT_EXPIRATION_DELTA = timedelta(hours=1)

# 创建TTL缓存，存储用户会话信息，最大1000个，有效期1小时
session_cache = TTLCache(maxsize=1000, ttl=3600)

# 创建FastAPI应用
app = FastAPI(
    title="Agent Copilot HITL API",
    description="Agent Copilot HITL API 服务，提供工具管理、任务调度、API规划等功能",
    version="1.0.0",
    openapi_url="/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc"
)

# 设置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],  # 允许前端和HITL前端
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 设置全局异常处理器
setup_exception_handlers(app)
logger.info("全局异常处理器已初始化")

# 获取CPU核心数并计算线程池大小
cpu_count = os.cpu_count()
if cpu_count is None:
    # 无法获取CPU数量时的默认值
    max_workers = 8
else:
    max_workers = cpu_count * 2

# 初始化管理器
toolManager = ToolManager(mongo_host, mongo_db, mongo_port, milvus_uri, milvus_db_name)
taskManager = TaskManager(mongo_host, mongo_db, mongo_port)
userManagerHub = UserManagerHub(mongo_host, mongo_db, mongo_port)

# 创建全局ApiPlanningHub实例，避免重复创建
import concurrent.futures
executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
api_planning_hub_global = ApiPlanningHub(milvus_uri, model_path, milvus_db_name, model_name,
                                    model_temperature, model_top_p, mongo_host, mongo_db, mongo_port, topK,
                                    model_base_url, model_api_key, executor)

# 初始化队列管理器并启动worker
queue_manager = get_queue_manager()
queue_manager.start_worker()
logger.info("队列管理器初始化完成并启动了worker")

shutdown_event = threading.Event()
shutdown_requested = False

# 初始化高可用组件
async def init_high_availability():
    # 不清除缓存，保留之前的任务数据
    logger.info("初始化高可用组件...")
    
    # 继续初始化高可用组件
    try:
        redis_client = get_redis_client()
        if async_mode:
            # 异步Redis客户端需要初始化连接池
            await redis_client._init_pool()
        if redis_client.is_healthy:
            logger.info("Redis connection established successfully")
        else:
            logger.warning("Redis is not healthy, some features may be limited")
    except Exception as e:
        logger.warning(f"Failed to initialize Redis: {e}")

    try:
        rate_limiter_factory = get_rate_limiter_factory()
        rate_limiter_factory.create_limiter(
            key="api:global",
            max_tokens=1000,
            refill_rate=100,
            strategy=RateLimitStrategy.SLIDING_WINDOW
        )
        logger.info("Global API rate limiter initialized")
    except Exception as e:
        logger.warning(f"Failed to initialize rate limiter: {e}")

    try:
        provider_manager = get_provider_manager()
        
        # 启动定时迁移任务
        asyncio.create_task(schedule_task_migration())
        logger.info("Task migration scheduler started")
    except Exception as e:
        logger.warning(f"Failed to initialize provider manager: {e}")

    try:
        provider_manager.start_health_check(interval=30.0)
        logger.info("Provider health check started")
    except Exception as e:
        logger.warning(f"Failed to start provider health check: {e}")

# 初始化分布式锁和幂等性处理器
try:
    redis_client = get_redis_client()
    distributed_lock = RedisDistributedLock(redis_client)
    idempotent_processor = RedisIdempotentProcessor(redis_client)
    logger.info("分布式锁和幂等性处理器初始化完成")
except Exception as e:
    logger.warning(f"Failed to initialize distributed lock or idempotent processor: {e}")
    distributed_lock = None
    idempotent_processor = None

async def schedule_task_migration():
    """
    定期将超过7天的任务迁移到session库
    """
    while True:
        try:
            # 每24小时执行一次迁移
            await asyncio.sleep(24 * 60 * 60)
            logger.info("Starting task migration...")
            migrated_count = taskManager.migrate_old_tasks_to_session(days=7)
            logger.info(f"Task migration completed. Migrated {migrated_count} tasks to session library.")
        except Exception as e:
            logger.error(f"Task migration failed: {e}\n{traceback.format_exc()}")
            # 如果出现错误，等待1小时后重试
            await asyncio.sleep(60 * 60)

# 启动事件
@app.on_event("startup")
async def startup_event():
    await init_high_availability()
    # 不清除任务缓存，保留之前的任务数据

# 关闭事件
@app.on_event("shutdown")
async def on_shutdown_event():
    global shutdown_requested
    logger.info("Starting graceful shutdown...")
    shutdown_requested = True
    shutdown_event.set()

    try:
        provider_manager = get_provider_manager()
        provider_manager.stop_health_check()
    except Exception as e:
        logger.warning(f"Error stopping health check: {e}")

    try:
        redis_client = get_redis_client()
        if async_mode:
            await redis_client.close()
        else:
            redis_client.close()
    except Exception as e:
        logger.warning(f"Error closing Redis connection: {e}")

    logger.info("Graceful shutdown completed")

# 权限验证依赖项
async def verify_token(request: Request):
    # 检查是否是免验证的端点
    endpoint = request.url.path
    if endpoint in ['/register_user', '/login_user', '/health']:
        return {"user": None}

    # 检查Authorization头
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        raise HTTPException(
            status_code=RESPONSE_AUTH_CODE_ERROR,
            detail="未提供认证令牌"
        )

    try:
        # 验证Bearer令牌格式
        if not auth_header.startswith('Bearer '):
            raise HTTPException(
                status_code=RESPONSE_AUTH_CODE_ERROR,
                detail="无效的令牌格式"
            )

        # 提取令牌
        token = auth_header.split(' ')[1]

        # 验证令牌是否在缓存中
        if token not in session_cache:
            raise HTTPException(
                status_code=RESPONSE_AUTH_CODE_ERROR,
                detail="令牌无效或已过期"
            )

        # 解码JWT令牌（验证签名和有效期）
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        except jwt.ExpiredSignatureError:
            # 令牌已过期，从缓存中移除
            if token in session_cache:
                del session_cache[token]
            raise HTTPException(
                status_code=RESPONSE_AUTH_CODE_ERROR,
                detail="令牌已过期"
            )
        except jwt.InvalidTokenError:
            raise HTTPException(
                status_code=RESPONSE_AUTH_CODE_ERROR,
                detail="无效的令牌"
            )

        # 将用户信息存储到返回值中，供后续处理使用
        user = session_cache[token]

        # 检查用户是否有权限访问当前接口
        endpoint_path = endpoint.strip('/')
        
        # 将实际端点路径映射到用户期望的权限名称
        endpoint_name_map = {
            'login_user': 'login',
            'logout_user': 'logout',
            'mesh_query': 'mesh_query',
            'api_planning': 'mesh_query',
            'api_task_status': 'get_task_status',
            'api_user_tasks': 'get_task_status',
            'delete_all_tool': 'delete_tool_db',
            'insert_tool': 'insert_tool',
            'delete_tool_by_ids': 'delete_tool_db_by_ids',
            'upload_tool': 'upload_tool',
            'get_all_tools': 'get_all_tools'
        }
        
        endpoint_name = endpoint_name_map.get(endpoint_path, endpoint_path)
        logger.info(f"用户 {user['username']} 请求访问接口 {endpoint_name}")
        logger.info(f"用户权限列表: {user['user_authority']}")
        
        if endpoint_name not in user['user_authority']:
            logger.warning(f"用户 {user['username']} 没有权限访问接口 {endpoint_name}")
            raise HTTPException(
                status_code=RESPONSE_ALLOW_CODE_ERROR,
                detail=f"没有权限访问 {endpoint_name} 接口"
            )
        logger.info(f"用户 {user['username']} 有权限访问接口 {endpoint_name}")

        return {"user": user}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"认证失败: {str(e)}")
        raise HTTPException(
            status_code=RESPONSE_AUTH_CODE_ERROR,
            detail="认证失败"
        )

async def check_health():
    try:
        logger.info("Starting health check...")
        health_service = get_health_check_service()
        result = await health_service.check_all_async()
        logger.info(f"Health check completed: {result}")
        return result
    except Exception as e:
        import traceback
        logger.critical(f"Health check failed: {e}")
        logger.critical(f"Health check traceback: {traceback.format_exc()}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "timestamp": datetime.now().isoformat()
        }

# 限流依赖
def rate_limit_dependency():
    async def inner():
        try:
            rate_limiter_factory = get_rate_limiter_factory()
            rate_limiter = rate_limiter_factory.get_limiter("api:global")
            result = rate_limiter.consume(1)
            if not result.allowed:
                raise RateLimitExceededError(
                    f"Rate limit exceeded. Retry after {result.retry_after:.2f}s"
                )
        except RateLimitExceededError as e:
            raise HTTPException(
                status_code=429,
                detail="请求频率过高，请稍后再试"
            )
        except Exception as e:
            logger.error(f"Rate limiting error: {e}")
            raise HTTPException(
                status_code=500,
                detail="内部服务器错误"
            )
    return Depends(inner)

@app.get('/health')
async def health_check(rate_limited: None = rate_limit_dependency()):
    """
    Health Check
    
    Check the health status of the service
    """
    logger.info("接收到健康检查请求")
    health_status = await check_health()
    logger.info(f"健康检查结果: {health_status}")
    status_code = 200 if health_status["status"] == "healthy" else 503
    
    return JSONResponse(health_status, status_code=status_code)

@app.get('/delete_all_tool')
async def delete_tool_db(user_info: dict = Depends(verify_token)):
    """
            工具数据库删除
            --- 
            tags:
              - Tool Delete
            description:
                上传文件到服务器
            responses:
              200:
                description: 数据库清空成功
              400:
                description: 数据库未清空成功
        """

    import asyncio
    await asyncio.to_thread(toolManager.delete_all_tools)
    return JSONResponse({'message': 'delete tool success! '}, status_code=RESPONSE_STATUS_CODE_SUCCESS)

@app.get('/metrics')
async def get_metrics():
    """
    Metrics
    
    Get system metrics including providers, rate limiters, and circuit breakers
    """
    try:
        provider_manager = get_provider_manager()
        rate_limiter_factory = get_rate_limiter_factory()
        circuit_breaker_registry = get_circuit_breaker_registry()

        metrics = {
            "providers": provider_manager.get_all_stats(),
            "rate_limiters": rate_limiter_factory.get_all_configs(),
            "circuit_breakers": circuit_breaker_registry.get_all_info()
        }

        return {
            "status": 200,
            "message": "Metrics retrieved successfully",
            "data": metrics
        }
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get metrics: {str(e)}"
        )

@app.post('/insert_tool')
async def insert_tool(request: Request, user_info: dict = Depends(verify_token)):
    """
    工具插入
    --- 
    tags:
      - Tool Management
    description:
        插入一个新的工具到数据库中
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: Tool Insert Request
          required:
            - operationId
            - name_for_human
            - name_for_model
            - description
            - url
            - path
            - method
            - params
          properties:
            operationId:
              type: string
              description: 操作ID
            name_for_human:
              type: string
              description: 人类可读的工具名称
            name_for_model:
              type: string
              description: 模型使用的工具名称
            description:
              type: string
              description: 工具的描述
            url:
              type: string
              description: API 的 URL
            path:
              type: string
              description: API 的路径
            method:
              type: string
              description: HTTP 方法 (如 GET, POST 等)
            params:
              type: array
              items:
                type: object
                properties:
                  param_name:
                    type: string
                    description: 参数名称
                  paramType:
                    type: string
                    description: 参数类型
                  param_description:
                    type: string
                    description: 参数描述
                  enum:
                    type: array
                    items:
                      type: string
                    description: 参数的枚举值
                  in_:
                    type: string
                    description: 参数的位置 (如 query, body 等)
    responses:
      200:
        description: 工具插入成功
        schema:
          type: object
          properties:
            message:
              type: string
      400:
        description: 请求参数缺失或无效
        schema:
          type: object
          properties:
            error:
              type: string
    """
    data = await request.json()
    if not data:
        return JSONResponse({'error': 'Missing query parameter'}, status_code=400)

    params = []
    for tmp in data["params"]:
        parameter = Parameter(
            name=tmp["param_name"],
            type=tmp["paramType"],
            description=tmp["param_description"],
            enum=tmp["enum"],
            required=True,
            in_=tmp["in_"]
        )
        params.append(parameter)

    tool = Tool(
        tool_id=0,
        operationId=data["operationId"],
        name_for_human=data["name_for_human"],
        name_for_model=data["name_for_model"],
        description=data["description"],
        api_url=data["url"],
        path=data["path"],
        method=data["method"],
        request_body=params
    )
    import asyncio
    await asyncio.to_thread(toolManager.insert_tools, [tool])
    return JSONResponse({'message': 'insert tool success! '}, status_code=RESPONSE_STATUS_CODE_SUCCESS)

@app.get('/get_all_tools')
async def get_all_tools(user_info: dict = Depends(verify_token)):
    """
    获取所有工具
    --- 
    tags:
      - Tool Management
    description:
        获取数据库中所有的工具
    responses:
      200:
        description: 获取工具成功
      400:
        description: 获取工具失败
    """
    import asyncio
    tools = await asyncio.to_thread(toolManager.get_all_tools)
    return JSONResponse({'status_code': RESPONSE_STATUS_CODE_SUCCESS, 'data': tools}, status_code=RESPONSE_STATUS_CODE_SUCCESS)

@app.post('/delete_tool_by_ids')
async def delete_tool_by_ids(request: Request, user_info: dict = Depends(verify_token)):
    """
    特定id工具数据库删除
    --- 
    tags:
      - Tool Delete By Ids
    description:
        根据提供的ID列表删除工具
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: Tool Delete Request
          required:
            - ids
          properties:
            ids:
              type: array
              items:
                type: integer
              description: 需要删除的工具ID列表
      - name: X-Request-ID
        in: header
        required: false
        type: string
        description: 请求唯一标识，用于幂等性处理
    responses:
      200:
        description: 数据库删除成功
        schema:
          type: object
          properties:
            message:
              type: string
      400:
        description: 数据库未删除成功
        schema:
          type: object
          properties:
            error:
              type: string
    """
    # 幂等性处理
    request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    idempotent_key = f"delete_tool:{request_id}"
    
    if idempotent_processor:
        is_first_request = await idempotent_processor.process(idempotent_key)
        if not is_first_request:
            logger.info(f"重复请求，request_id: {request_id}")
            return JSONResponse({'message': 'delete tool success! '}, status_code=RESPONSE_STATUS_CODE_SUCCESS)
    
    data = await request.json()
    if not data or 'ids' not in data:
        return JSONResponse({'error': 'Missing query parameter'}, status_code=400)

    ids = data['ids']
    
    # 使用分布式锁防止并发删除
    lock_key = f"delete_tool:{'-'.join(map(str, ids))}"
    if distributed_lock:
        if not await distributed_lock.acquire(lock_key, expire=10):
            logger.warning(f"获取锁失败，ids: {ids}")
            return JSONResponse({'error': '操作正在进行中，请稍后重试'}, status_code=400)
        
        try:
            import asyncio
            await asyncio.to_thread(toolManager.delete_tools, ids)
        finally:
            await distributed_lock.release(lock_key)
    else:
        import asyncio
        await asyncio.to_thread(toolManager.delete_tools, ids)
    
    logger.info(f"工具删除成功")
    return JSONResponse({'message': 'delete tool success! '}, status_code=RESPONSE_STATUS_CODE_SUCCESS)

@app.post('/upload_tool')
async def upload_file(request: Request, user_info: dict = Depends(verify_token)):
    """
        文件上传接口
        --- 
        tags:
          - File Upload
        description:
            上传文件到服务器
        parameters:
          - name: file
            in: formData
            type: file
            required: true
          - name: X-Request-ID
            in: header
            required: false
            type: string
            description: 请求唯一标识，用于幂等性处理
        responses:
          200:
            description: 文件上传成功
          400:
            description: 文件未上传成功
    """
    # 幂等性处理
    request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    idempotent_key = f"upload_file:{request_id}"
    
    if idempotent_processor:
        is_first_request = await idempotent_processor.process(idempotent_key)
        if not is_first_request:
            logger.info(f"重复请求，request_id: {request_id}")
            return JSONResponse({'message': 'upload file success! '}, status_code=RESPONSE_STATUS_CODE_SUCCESS)
    
    # 处理文件上传
    form = await request.form()
    file = form.get('file')
    
    if not file:
        return JSONResponse({'error': '没有文件部分'}, status_code=400)
    
    # 使用分布式锁防止并发上传
    lock_key = f"upload_file:{request_id}"
    lock_acquired = False
    try:
        if distributed_lock:
            if not await distributed_lock.acquire(lock_key, expire=30):
                logger.warning(f"获取锁失败，request_id: {request_id}")
                return JSONResponse({'error': '操作正在进行中，请稍后重试'}, status_code=400)
            lock_acquired = True
            
        # 保存文件
        import os
        # 使用绝对路径确保文件保存到正确的位置
        api_data_dir = os.path.join(os.path.dirname(__file__), 'api_data')
        # 确保目录存在
        os.makedirs(api_data_dir, exist_ok=True)
        file_path = os.path.join(api_data_dir, file.filename)
        with open(file_path, 'wb') as f:
            f.write(await file.read())
        
        # 调用toolManager处理文件
        import asyncio
        await asyncio.to_thread(toolManager.upload_file, file_path)
        
        logger.info(f"文件上传成功: {file.filename}")
        return JSONResponse({'message': 'upload file success! '}, status_code=RESPONSE_STATUS_CODE_SUCCESS)
    finally:
        if distributed_lock and lock_acquired:
            try:
                await distributed_lock.release(lock_key)
            except Exception as e:
                logger.error(f"释放分布式锁失败: {e}")

@app.post('/register_user')
async def register(request: Request):
    """
    用户注册
    --- 
    tags:
      - User Management
    description:
        注册新用户
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: User Registration Request
          required:
            - userName
            - password
            - confirm_password
          properties:
            userName:
              type: string
              description: 用户名
            password:
              type: string
              description: 密码
            confirm_password:
              type: string
              description: 确认密码
      - name: X-Request-ID
        in: header
        required: false
        type: string
        description: 请求唯一标识，用于幂等性处理
    responses:
      200:
        description: 用户注册成功
        schema:
          type: object
          properties:
            message:
              type: string
              example: "create user success!"
      400:
        description: 用户注册失败
        schema:
          type: object
          properties:
            message:
              type: string
              example: "create user failed!"
      409:
        description: 用户名已存在
        schema:
          type: object
          properties:
            message:
              type: string
              example: "该用户已注册"
    """
    # 幂等性处理
    request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    idempotent_key = f"register:{request_id}"
    
    if idempotent_processor:
        is_first_request = await idempotent_processor.process(idempotent_key)
        if not is_first_request:
            logger.info(f"重复请求，request_id: {request_id}")
            return JSONResponse({'message': 'create user success!'}, status_code=RESPONSE_STATUS_CODE_SUCCESS)
    
    data = await request.json()
    if not data:
        raise MissingParameterException("请求体")
    
    # 检查必要字段，同时支持userName和username
    has_username = "userName" in data or "username" in data
    has_password = "password" in data
    has_confirm = "confirm_password" in data or "confirm" in data
    
    if not (has_username and has_password and has_confirm):
        raise MissingParameterException("用户名、密码和确认密码")

    # 优先使用userName，兼容username
    userName = data.get("userName", data.get("username", "")).strip()
    # 优先使用confirm_password，兼容confirm
    confirm_password = data.get("confirm_password", data.get("confirm", ""))
    password = data["password"]
    
    # 输入验证
    if not userName:
        raise InvalidParameterException("userName", "用户名不能为空")
    
    if not password:
        raise InvalidParameterException("password", "密码不能为空")
    
    if not confirm_password:
        raise InvalidParameterException("confirm_password", "确认密码不能为空")
    
    if len(userName) < 3 or len(userName) > 20:
        raise InvalidParameterException("userName", "用户名长度必须在3-20个字符之间")
    
    if len(password) < 6:
        raise InvalidParameterException("password", "密码长度必须至少为6个字符")
    
    if password != confirm_password:
        raise InvalidPasswordException("两次输入的密码不一致")
    
    # 使用分布式锁防止并发注册同一用户
    lock_key = f"register:{userName}"
    if distributed_lock:
        if not await distributed_lock.acquire(lock_key, expire=30):
            logger.warning(f"获取锁失败，用户注册: {userName}")
            raise DistributedLockException("操作正在进行中，请稍后重试")
        
        try:
            status_code, message = userManagerHub.create_user(userName, password, confirm_password, DEFAULT_PERMISSIONS)
            if status_code == RESPONSE_STATUS_CODE_SUCCESS:
                logger.info(f"成功创建用户")
                return JSONResponse({'message': 'create user success!'}, status_code=RESPONSE_STATUS_CODE_SUCCESS)
            elif status_code == 409:
                logger.warning(f"尝试注册已存在的用户: {userName}")
                raise UserAlreadyExistsException(userName)
            else:
                logger.error(f"创建用户失败: {userName}, 原因: {message}")
                raise InvalidParameterException("user", message)
        finally:
            await distributed_lock.release(lock_key)
    else:
        status_code, message = userManagerHub.create_user(userName, password, confirm_password, DEFAULT_PERMISSIONS)
        if status_code == RESPONSE_STATUS_CODE_SUCCESS:
            logger.info(f"成功创建用户")
            return JSONResponse({'message': 'create user success!'}, status_code=RESPONSE_STATUS_CODE_SUCCESS)
        elif status_code == 409:
            logger.warning(f"尝试注册已存在的用户: {userName}")
            raise UserAlreadyExistsException(userName)
        else:
            logger.error(f"创建用户失败: {userName}, 原因: {message}")
            raise InvalidParameterException("user", message)

@app.post('/login_user')
async def login(request: Request):
    """
    用户登录
    --- 
    tags:
      - User Management
    description:
        用户登录接口
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: User Login Request
          required:
            - userName
            - password
          properties:
            userName:
              type: string
              description: 用户名
            password:
              type: string
              description: 密码
      - name: X-Request-ID
        in: header
        required: false
        type: string
        description: 请求唯一标识，用于幂等性处理
    responses:
      200:
        description: 用户登录成功
        schema:
          type: object
          properties:
            status:
              type: integer
              example: 200
            message:
              type: string
              example: "登录成功"
            data:
              type: object
              properties:
                token:
                  type: object
                  properties:
                    access_token:
                      type: string
                      example: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
                    expires_in:
                      type: integer
                      example: 3600
                    token_type:
                      type: string
                      example: "Bearer"
      400:
        description: 用户登录失败
        schema:
          type: object
          properties:
            message:
              type: string
              example: "登录失败！请检查用户名和密码"
    """
    # 处理OPTIONS预检请求
    if request.method == "OPTIONS":
        return JSONResponse(status_code=200)
    
    # 幂等性处理 - 登录操作可以重复执行，返回相同的结果
    request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    
    try:
        data = await request.json()
        if not data:
            raise MissingParameterException("请求体")
    except Exception as e:
        # 捕获解析JSON时的异常，可能是OPTIONS请求或其他格式错误
        # 如果是OPTIONS请求，返回200
        if request.method == "OPTIONS":
            return JSONResponse(status_code=200)
        # 否则，抛出异常
        raise MissingParameterException("请求体")

    # 检查必要字段，同时支持userName和username
    if ("userName" not in data and "username" not in data) or "password" not in data:
        raise MissingParameterException("用户名或密码")

    # 优先使用userName，兼容username
    userName = data.get("userName", data.get("username", "")).strip()
    password = data["password"]
    
    # 输入验证
    if not userName:
        raise InvalidParameterException("userName", "用户名不能为空")
    
    if not password:
        raise InvalidParameterException("password", "密码不能为空")
    
    if len(userName) < 3 or len(userName) > 20:
        raise InvalidParameterException("userName", "用户名长度必须在3-20个字符之间")
    
    if len(password) < 6:
        raise InvalidParameterException("password", "密码长度必须至少为6个字符")
    
    # 使用分布式锁防止并发登录导致的性能问题
    lock_key = f"login:{userName}"
    if distributed_lock:
        if not await distributed_lock.acquire(lock_key, expire=10):
            logger.warning(f"获取锁失败，用户登录: {userName}")
            raise DistributedLockException("操作正在进行中，请稍后重试")
        
        try:
            user = userManagerHub.login(userName, password)
            if user.user_id > 0:
                logger.info(f"登录成功，用户: {user.userName}, 权限列表: {user.user_authority}")

                # 生成唯一访问令牌
                access_token = jwt.encode({
                    'jti': str(uuid.uuid4()),  # JWT ID，确保唯一性
                    'user_id': user.user_id,
                    'username': user.userName,
                    'user_authority': user.user_authority,
                    'exp': datetime.utcnow() + JWT_EXPIRATION_DELTA
                }, SECRET_KEY, algorithm=JWT_ALGORITHM)

                logger.info(f"生成访问令牌: {access_token}")

                # 将用户信息存储到缓存
                session_cache[access_token] = {
                    'user_id': user.user_id,
                    'username': user.userName,
                    'user_authority': user.user_authority
                }

                logger.info(f"用户访问令牌: {access_token}已存入本地缓存")

                # 返回符合要求的响应结构
                return JSONResponse({
                    "status": RESPONSE_STATUS_CODE_SUCCESS,
                    "message": "登录成功",
                    "auth_data": {
                        "token": {
                            "access_token": access_token,
                            "expires_in": 3600,
                            "token_type": "Bearer"
                        }
                    }
                }, status_code=200)
            else:
                raise InvalidCredentialsException()
        finally:
            await distributed_lock.release(lock_key)
    else:
        user = userManagerHub.login(userName, password)
        if user.user_id > 0:
            logger.info(f"登录成功，用户: {user.userName}, 权限列表: {user.user_authority}")

            # 生成唯一访问令牌
            access_token = jwt.encode({
                'jti': str(uuid.uuid4()),  # JWT ID，确保唯一性
                'user_id': user.user_id,
                'username': user.userName,
                'user_authority': user.user_authority,
                'exp': datetime.utcnow() + JWT_EXPIRATION_DELTA
            }, SECRET_KEY, algorithm=JWT_ALGORITHM)

            logger.info(f"生成访问令牌: {access_token}")

            # 将用户信息存储到缓存
            session_cache[access_token] = {
                'user_id': user.user_id,
                'username': user.userName,
                'user_authority': user.user_authority
            }

            logger.info(f"用户访问令牌: {access_token}已存入本地缓存")

            # 返回符合要求的响应结构
            return JSONResponse({
                "status": RESPONSE_STATUS_CODE_SUCCESS,
                "message": "登录成功",
                "auth_data": {
                    "token": {
                        "access_token": access_token,
                        "expires_in": 3600,
                        "token_type": "Bearer"
                    }
                }
            }, status_code=200)
        else:
            raise InvalidCredentialsException()

@app.post('/logout_user')
async def logout(request: Request):
    """
    登出用户
    --- 
    tags:
      - user management
    description:
      用户登出接口，通过 POST 方法接收用户 ID 并执行登出操作。
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: logout_body
          required:
            - user_id
          properties:
            user_id:
              type: integer
              format: int32
              description: 需要登出的用户 ID。
    responses:
      200:
        description: 登出成功
        schema:
          type: object
          properties:
            message:
              type: string
              example: logout success!
      400:
        description: 登出失败或缺少参数
        schema:
          type: object
          properties:
            error:
              type: string
              example: logout failed! 或 Missing query parameter
    """
    # 从Authorization头获取令牌
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return JSONResponse({'error': 'Invalid token format'}, status_code=400)

    token = auth_header.split(' ')[1]

    # 从缓存中删除令牌
    if token in session_cache:
        del session_cache[token]

    return JSONResponse({'message': 'logout success!'}, status_code=RESPONSE_STATUS_CODE_SUCCESS)

@app.post('/test_llm')
async def test_llm(request: Request, user_info: dict = Depends(verify_token)):
    """
    测试大语言模型
    --- 
    tags:
      - Model Test
    description:
        测试大语言模型的功能
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: LLM Test Request
          required:
            - prompt
          properties:
            prompt:
              type: string
    responses:
      200:
        description: 测试成功
      400:
        description: 测试失败
    """
    data = await request.json()
    prompt = data.get('prompt', '')
    llm = LargeLanguageModel(model_name, model_api_key, model_base_url)
    response = llm.generate(prompt)
    return JSONResponse({'response': response}, status_code=RESPONSE_STATUS_CODE_SUCCESS)

@app.post('/api_task_status')
async def get_task_status(request: Request, user_info: dict = Depends(verify_token)):
    """
    获取任务状态
    
    获取指定任务ID的任务状态
    --- 
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: Task Status Request
          required:
            - task_id
          properties:
            task_id:
              type: string
              description: 任务ID
      - name: X-Request-ID
        in: header
        required: false
        type: string
        description: 请求唯一标识，用于幂等性处理
    responses:
      200:
        description: 获取任务状态成功
        schema:
          type: object
          properties:
            task:
              type: object
      400:
        description: 请求参数缺失或无效
        schema:
          type: object
          properties:
            detail:
              type: string
      404:
        description: 任务未找到或仍在运行
        schema:
          type: object
          properties:
            detail:
              type: string
    """
    # 幂等性处理 - 对于查询操作，可以使用缓存来提高性能
    request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    
    data = await request.json()
    if not data:
        raise MissingParameterException("请求体")
    
    if 'task_id' not in data:
        raise MissingParameterException("task_id")
    
    task_id = data['task_id']
    
    # 使用分布式锁防止并发查询导致的性能问题
    lock_key = f"get_task_status:{task_id}"
    if distributed_lock:
        if not await distributed_lock.acquire(lock_key, expire=10):
            logger.warning(f"获取锁失败，task_id: {task_id}")
            # 对于查询操作，可以不返回错误，直接执行
    
    try:
        task = taskManager.get_task_by_id(task_id)
        if task is not None:
            # 将Task对象转换为前端期望的字典格式
            task_dict = task.to_dict()
            return {"task": task_dict}
        else:
            raise TaskNotFoundException(task_id)
    finally:
        if distributed_lock:
            await distributed_lock.release(lock_key)

@app.post('/api_planning')
async def mesh_query(request: Request, user_info: dict = Depends(verify_token)):
    """
    Agent Planning
    
    Agent Planning接口，json格式
    --- 
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: Agent Planning Request
          required:
            - query
            - contexts
            - isCopilot
            - isContext
            - contextNumber
          properties:
            query:
              type: string
              description: 查询语句
            contexts:
              type: array
              description: 上下文信息
            isCopilot:
              type: boolean
              description: 是否使用Copilot模式
            isContext:
              type: boolean
              description: 是否使用上下文
            contextNumber:
              type: integer
              description: 上下文数量
      - name: X-Request-ID
        in: header
        required: false
        type: string
        description: 请求唯一标识，用于幂等性处理
    responses:
      200:
        description: 任务创建成功
        schema:
          type: object
          properties:
            task_id:
              type: string
      400:
        description: 请求参数缺失或无效
        schema:
          type: object
          properties:
            detail:
              type: string
    """
    # 幂等性处理
    request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    idempotent_key = f"api_planning:{request_id}"
    
    if idempotent_processor:
        is_first_request = await idempotent_processor.process(idempotent_key)
        if not is_first_request:
            logger.info(f"重复请求，request_id: {request_id}")
            # 这里可以返回之前创建的任务ID，或者生成一个新的任务ID
            task = taskManager.create_task("重复请求", user_id=user_info.get('user_id', ''))
            return {"task_id": task.task_id}
    
    data = await request.json()
    if not data:
        raise MissingParameterException("请求体")
    
    # 获取用户ID
    user_id = user_info.get('user_id', '')
    
    # 使用分布式锁防止并发创建任务
    lock_key = f"create_task:{request_id}"
    if distributed_lock:
        if not await distributed_lock.acquire(lock_key, expire=30):
            logger.warning(f"获取锁失败，request_id: {request_id}")
            raise DistributedLockException("操作正在进行中，请稍后重试")
        
        try:
            # 创建任务ID
            task = taskManager.create_task(data.get('query', ''), user_id=user_id)
            task_id = task.task_id
            
            # 异步处理任务
            asyncio.create_task(process_task(task_id, data))
            
            return {"task_id": task_id}
        finally:
            await distributed_lock.release(lock_key)
    else:
        # 创建任务ID
        task = taskManager.create_task(data.get('query', ''), user_id=user_id)
        task_id = task.task_id
        
        # 异步处理任务
        asyncio.create_task(process_task(task_id, data))
        
        return {"task_id": task_id}

@app.get('/api_user_tasks')
async def get_user_tasks(request: Request, user_info: dict = Depends(verify_token)):
    """
    获取用户历史记录列表
    
    获取指定用户的完整历史记录列表，包括最近7天的任务和更早的会话，支持分页和时间范围过滤
    """
    # 获取用户ID
    user_id = user_info.get('user_id', '')
    
    # 获取查询参数
    limit = int(request.query_params.get('limit', 100))
    days = int(request.query_params.get('days', 7))
    
    # 获取用户的完整历史记录（包括最近7天的任务和更早的会话）
    tasks = taskManager.get_user_history(user_id, limit=limit, days=days)
    
    return {"tasks": tasks}

async def process_task(task_id, data):
    """
    异步处理任务
    """
    logger.info(f"准备处理任务{task_id}，任务数据：{data}，处理中......")
    # 设置任务处理超时时间（300秒 = 5分钟）
    timeout_seconds = 300
    # 创建一个事件用于超时控制
    timeout_event = asyncio.Event()
    
    # 定义超时处理函数
    async def timeout_handler():
        await asyncio.sleep(timeout_seconds)
        if not timeout_event.is_set():
            logger.warning(f"任务[{task_id}]处理超时，已超过{timeout_seconds}秒")
            timeout_event.set()
    
    try:
        query = data["query"]
        contexts = data["contexts"]
        isCopilot = data["isCopilot"]
        isContext = data["isContext"]
        contextNumber = data["contextNumber"]

        curr_model_name = model_name
        curr_temperature = model_temperature
        curr_api_key = model_api_key
        curr_api_url = model_base_url

    except Exception as e:
        taskManager.update_task_recorder(task_id, TASK_STATUS_FINISH, '获取前端参数失败！', graph_title=GRAPH_TITLE_FAILURE, isSuccess="失败")
        logger.error(f"任务[{task_id}]获取前端参数失败: {e}\n{traceback.format_exc()}")
        return

    if not isCopilot:
        llm = LargeLanguageModel(curr_api_url, curr_api_key)
        try:
            if isContext:
                results = llm.context_chat_completions(contexts, curr_model_name, curr_temperature, model_top_p, contextNumber)
            else:
                results = llm.chat_completions(query, curr_model_name, curr_temperature, model_top_p)
        except:
            results = ""
        if results is not None and len(results) != 0:
            taskManager.update_task_recorder(task_id, TASK_STATUS_FINISH, results, graph_title=GRAPH_TITLE_SUCESS, isSuccess="成功")
        else:
            taskManager.update_task_recorder(task_id, TASK_STATUS_FINISH, results, graph_title=GRAPH_TITLE_SUCESS, isSuccess="成功")
    else:
        logger.info(f"Task[{task_id}] started successfully, Go on ===>")
    try:
        # 使用全局ApiPlanningHub实例
        global api_planning_hub_global
        generate_task_hub = GenerateTaskHub(curr_model_name, curr_temperature, model_top_p,
                                            curr_api_url, curr_api_key, mongo_host, mongo_db, mongo_port, milvus_uri, milvus_db_name)
        if isContext:
            if len(contexts) < contextNumber:
                target_contexts = contexts
            else:
                target_contexts = contexts[len(contexts) - contextNumber:len(contexts)]
            target_query = generate_task_hub.gen_context_request_task(target_contexts)
        else:
            target_query = query
        
        # 启动超时处理
        timeout_task = asyncio.create_task(timeout_handler())
        
        try:
            # 使用wait_for设置超时
            await asyncio.wait_for(
                asyncio.to_thread(api_planning_hub_global.apis_planning, target_query, task_id),
                timeout=timeout_seconds
            )
            # 标记超时事件已完成
            timeout_event.set()
            
            # 任务处理完成，显式更新任务状态
            logger.debug(f"任务[{task_id}]处理完成，更新最终状态")
            # 直接从数据库获取最新的任务数据
            task = taskManager.get_task_by_id(task_id)
            if task:
                # 更新任务状态为完成
                taskManager.update_task_recorder(
                    task_id=task_id,
                    task_status=TASK_STATUS_FINISH,
                    system_output=task.system_output or "任务完成",
                    graph_title=GRAPH_TITLE_SUCESS,
                    nodes=task.nodes or [],
                    edges=task.edges or []
                )
                
        except asyncio.TimeoutError:
            logger.error(f"任务[{task_id}]处理超时，已取消")
            taskManager.update_task_recorder(task_id, TASK_STATUS_FINISH, "任务处理超时，请稍后重试", graph_title=GRAPH_TITLE_FAILURE, isSuccess="失败")
        except Exception as e:
            logger.error(f"任务[{task_id}]处理失败: {e}\n{traceback.format_exc()}")
            taskManager.update_task_recorder(task_id, TASK_STATUS_FINISH, f"任务处理失败: {str(e)}", graph_title=GRAPH_TITLE_FAILURE, isSuccess="失败")
        finally:
            # 确保超时任务被取消
            timeout_task.cancel()
            try:
                await timeout_task
            except asyncio.CancelledError:
                pass
    except Exception as e:
        logger.error(f"任务[{task_id}]处理失败: {e}\n{traceback.format_exc()}")
        taskManager.update_task_recorder(task_id, TASK_STATUS_FINISH, "任务处理失败，请联系你的系统管理员", graph_title=GRAPH_TITLE_FAILURE, isSuccess="失败")

if __name__ == '__main__':
    import uvicorn
    
    # 设置日志
    setup_logger('copilot')
    
    # 启动服务器，使用1个worker以确保session_cache共享
    uvicorn.run(
        "app_async:app",
        host="0.0.0.0",
        port=5001,
        reload=False,
        workers=1,
        log_level="info"
    )