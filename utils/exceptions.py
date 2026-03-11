from typing import Optional, Any, Dict


class BaseException(Exception):
    """基础异常类"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 1000,
        http_status_code: int = 500,
        details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.error_code = error_code
        self.http_status_code = http_status_code
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'error_code': self.error_code,
            'message': self.message,
            'details': self.details
        }


class BusinessException(BaseException):
    """业务异常基类"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 5000,
        http_status_code: int = 400,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, error_code, http_status_code, details)


class UserException(BusinessException):
    """用户相关异常"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 5100,
        http_status_code: int = 400,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, error_code, http_status_code, details)


class UserNotFoundException(UserException):
    """用户不存在异常"""
    
    def __init__(self, user_id: Optional[str] = None):
        message = "用户不存在" if user_id is None else f"用户 {user_id} 不存在"
        super().__init__(message, error_code=5101, http_status_code=404)


class UserAlreadyExistsException(UserException):
    """用户已存在异常"""
    
    def __init__(self, username: str):
        message = f"用户 {username} 已存在"
        super().__init__(message, error_code=5102, http_status_code=409)


class InvalidPasswordException(UserException):
    """密码无效异常"""
    
    def __init__(self, message: str = "密码无效"):
        super().__init__(message, error_code=5103, http_status_code=400)


class InvalidCredentialsException(UserException):
    """凭证无效异常"""
    
    def __init__(self, message: str = "用户名或密码错误"):
        super().__init__(message, error_code=5104, http_status_code=401)


class ToolException(BusinessException):
    """工具相关异常"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 5200,
        http_status_code: int = 400,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, error_code, http_status_code, details)


class ToolNotFoundException(ToolException):
    """工具不存在异常"""
    
    def __init__(self, tool_id: Optional[int] = None):
        message = "工具不存在" if tool_id is None else f"工具 {tool_id} 不存在"
        super().__init__(message, error_code=5201, http_status_code=404)


class ToolOperationException(ToolException):
    """工具操作异常"""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code=5202, http_status_code=400, details=details)


class TaskException(BusinessException):
    """任务相关异常"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 5300,
        http_status_code: int = 400,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, error_code, http_status_code, details)


class TaskNotFoundException(TaskException):
    """任务不存在异常"""
    
    def __init__(self, task_id: Optional[str] = None):
        message = "任务不存在" if task_id is None else f"任务 {task_id} 不存在"
        super().__init__(message, error_code=5301, http_status_code=404)


class TaskTimeoutException(TaskException):
    """任务超时异常"""
    
    def __init__(self, task_id: str, timeout: int):
        message = f"任务 {task_id} 执行超时（{timeout}秒）"
        super().__init__(message, error_code=5302, http_status_code=408)


class TaskFailedException(TaskException):
    """任务失败异常"""
    
    def __init__(self, task_id: str, reason: str):
        message = f"任务 {task_id} 执行失败: {reason}"
        super().__init__(message, error_code=5303, http_status_code=500)


class AuthException(BaseException):
    """认证授权异常基类"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 2000,
        http_status_code: int = 401,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, error_code, http_status_code, details)


class AuthenticationException(AuthException):
    """认证异常"""
    
    def __init__(self, message: str = "认证失败"):
        super().__init__(message, error_code=2001, http_status_code=401)


class TokenExpiredException(AuthException):
    """令牌过期异常"""
    
    def __init__(self, message: str = "令牌已过期"):
        super().__init__(message, error_code=2002, http_status_code=401)


class TokenInvalidException(AuthException):
    """令牌无效异常"""
    
    def __init__(self, message: str = "令牌无效"):
        super().__init__(message, error_code=2003, http_status_code=401)


class AuthorizationException(AuthException):
    """授权异常"""
    
    def __init__(self, message: str = "权限不足"):
        super().__init__(message, error_code=3000, http_status_code=403)


class PermissionDeniedException(AuthorizationException):
    """权限拒绝异常"""
    
    def __init__(self, resource: str, action: str):
        message = f"没有权限执行操作: {action} on {resource}"
        super().__init__(message, error_code=3001, http_status_code=403)


class ParameterException(BaseException):
    """参数异常基类"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 4000,
        http_status_code: int = 400,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, error_code, http_status_code, details)


class MissingParameterException(ParameterException):
    """缺少参数异常"""
    
    def __init__(self, parameter_name: str):
        message = f"缺少必要参数: {parameter_name}"
        super().__init__(message, error_code=4001, http_status_code=400)


class InvalidParameterException(ParameterException):
    """无效参数异常"""
    
    def __init__(self, parameter_name: str, reason: str = ""):
        message = f"参数 {parameter_name} 无效" + (f": {reason}" if reason else "")
        super().__init__(message, error_code=4002, http_status_code=400)


class DatabaseException(BaseException):
    """数据库异常基类"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 6000,
        http_status_code: int = 500,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, error_code, http_status_code, details)


class DatabaseConnectionException(DatabaseException):
    """数据库连接异常"""
    
    def __init__(self, message: str = "数据库连接失败"):
        super().__init__(message, error_code=6001, http_status_code=503)


class DatabaseQueryException(DatabaseException):
    """数据库查询异常"""
    
    def __init__(self, message: str = "数据库查询失败"):
        super().__init__(message, error_code=6002, http_status_code=500)


class DatabaseWriteException(DatabaseException):
    """数据库写入异常"""
    
    def __init__(self, message: str = "数据库写入失败"):
        super().__init__(message, error_code=6003, http_status_code=500)


class CacheException(BaseException):
    """缓存异常基类"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 7000,
        http_status_code: int = 500,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, error_code, http_status_code, details)


class CacheConnectionException(CacheException):
    """缓存连接异常"""
    
    def __init__(self, message: str = "缓存连接失败"):
        super().__init__(message, error_code=7001, http_status_code=503)


class CacheOperationException(CacheException):
    """缓存操作异常"""
    
    def __init__(self, message: str = "缓存操作失败"):
        super().__init__(message, error_code=7002, http_status_code=500)


class ExternalServiceException(BaseException):
    """外部服务异常基类"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 8000,
        http_status_code: int = 502,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, error_code, http_status_code, details)


class ExternalServiceUnavailableException(ExternalServiceException):
    """外部服务不可用异常"""
    
    def __init__(self, service_name: str):
        message = f"外部服务 {service_name} 不可用"
        super().__init__(message, error_code=8001, http_status_code=503)


class ExternalServiceTimeoutException(ExternalServiceException):
    """外部服务超时异常"""
    
    def __init__(self, service_name: str, timeout: int):
        message = f"外部服务 {service_name} 响应超时（{timeout}秒）"
        super().__init__(message, error_code=8002, http_status_code=504)


class SystemException(BaseException):
    """系统级异常基类"""
    
    def __init__(
        self,
        message: str,
        error_code: int = 1000,
        http_status_code: int = 500,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message, error_code, http_status_code, details)


class InternalServerException(SystemException):
    """内部服务器异常"""
    
    def __init__(self, message: str = "内部服务器错误"):
        super().__init__(message, error_code=1001, http_status_code=500)


class ResourceNotFoundException(SystemException):
    """资源不存在异常"""
    
    def __init__(self, resource: str):
        message = f"资源 {resource} 不存在"
        super().__init__(message, error_code=1002, http_status_code=404)


class RateLimitException(BaseException):
    """限流异常"""
    
    def __init__(self, message: str = "请求频率过高，请稍后再试"):
        super().__init__(message, error_code=9000, http_status_code=429)


class DistributedLockException(BaseException):
    """分布式锁异常"""
    
    def __init__(self, message: str = "获取分布式锁失败"):
        super().__init__(message, error_code=9100, http_status_code=429)


class IdempotentException(BaseException):
    """幂等性异常"""
    
    def __init__(self, message: str = "幂等性处理失败"):
        super().__init__(message, error_code=9200, http_status_code=500)
