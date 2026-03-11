import traceback
import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
import json

from utils.exceptions import (
    BaseException,
    BusinessException,
    AuthException,
    ParameterException,
    DatabaseException,
    CacheException,
    ExternalServiceException,
    SystemException,
    RateLimitException,
    DistributedLockException,
    IdempotentException,
    InternalServerException
)
from utils.data_masker import sensitive_data_filter
from utils.logger_config import logger


class GlobalExceptionHandler:
    """全局异常处理器"""
    
    def __init__(self):
        self.masker = sensitive_data_filter
        self.enable_detailed_logging = True
        self.enable_request_id = True
    
    def generate_request_id(self) -> str:
        """生成请求ID"""
        return str(uuid.uuid4())
    
    def create_error_response(
        self,
        error_code: int,
        message: str,
        http_status_code: int,
        request_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        创建标准化的错误响应
        
        Args:
            error_code: 错误码
            message: 错误消息
            http_status_code: HTTP状态码
            request_id: 请求ID
            details: 详细信息
        
        Returns:
            错误响应字典
        """
        response = {
            'status': http_status_code,
            'error_code': error_code,
            'message': message,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if request_id:
            response['request_id'] = request_id
        
        if details:
            response['details'] = self.masker.filter_response_data(details)
        
        return response
    
    def log_exception(
        self,
        exception: Exception,
        request: Optional[Request] = None,
        request_id: Optional[str] = None,
        include_traceback: bool = True
    ):
        """
        记录异常信息
        
        Args:
            exception: 异常对象
            request: 请求对象
            request_id: 请求ID
            include_traceback: 是否包含堆栈跟踪
        """
        # 记录请求信息
        request_info = {}
        if request:
            request_info = {
                'method': request.method,
                'url': str(request.url),
                'path': request.url.path,
                'client': request.client.host if request.client else None
            }
        
        # 过滤敏感信息
        if request_info:
            request_info = self.masker.filter_request_data(request_info)
        
        # 构建日志消息
        log_message = f"Exception occurred"
        if request_id:
            log_message += f" [request_id={request_id}]"
        
        # 记录错误日志（包含完整信息）
        error_info = {
            'exception_type': type(exception).__name__,
            'exception_message': str(exception),
            'request_info': request_info
        }
        
        if include_traceback:
            error_info['traceback'] = traceback.format_exc()
        
        # 根据异常类型选择日志级别
        if isinstance(exception, (ParameterException, BusinessException)):
            logger.warning(f"{log_message}: {error_info}")
        elif isinstance(exception, (AuthException, RateLimitException)):
            logger.warning(f"{log_message}: {error_info}")
        elif isinstance(exception, (DatabaseException, CacheException, ExternalServiceException)):
            logger.error(f"{log_message}: {error_info}")
        else:
            logger.critical(f"{log_message}: {error_info}")
    
    def handle_base_exception(
        self,
        exception: BaseException,
        request: Optional[Request] = None
    ) -> JSONResponse:
        """
        处理基础异常
        
        Args:
            exception: 基础异常对象
            request: 请求对象
        
        Returns:
            JSON响应
        """
        request_id = self.generate_request_id() if self.enable_request_id else None
        
        # 记录异常
        self.log_exception(exception, request, request_id, include_traceback=False)
        
        # 创建错误响应
        response_data = self.create_error_response(
            error_code=exception.error_code,
            message=exception.message,
            http_status_code=exception.http_status_code,
            request_id=request_id,
            details=exception.details
        )
        
        return JSONResponse(
            content=response_data,
            status_code=exception.http_status_code
        )
    
    def handle_http_exception(
        self,
        exception: HTTPException,
        request: Optional[Request] = None
    ) -> JSONResponse:
        """
        处理HTTP异常
        
        Args:
            exception: HTTP异常对象
            request: 请求对象
        
        Returns:
            JSON响应
        """
        request_id = self.generate_request_id() if self.enable_request_id else None
        
        # 记录异常
        self.log_exception(exception, request, request_id, include_traceback=False)
        
        # 创建错误响应
        response_data = self.create_error_response(
            error_code=exception.status_code,
            message=str(exception.detail),
            http_status_code=exception.status_code,
            request_id=request_id
        )
        
        return JSONResponse(
            content=response_data,
            status_code=exception.status_code
        )
    
    def handle_validation_exception(
        self,
        exception: RequestValidationError,
        request: Optional[Request] = None
    ) -> JSONResponse:
        """
        处理请求验证异常
        
        Args:
            exception: 请求验证异常对象
            request: 请求对象
        
        Returns:
            JSON响应
        """
        request_id = self.generate_request_id() if self.enable_request_id else None
        
        # 记录异常
        self.log_exception(exception, request, request_id, include_traceback=False)
        
        # 提取验证错误信息
        validation_errors = []
        for error in exception.errors():
            error_info = {
                'field': '.'.join(str(loc) for loc in error['loc']),
                'message': error['msg'],
                'type': error['type']
            }
            validation_errors.append(error_info)
        
        # 创建错误响应
        response_data = self.create_error_response(
            error_code=4000,
            message="请求参数验证失败",
            http_status_code=422,
            request_id=request_id,
            details={'validation_errors': validation_errors}
        )
        
        return JSONResponse(
            content=response_data,
            status_code=422
        )
    
    def handle_generic_exception(
        self,
        exception: Exception,
        request: Optional[Request] = None
    ) -> JSONResponse:
        """
        处理通用异常
        
        Args:
            exception: 异常对象
            request: 请求对象
        
        Returns:
            JSON响应
        """
        request_id = self.generate_request_id() if self.enable_request_id else None
        
        # 记录异常（包含堆栈跟踪）
        self.log_exception(exception, request, request_id, include_traceback=True)
        
        # 创建错误响应（不暴露内部错误信息）
        response_data = self.create_error_response(
            error_code=1000,
            message="系统内部错误，请稍后重试",
            http_status_code=500,
            request_id=request_id
        )
        
        return JSONResponse(
            content=response_data,
            status_code=500
        )
    
    def handle_exception(self, request: Request, exc: Exception) -> JSONResponse:
        """
        统一异常处理入口
        
        Args:
            request: 请求对象
            exc: 异常对象
        
        Returns:
            JSON响应
        """
        try:
            # 基础异常
            if isinstance(exc, BaseException):
                return self.handle_base_exception(exc, request)
            
            # HTTP异常
            elif isinstance(exc, (HTTPException, StarletteHTTPException)):
                return self.handle_http_exception(exc, request)
            
            # 请求验证异常
            elif isinstance(exc, RequestValidationError):
                return self.handle_validation_exception(exc, request)
            
            # 通用异常
            else:
                return self.handle_generic_exception(exc, request)
        
        except Exception as e:
            # 异常处理器本身出错，记录并返回简单错误
            logger.critical(f"Exception handler failed: {e}\n{traceback.format_exc()}")
            return JSONResponse(
                content={
                    'status': 500,
                    'error_code': 1000,
                    'message': '系统内部错误',
                    'timestamp': datetime.utcnow().isoformat()
                },
                status_code=500
            )


# 全局异常处理器实例
global_exception_handler = GlobalExceptionHandler()


def setup_exception_handlers(app):
    """
    为FastAPI应用设置全局异常处理器
    
    Args:
        app: FastAPI应用实例
    """
    
    @app.exception_handler(BaseException)
    async def base_exception_handler(request: Request, exc: BaseException):
        return global_exception_handler.handle_base_exception(exc, request)
    
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return global_exception_handler.handle_http_exception(exc, request)
    
    @app.exception_handler(StarletteHTTPException)
    async def starlette_http_exception_handler(request: Request, exc: StarletteHTTPException):
        return global_exception_handler.handle_http_exception(exc, request)
    
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        return global_exception_handler.handle_validation_exception(exc, request)
    
    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception):
        return global_exception_handler.handle_generic_exception(exc, request)
    
    logger.info("全局异常处理器已设置")


def create_error_response(
    error_code: int,
    message: str,
    http_status_code: int = 500,
    details: Optional[Dict[str, Any]] = None
) -> JSONResponse:
    """
    创建错误响应的便捷函数
    
    Args:
        error_code: 错误码
        message: 错误消息
        http_status_code: HTTP状态码
        details: 详细信息
    
    Returns:
        JSON响应
    """
    response_data = {
        'status': http_status_code,
        'error_code': error_code,
        'message': message,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if details:
        response_data['details'] = sensitive_data_filter.filter_response_data(details)
    
    return JSONResponse(content=response_data, status_code=http_status_code)
