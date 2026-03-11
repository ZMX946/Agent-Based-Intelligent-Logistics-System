import re
import json
from typing import Any, Dict, List, Optional
import os

class DataMasker:
    """数据脱敏工具，用于防止敏感信息泄露"""
    
    def __init__(self):
        # 敏感字段列表
        self.sensitive_fields = [
            'password', 'pwd', 'passwd', 'secret', 'token', 'key', 
            'api_key', 'apikey', 'access_token', 'refresh_token',
            'authorization', 'auth', 'credential', 'credentials',
            'private_key', 'public_key', 'certificate', 'cert',
            'session_id', 'session_key', 'cookie', 'cookies',
            'user_id', 'userid', 'username', 'user', 'email',
            'phone', 'mobile', 'telephone', 'address', 'id_card',
            'ssn', 'credit_card', 'bank_account', 'account'
        ]
        
        # 敏感模式
        self.sensitive_patterns = [
            # JWT Token
            r'eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+',
            # API Key
            r'[a-zA-Z0-9]{32,}',
            # 密码字段
            r'["\']?(password|pwd|passwd|secret|token|key)["\']?\s*[:=]\s*["\']?[^"\'\s,}]+["\']?',
            # 手机号
            r'1[3-9]\d{9}',
            # 邮箱
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            # 身份证号
            r'\d{17}[\dXx]',
            # IP地址
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}',
            # 文件路径
            r'[a-zA-Z]:\\[^"\'\s,}]+|/[^"\'\s,}]+',
            # URL
            r'https?://[^\s"\'<>,}]+'
        ]
    
    def mask_value(self, value: Any, mask_type: str = 'default') -> str:
        """
        对单个值进行脱敏
        
        Args:
            value: 需要脱敏的值
            mask_type: 脱敏类型 (default, partial, length, type)
        
        Returns:
            脱敏后的字符串
        """
        if value is None:
            return 'null'
        
        str_value = str(value)
        
        if mask_type == 'default':
            return '******'
        elif mask_type == 'partial':
            if len(str_value) <= 4:
                return '****'
            elif len(str_value) <= 8:
                return str_value[:2] + '****'
            else:
                return str_value[:3] + '****' + str_value[-3:]
        elif mask_type == 'length':
            if len(str_value) <= 50:
                return '****'
            else:
                return str_value[:50] + '...'
        elif mask_type == 'type':
            return 'SensitiveData'
        else:
            return '******'
    
    def mask_dict(self, data: Dict[str, Any], deep: bool = True) -> Dict[str, Any]:
        """
        对字典进行脱敏
        
        Args:
            data: 需要脱敏的字典
            deep: 是否深度递归
        
        Returns:
            脱敏后的字典
        """
        if not isinstance(data, dict):
            return data
        
        masked_data = {}
        for key, value in data.items():
            # 检查是否是敏感字段
            is_sensitive = any(
                sensitive_field in key.lower() 
                for sensitive_field in self.sensitive_fields
            )
            
            if is_sensitive:
                # 根据字段类型选择脱敏方式
                if any(field in key.lower() for field in ['password', 'pwd', 'secret', 'key']):
                    masked_data[key] = self.mask_value(value, 'default')
                elif any(field in key.lower() for field in ['phone', 'mobile', 'telephone']):
                    masked_data[key] = self.mask_value(value, 'partial')
                elif any(field in key.lower() for field in ['email', 'address']):
                    masked_data[key] = self.mask_value(value, 'partial')
                else:
                    masked_data[key] = self.mask_value(value, 'default')
            elif deep and isinstance(value, (dict, list)):
                if isinstance(value, dict):
                    masked_data[key] = self.mask_dict(value, deep)
                elif isinstance(value, list):
                    masked_data[key] = self.mask_list(value, deep)
                else:
                    masked_data[key] = value
            else:
                masked_data[key] = value
        
        return masked_data
    
    def mask_list(self, data: List[Any], deep: bool = True) -> List[Any]:
        """
        对列表进行脱敏
        
        Args:
            data: 需要脱敏的列表
            deep: 是否深度递归
        
        Returns:
            脱敏后的列表
        """
        if not isinstance(data, list):
            return data
        
        masked_list = []
        for item in data:
            if deep and isinstance(item, (dict, list)):
                if isinstance(item, dict):
                    masked_list.append(self.mask_dict(item, deep))
                elif isinstance(item, list):
                    masked_list.append(self.mask_list(item, deep))
                else:
                    masked_list.append(item)
            else:
                masked_list.append(item)
        
        return masked_list
    
    def mask_string(self, text: str) -> str:
        """
        对字符串进行模式匹配脱敏
        
        Args:
            text: 需要脱敏的字符串
        
        Returns:
            脱敏后的字符串
        """
        if not isinstance(text, str):
            return text
        
        masked_text = text
        for pattern in self.sensitive_patterns:
            masked_text = re.sub(pattern, '******', masked_text, flags=re.IGNORECASE)
        
        return masked_text
    
    def mask_error_message(self, error_message: str) -> str:
        """
        对错误消息进行脱敏，防止泄露敏感信息
        
        Args:
            error_message: 错误消息
        
        Returns:
            脱敏后的错误消息
        """
        if not isinstance(error_message, str):
            return str(error_message)
        
        # 移除文件路径
        masked = re.sub(r'[a-zA-Z]:\\[^"\'\s,}]+|/[^"\'\s,}]+', '[PATH]', masked := error_message)
        
        # 移除IP地址
        masked = re.sub(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', '[IP]', masked)
        
        # 移除端口号
        masked = re.sub(r':\d{4,5}', '[PORT]', masked)
        
        # 移除敏感字段值
        masked = re.sub(r'["\']?(password|pwd|secret|token|key)["\']?\s*[:=]\s*["\']?[^"\'\s,}]+["\']?', 
                        r'\1=******', masked, flags=re.IGNORECASE)
        
        return masked
    
    def mask_traceback(self, traceback_str: str) -> str:
        """
        对堆栈跟踪进行脱敏
        
        Args:
            traceback_str: 堆栈跟踪字符串
        
        Returns:
            脱敏后的堆栈跟踪（仅保留关键信息）
        """
        if not isinstance(traceback_str, str):
            return str(traceback_str)
        
        lines = traceback_str.split('\n')
        masked_lines = []
        
        for line in lines:
            # 脱敏文件路径
            line = re.sub(r'[a-zA-Z]:\\[^"\'\s,}]+|/[^"\'\s,}]+', '[PATH]', line)
            
            # 脱敏IP地址
            line = re.sub(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', '[IP]', line)
            
            masked_lines.append(line)
        
        return '\n'.join(masked_lines)
    
    def mask_response_data(self, data: Any) -> Any:
        """
        对响应数据进行脱敏
        
        Args:
            data: 响应数据
        
        Returns:
            脱敏后的响应数据
        """
        if isinstance(data, dict):
            return self.mask_dict(data, deep=True)
        elif isinstance(data, list):
            return self.mask_list(data, deep=True)
        elif isinstance(data, str):
            return self.mask_string(data)
        else:
            return data


class SensitiveDataFilter:
    """敏感数据过滤器，用于日志和错误信息"""
    
    def __init__(self):
        self.masker = DataMasker()
        self.enabled = os.getenv('ENABLE_DATA_MASKING', 'true').lower() == 'true'
    
    def filter_log(self, message: str) -> str:
        """
        过滤日志消息中的敏感数据
        
        Args:
            message: 日志消息
        
        Returns:
            过滤后的日志消息
        """
        if not self.enabled:
            return message
        
        return self.masker.mask_string(message)
    
    def filter_error(self, error: Exception, include_traceback: bool = False) -> Dict[str, Any]:
        """
        过滤异常信息中的敏感数据
        
        Args:
            error: 异常对象
            include_traceback: 是否包含堆栈跟踪
        
        Returns:
            过滤后的错误信息字典
        """
        if not self.enabled:
            return {
                'error_type': type(error).__name__,
                'error_message': str(error),
                'traceback': traceback.format_exc() if include_traceback else None
            }
        
        error_info = {
            'error_type': type(error).__name__,
            'error_message': self.masker.mask_error_message(str(error))
        }
        
        if include_traceback:
            import traceback
            error_info['traceback'] = self.masker.mask_traceback(traceback.format_exc())
        
        return error_info
    
    def filter_request_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        过滤请求数据中的敏感字段
        
        Args:
            data: 请求数据
        
        Returns:
            过滤后的请求数据
        """
        if not self.enabled:
            return data
        
        return self.masker.mask_dict(data, deep=True)
    
    def filter_response_data(self, data: Any) -> Any:
        """
        过滤响应数据中的敏感字段
        
        Args:
            data: 响应数据
        
        Returns:
            过滤后的响应数据
        """
        if not self.enabled:
            return data
        
        return self.masker.mask_response_data(data)


# 全局实例
data_masker = DataMasker()
sensitive_data_filter = SensitiveDataFilter()
