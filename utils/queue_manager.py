import redis
import rq
from rq import Queue
from rq.worker import Worker
import time
import threading
from utils.logger_config import logger
from utils.redis_client import get_redis_client

class QueueManager:
    """
    队列管理器，用于管理项目中的各种任务队列
    """
    def __init__(self):
        self.redis_client = get_redis_client()
        self.redis_is_async = hasattr(self.redis_client, '_pool') and hasattr(self.redis_client, '_init_pool')
        
        # 如果是异步Redis客户端，我们需要获取同步客户端用于RQ
        if self.redis_is_async:
            # 使用同步Redis客户端连接
            self.sync_redis = redis.Redis(
                host=self.redis_client.host,
                port=self.redis_client.port,
                password=self.redis_client.password,
                db=self.redis_client.db,
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                retry_on_timeout=True
            )
        else:
            # 确保获取到正确的同步Redis客户端
            if hasattr(self.redis_client, 'client') and isinstance(self.redis_client.client, redis.Redis):
                self.sync_redis = self.redis_client.client
            elif hasattr(self.redis_client, '_client') and isinstance(self.redis_client._client, redis.Redis):
                self.sync_redis = self.redis_client._client
            else:
                # 直接创建一个新的redis.Redis实例，使用与redis_client相同的连接参数
                host = getattr(self.redis_client, 'host', 'localhost')
                port = getattr(self.redis_client, 'port', 6379)
                password = getattr(self.redis_client, 'password', None)
                db = getattr(self.redis_client, 'db', 0)
                
                self.sync_redis = redis.Redis(
                    host=host,
                    port=port,
                    password=password,
                    db=db,
                    decode_responses=True,
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0,
                    retry_on_timeout=True
                )
        
        # 创建不同优先级的队列
        self.high_priority_queue = Queue('high', connection=self.sync_redis)
        self.normal_queue = Queue('default', connection=self.sync_redis)
        self.low_priority_queue = Queue('low', connection=self.sync_redis)
        
        self.worker_thread = None
        self.is_worker_running = False
    
    def enqueue_task(self, func, *args, priority='default', **kwargs):
        """
        将任务添加到队列中
        
        :param func: 要执行的函数
        :param args: 函数参数
        :param priority: 任务优先级 ('high', 'default', 'low')
        :param kwargs: 函数关键字参数
        :return: 任务ID
        """
        try:
            if priority == 'high':
                job = self.high_priority_queue.enqueue(func, *args, **kwargs)
            elif priority == 'low':
                job = self.low_priority_queue.enqueue(func, *args, **kwargs)
            else:
                job = self.normal_queue.enqueue(func, *args, **kwargs)
            
            logger.info(f"任务已添加到队列，ID: {job.id}, 优先级: {priority}")
            return job.id
        except Exception as e:
            logger.error(f"添加任务到队列失败: {e}")
            return None
    
    def enqueue_with_delay(self, func, delay_seconds, *args, priority='default', **kwargs):
        """
        将任务添加到队列中，并延迟执行
        
        :param func: 要执行的函数
        :param delay_seconds: 延迟秒数
        :param args: 函数参数
        :param priority: 任务优先级 ('high', 'default', 'low')
        :param kwargs: 函数关键字参数
        :return: 任务ID
        """
        try:
            if priority == 'high':
                job = self.high_priority_queue.enqueue_in(timeout=time.sleep(delay_seconds), func=func, args=args, kwargs=kwargs)
            elif priority == 'low':
                job = self.low_priority_queue.enqueue_in(timeout=time.sleep(delay_seconds), func=func, args=args, kwargs=kwargs)
            else:
                job = self.normal_queue.enqueue_in(timeout=time.sleep(delay_seconds), func=func, args=args, kwargs=kwargs)
            
            logger.info(f"延迟任务已添加到队列，ID: {job.id}, 延迟: {delay_seconds}秒, 优先级: {priority}")
            return job.id
        except Exception as e:
            logger.error(f"添加延迟任务到队列失败: {e}")
            return None
    
    def get_job_status(self, job_id):
        """
        获取任务状态
        
        :param job_id: 任务ID
        :return: 任务状态
        """
        try:
            job = rq.job.Job.fetch(job_id, connection=self.sync_redis)
            return job.get_status()
        except Exception as e:
            logger.error(f"获取任务状态失败: {e}")
            return None
    
    def get_job_result(self, job_id):
        """
        获取任务结果
        
        :param job_id: 任务ID
        :return: 任务结果
        """
        try:
            job = rq.job.Job.fetch(job_id, connection=self.sync_redis)
            return job.result
        except Exception as e:
            logger.error(f"获取任务结果失败: {e}")
            return None
    
    def start_worker(self, queues=None):
        """
        启动队列 worker
        
        :param queues: 要监听的队列列表，如果为None则使用默认队列
        """
        if self.is_worker_running:
            logger.warning("Worker already running")
            return
        
        # 如果没有指定队列，则使用默认队列
        if queues is None:
            queues = [self.high_priority_queue, self.normal_queue, self.low_priority_queue]
        
        def run_worker():
            try:
                logger.info(f"Starting worker for queues: {[queue.name for queue in queues]}")
                worker = Worker(queues, connection=self.sync_redis)
                worker.work()
            except Exception as e:
                logger.error(f"Worker error: {e}")
                self.is_worker_running = False
        
        self.worker_thread = threading.Thread(target=run_worker, daemon=True)
        self.worker_thread.start()
        self.is_worker_running = True
        logger.info("Worker started in background thread")
    
    def stop_worker(self):
        """
        停止队列 worker
        """
        if not self.is_worker_running:
            logger.warning("Worker not running")
            return
        
        # RQ worker 会在处理完当前任务后退出
        logger.info("Worker will stop after current task")
        self.is_worker_running = False

# 全局队列管理器实例
def get_queue_manager():
    """
    获取队列管理器实例
    
    :return: QueueManager 实例
    """
    if not hasattr(get_queue_manager, '_instance'):
        get_queue_manager._instance = QueueManager()
    return get_queue_manager._instance

# 任务执行函数示例
def execute_task(task_func, *args, **kwargs):
    """
    执行任务的包装函数
    
    :param task_func: 要执行的函数
    :param args: 函数参数
    :param kwargs: 函数关键字参数
    :return: 函数执行结果
    """
    try:
        logger.info(f"开始执行任务: {task_func.__name__}")
        result = task_func(*args, **kwargs)
        logger.info(f"任务执行完成: {task_func.__name__}")
        return result
    except Exception as e:
        logger.error(f"任务执行失败: {task_func.__name__}, 错误: {e}")
        raise