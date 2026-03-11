import sys
import os
# 将项目根目录添加到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uuid
from mongoengine import *
import threading
from cachetools import TTLCache

from entity import Parameter, Tool, Task
from entity.session_entity import Session
from utils import logger, TASK_STATUS_INIT, TASK_STATUS_FINISH, TASK_INIT_TOOL_ID, TASK_TYPE_UNKNOWN, \
    TASK_TYPE_MAINTAIN, TASK_SYS_OUTPUT_STOP
import traceback


class TaskManager:
    """
    task实例和状态管理，提供task的生命周期管理方法，在数据库中新建任务、更新任务等。

    __init__ 方法获得访问数据库的连接和设置并发控制。
    """
    def __init__(self, mongo_host, mongo_db, mongo_port):
        self.mongoClient = connect(mongo_db, host=mongo_host, port=mongo_port)
        self.cache_lock = threading.Lock()

    def create_task(self, user_raw_query: str, user_id: str = "", exists_task_id: str = "") -> Task:
        """
        创建任务
        """
        from datetime import datetime
        current_time = datetime.now().isoformat()
        
        if not exists_task_id:
            new_task_status = TASK_STATUS_INIT
            system_output = "正在初始化任务......"
            while True:
                # 创建任务ID，并保证唯一性
                task_id = str(uuid.uuid4())
                task = Task.objects(task_id=task_id).first()
                if task is None:
                    break
        else:
            task_id = exists_task_id
            new_task_status = TASK_STATUS_FINISH
            system_output = TASK_SYS_OUTPUT_STOP + "该任务早已结束，请重新登录"
        task = Task()
        task.task_id = task_id
        task.user_id = user_id
        task.status = new_task_status
        task.task_type = TASK_TYPE_UNKNOWN
        task.raw_query = user_raw_query
        task.changed_query = user_raw_query
        task.curr_task_desc = ""
        task.edges = []
        task.nodes = []
        task.system_output = system_output
        task.curr_tool_id = TASK_INIT_TOOL_ID
        task.curr_tool_param = {}
        task.created_at = current_time
        task.updated_at = current_time
        task.save()
        return task

    def update_task_recorder(self, task_id: str, task_status: int, system_output: str, graph_title: str = "",
                            curr_task_desc="", task_type: int = TASK_TYPE_MAINTAIN, nodes: list = None,
                            edges: list = None, curr_tool_id: int = 0, curr_tool_param: dict = None,changed_query="") -> str:
        """
        更新任务表update_task
        """
        from datetime import datetime
        current_time = datetime.now().isoformat()
        
        try:
            task = Task.objects.get(task_id=task_id)
            logger.debug(f"准备更新表中的任务task= {task.task_id}: task_status={task.status},nodes={task.nodes}, edges={task.edges}, "
                        f"curr_task_desc={task.curr_task_desc},systemOutput={task.system_output}, graph_title={task.graph_title}, task_type={task.task_type},"
                        f"curr_tool_id={task.curr_tool_id}, curr_tool_param={task.curr_tool_param},changed_query={task.changed_query} ")
            logger.debug(f"更新为 task= {task_id}: task_status={task_status},nodes={nodes}, edges={edges}, "
                        f"curr_task_desc={curr_task_desc},systemOutput={system_output}, graph_title={graph_title}, task_type={task_type},"
                        f"curr_tool_id={curr_tool_id}, curr_tool_param={curr_tool_param},changed_query={changed_query} ")
            task.status = task_status
            task.system_output = system_output
            task.graph_title = graph_title
            task.updated_at = current_time
            if changed_query:
                task.changed_query = changed_query
            if curr_task_desc:
                task.curr_task_desc = curr_task_desc
            if task_type:
                task.task_type = task_type
            # 确保nodes始终是数组
            task.nodes = nodes if nodes is not None else []
            # 确保edges始终是数组
            task.edges = edges if edges is not None else []
            if curr_tool_id:
                task.curr_tool_id = curr_tool_id
            if curr_tool_param:
                task.curr_tool_param = curr_tool_param

            task.save()
            task_saved = Task.objects.get(task_id=task_id)
            logger.debug(f"更新后表中的任务task= {task_saved.task_id}: task_status={task_saved.status},nodes={task_saved.nodes}, edges={task_saved.edges}, "
                        f"curr_task_desc={task_saved.curr_task_desc},systemOutput={task_saved.system_output}, graph_title={task_saved.graph_title}, task_type={task_saved.task_type},"
                        f"curr_tool_id={task_saved.curr_tool_id}, curr_tool_param={task_saved.curr_tool_param},changed_query={task_saved.changed_query},"
                        f"updated_at={task_saved.updated_at}")
            return task_id
        #TODO DoesNotExist是个什么？
        except Task.DoesNotExist:
            logger.error(f"任务[{task_id}]不存在")
            # 可以选择创建新任务或返回错误
        except Exception as e:
            logger.error(f"任务[{task_id}]保存失败: {e}\n{traceback.format_exc()}")
            raise  e

    def get_task_by_id(self, task_id) -> Task| None:
        """
        通过task_id获得任务实体
        """
        try:
            task = Task.objects.get(task_id=task_id)
            return task
        except Task.DoesNotExist:
            return None
        except Exception as e:
            logger.error(f"获取任务[{task_id}]失败: {e}\n{traceback.format_exc()}")
            return None
            
    def get_user_tasks(self, user_id: str, limit: int = 100, days: int = 7) -> list:
        """
        获取用户的历史任务列表
        
        Args:
            user_id: 用户ID
            limit: 限制返回的任务数量
            days: 限制返回多少天内的任务
            
        Returns:
            任务列表
        """
        try:
            from datetime import datetime, timedelta
            import json
            
            # 计算时间范围
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            start_time_str = start_time.isoformat()
            
            # 查询用户的任务
            tasks = Task.objects(
                user_id=user_id,
                created_at__gte=start_time_str
            ).order_by('-created_at').limit(limit)
            
            # 转换为字典列表
            task_list = [task.to_dict() for task in tasks]
            return task_list
        except Exception as e:
            logger.error(f"获取用户[{user_id}]的任务列表失败: {e}\n{traceback.format_exc()}")
            return []
    
    def migrate_old_tasks_to_session(self, days: int = 7) -> int:
        """
        将超过指定天数的任务迁移到session库
        
        Args:
            days: 迁移超过多少天的任务
            
        Returns:
            迁移的任务数量
        """
        try:
            from datetime import datetime, timedelta
            
            # 计算时间范围
            end_time = datetime.now()
            cutoff_time = end_time - timedelta(days=days)
            cutoff_time_str = cutoff_time.isoformat()
            migrated_count = 0
            
            # 查询需要迁移的任务
            tasks = Task.objects(
                created_at__lt=cutoff_time_str
            )
            
            for task in tasks:
                try:
                    # 检查session库中是否已存在
                    existing_session = Session.objects(session_id=task.task_id).first()
                    if not existing_session:
                        # 创建session记录
                        session = Session(
                            session_id=task.task_id,
                            user_id=task.user_id,
                            task_id=task.task_id,
                            status=task.status,
                            task_type=task.task_type,
                            raw_query=task.raw_query,
                            changed_query=task.changed_query,
                            curr_task_desc=task.curr_task_desc,
                            nodes=task.nodes,
                            edges=task.edges,
                            graph_title=task.graph_title,
                            system_output=task.system_output,
                            curr_tool_id=task.curr_tool_id,
                            curr_tool_param=task.curr_tool_param,
                            created_at=task.created_at,
                            updated_at=task.updated_at,
                            migrated_at=datetime.now().isoformat()
                        )
                        session.save()
                        
                        # 从原库删除任务
                        task.delete()
                        migrated_count += 1
                except Exception as task_error:
                    logger.error(f"迁移任务[{task.task_id}]失败: {task_error}\n{traceback.format_exc()}")
            
            logger.info(f"成功迁移了{migrated_count}个任务到session库")
            return migrated_count
        except Exception as e:
            logger.error(f"任务迁移失败: {e}\n{traceback.format_exc()}")
            return 0
    
    def get_user_sessions(self, user_id: str, limit: int = 100, days: int = 30) -> list:
        """
        获取用户的历史会话列表
        
        Args:
            user_id: 用户ID
            limit: 限制返回的会话数量
            days: 限制返回多少天内的会话
            
        Returns:
            会话列表
        """
        try:
            from datetime import datetime, timedelta
            
            # 计算时间范围
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            start_time_str = start_time.isoformat()
            
            # 查询用户的会话
            sessions = Session.objects(
                user_id=user_id,
                created_at__gte=start_time_str
            ).order_by('-created_at').limit(limit)
            
            # 转换为字典列表
            session_list = [session.to_dict() for session in sessions]
            return session_list
        except Exception as e:
            logger.error(f"获取用户[{user_id}]的会话列表失败: {e}\n{traceback.format_exc()}")
            return []
    
    def get_user_history(self, user_id: str, limit: int = 100, days: int = 7) -> list:
        """
        获取用户的完整历史记录（包括最近7天的任务和更早的会话）
        
        Args:
            user_id: 用户ID
            limit: 限制返回的记录数量
            days: 限制返回多少天内的记录
            
        Returns:
            历史记录列表
        """
        try:
            from datetime import datetime, timedelta
            
            # 获取最近7天的任务
            recent_tasks = self.get_user_tasks(user_id, limit=limit, days=min(days, 7))
            
            # 如果需要获取超过7天的记录，从session库中获取
            if days > 7:
                older_sessions = self.get_user_sessions(user_id, 
                                                       limit=limit - len(recent_tasks), 
                                                       days=days - 7)
                return recent_tasks + older_sessions
            else:
                return recent_tasks
        except Exception as e:
            logger.error(f"获取用户[{user_id}]的历史记录失败: {e}\n{traceback.format_exc()}")
            return []

if __name__ == "__main__":
    taskManager = TaskManager('localhost', "tools", 27017)
    # 测试迁移功能
    migrated_count = taskManager.migrate_old_tasks_to_session(days=7)
    logger.info(f"迁移了{migrated_count}个任务到session库")
