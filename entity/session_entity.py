from mongoengine import Document, StringField, ListField, DictField, IntField


class Session(Document):
    session_id = StringField(primary_key=True)  # 使用原来的task_id作为session_id
    user_id = StringField(required=True)  # 用户ID，关联到具体用户
    task_id = StringField(required=True)  # 原任务ID
    status = IntField()  # 任务的状态
    task_type = IntField()  # 任务的类型
    raw_query = StringField()  # 用户的最初查询请求
    changed_query = StringField()  # 查询请求，最初与raw_query一致，任务执行中间可能发生变化
    curr_task_desc = StringField()  # 任务的当前描述
    nodes = ListField(DictField())  # 前端界面调用链展示部分
    edges = ListField(DictField())  # 前端界面调用链展示部分
    graph_title = StringField()  # 前端界面调用链展示部分的标题
    system_output = StringField()  # 任务的结果文字输出
    curr_tool_id = IntField()  # 当前等待被确认的工具ID
    curr_tool_param = DictField()  # 当前等待被确认的工具ID的参数
    created_at = StringField(required=True)  # 任务创建时间
    updated_at = StringField(required=True)  # 任务更新时间
    migrated_at = StringField(required=True)  # 迁移到session库的时间

    def to_dict(self):
        """将 Session 对象转换为字典，前端页面使用"""
        return {
            'key': self.session_id,
            'task_id': self.task_id,
            'session_id': self.session_id,
            'status': self.status,
            'nodes': self.nodes,
            'edges': self.edges,
            'isSuccess': self.graph_title,
            'systemOutput': self.system_output,
            'rawQuery': self.raw_query,
            'userId': self.user_id,
            'createdAt': self.created_at,
            'updatedAt': self.updated_at
        }

    # 定义元数据，指定集合名称
    meta = {
        'collection': 'sessions'
    }
