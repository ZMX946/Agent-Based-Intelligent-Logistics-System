import json
import time
from typing import List

from mongoengine import *
import threading
from cachetools import TTLCache
import yaml
from utils import logger, model_api_key

from customize_milvus_wrapper import CustomizeMilvus
from entity import Parameter, Tool

# 本地重排序模型导入
# from models.reranker_model import get_reranker_instance
# 导入通义千问重排序模型
from models.qwen_reranker_model import get_qwen_reranker_instance
import os
from mongoengine import connect, disconnect, get_connection
from dotenv import load_dotenv
load_dotenv()


class ToolManager:
    def __init__(self, mongo_host, mongo_db, mongo_port, milvus_uri, milvus_db_name):
        try:
            # 检查是否已有连接
            existing_connection = get_connection()
            if existing_connection:
                # 复用现有连接
                self.mongoClient = existing_connection
            else:
                # 创建新连接
                self.mongoClient = connect(mongo_db, host=mongo_host, port=mongo_port)
        except Exception as e:
            logger.warning(f"检查ToolManager的已有MongoDB连接: {e}，已重建连接 。")
            self.mongoClient = connect(mongo_db, host=mongo_host, port=mongo_port)
        self.db_name = mongo_db  # 保存数据库名称
        self.cache_lock = threading.Lock()
        self.tool_cache = TTLCache(maxsize=100, ttl=3600)
        
        # 初始化Milvus，允许降级
        try:
            self.milvus = CustomizeMilvus(milvus_uri, milvus_db_name)
            self.milvus_available = True
            logger.info("Milvus连接成功")
        except Exception as e:
            logger.warning(f"Milvus连接失败: {e}，部分功能可能不可用")
            self.milvus = None
            self.milvus_available = False
        
        # 初始化重排序模型，允许降级
        try:
            if model_api_key:
                self.reranker = get_qwen_reranker_instance(model_api_key)
                self.reranker_available = True
                logger.info("重排序模型初始化成功")
            else:
                logger.warning("缺少model_api_key，重排序功能不可用")
                self.reranker = None
                self.reranker_available = False
        except Exception as e:
            logger.warning(f"重排序模型初始化失败: {e}，重排序功能不可用")
            self.reranker = None
            self.reranker_available = False

    def delete_all_tools(self):
        """
        工具删除方法。该方法根据提供的工具 ID，从数据库中删除对应的工具。
        如果工具 ID 为空，则返回 None。
        参数:
            tool_id (int): 工具 ID，用于唯一标识要删除的工具。
        返回:
            None
        """
        Tool.objects.delete()
        self.milvus.drop_collection(collection_name="tools")
        self.clear_cache()

    def get_raw_all_tools(self):
        return Tool.objects.all()

    def get_all_tools(self):
        tools = Tool.objects.all()
        results = []
        index = 1
        for tool in tools:
            arguments = []
            for chat_parameter in tool.request_body:
                arguments.append({
                    "name": chat_parameter.name,
                    "description": chat_parameter.description,
                    "schema": {
                        "type": chat_parameter.type,
                        "format": chat_parameter.format,
                        "enum": chat_parameter.enum,
                    }
                })
            requestBody = json.dumps(arguments, ensure_ascii=False)
            results.append({
                "key": tool.tool_id,
                "index": index,
                "name": tool.name_for_human,
                "description": tool.description,
                "params": requestBody,
                "method": tool.method
            })
            index += 1
        return results

    def delete_tools(self,tool_ids):
        tools = self.get_tools_by_ids(tool_ids)
        for tool in tools:
            Tool.objects(tool_id=tool.tool_id).delete()
        self.milvus.delete_tools(tools)
        self.clear_cache()

    # def max_id(self):
    #     with self.cache_lock:
    #         tools = Tool.objects().all()
    #         max_target_id = -1
    #         for tool in tools:
    #             if tool.tool_id > max_target_id:
    #                 max_target_id = tool.tool_id
    #         if max_target_id == -1:
    #             return 0
    #         else:
    #             return max_target_id

    def get_next_tool_id(self):
        db = self.mongoClient[self.db_name]
        # 使用findAndModify原子操作
        counter = db.counters.find_one_and_update(
            {"_id": "tool_id"},
            {"$inc": {"sequence_value": 1}},
            upsert=True,
            return_document=True
        )
        return counter["sequence_value"]

    def insert_tools(self, tools: List[Tool]):
        new_tools = []
        for tool in tools:
            tool.tool_id = self.get_next_tool_id()
            tool.save()
            new_tools.append(tool)
        self.milvus.insert_tools("tools", new_tools)

        return tools

    def clear_cache(self):
        """
        工具缓存清除方法。该方法清除工具缓存中的所有工具。
        """
        with self.cache_lock:
            tool_ids = self.tool_cache.keys()
            for tool_id in tool_ids:
                self.tool_cache.pop(tool_id, None)


    def get_tools_by_ids_from_mongo(self, tool_ids: List[int]):
        """
        工具 ID 列表查询方法。该方法根据提供的工具 ID 列表，从数据库中查询对应的工具对象。
        如果工具 ID 列表为空，则返回空列表。
        参数:
            tool_ids (List[int]): 工具 ID 列表，用于唯一标识要查询的工具。
        返回:
            List[Tool]: 工具对象列表。
        """
        tools = []
        for tool_id in tool_ids:
            try:
                tool = Tool.objects.get(tool_id=tool_id)
                tools.append(tool)
            except:
                logger.info(f"User with id {tool_id} does not exist.")
                continue
        return tools



    def get_tools_by_ids(self, tool_ids: List[int]) -> List[Tool]:
        """
        工具 ID 列表查询方法。该方法根据提供的工具 ID 列表，从数据库中查询对应的工具对象。
        如果工具 ID 列表为空，则返回空列表。
        参数:
            tool_ids (List[int]): 工具 ID 列表，用于唯一标识要查询的工具。
        返回:
            List[Tool]: 工具对象列表。
        """
        if len(tool_ids) == 0:
            return []
        with self.cache_lock:
            cached_tools = [self.tool_cache.get(pid) for pid in tool_ids]

        #进行缓存更新，将缓存中不存在的工具添加到缓存中
        missing_tool_ids = [tool_id for tool_id, tool in zip(tool_ids, cached_tools) if tool is None]
        cached_tools = [tool for tool in cached_tools if tool is not None]
        if missing_tool_ids:
            data = self.get_tools_by_ids_from_mongo(missing_tool_ids)
            for tool in data:
                with self.cache_lock:
                    self.tool_cache[tool.tool_id] = tool

                cached_tools.append(tool)
        return cached_tools



    def get_tools_by_operationIds(self, operationIds) -> List[Tool]:
        """
        工具 operationId 列表查询方法。该方法根据提供的工具 operationId 列表，从数据库中查询对应的工具对象。
        如果工具 operationId 列表为空，则返回空列表。
        参数:
            operationIds (List[int]): 工具 operationId 列表，用于唯一标识要查询的工具。
        返回:
            List[Tool]: 工具对象列表。
        """
        tools = []
        for operationId in operationIds:
            try:
                tool = Tool.objects.get(operationId=operationId)
                tools.append(tool)
            except:
                logger.info(f"User with id {operationId} does not exist.")
                continue
        return tools



    def upload_file(self, filename):
        """
        工具文件上传方法。该方法根据提供的工具文件，将工具上传到数据库中。
        如果工具文件为空，则返回 None。
        参数:
            filename (str): 工具文件路径，用于唯一标识要上传的工具。
        返回:
            None
        """
        try:
            # 读取 JSON 文件
            with open(filename, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
            # 检查必要的字段
            if "components" not in data or "schemas" not in data["components"]:
                logger.error(f"文件 {filename} 缺少必要的 components/schemas 部分")
                return []
            
            if "servers" not in data or len(data["servers"]) == 0:
                logger.error(f"文件 {filename} 缺少必要的 servers 部分")
                return []
            
            if "paths" not in data:
                logger.error(f"文件 {filename} 缺少必要的 paths 部分")
                return []
            
            schemas = data["components"]["schemas"]
            url = data["servers"][0]["url"]
            tools = []
            index = 0
            
            for path in data["paths"]:
                for method in data["paths"][path]:
                    index += 1
                    api_information = data["paths"][path][method]
                    
                    # 检查必要的字段
                    if "summary" not in api_information:
                        logger.warning(f"路径 {path} 方法 {method} 缺少 summary 字段，跳过")
                        continue
                    
                    if "description" not in api_information:
                        logger.warning(f"路径 {path} 方法 {method} 缺少 description 字段，使用默认值")
                        api_information["description"] = api_information["summary"]
                    
                    # 提取 operationId
                    operationIds = path.split('/')
                    operationId = None
                    for i in range(len(operationIds)):
                        if "{" in operationIds[len(operationIds) - 1 - i] and "}" in operationIds[
                            len(operationIds) - 1 - i]:
                            continue
                        else:
                            operationId = operationIds[len(operationIds) - 1 - i]
                            break
                    
                    if not operationId:
                        logger.warning(f"路径 {path} 方法 {method} 无法提取 operationId，使用默认值")
                        operationId = f"operation_{index}"
                    
                    name_for_human = api_information["summary"]
                    name_for_model = "tool" + str(index)
                    description = api_information["description"]
                    params = []
                    
                    # 处理 parameters
                    if "parameters" in api_information:
                        requestParams = api_information["parameters"]
                        for param in requestParams:
                            try:
                                param_name = param["name"]
                                param_description = param.get("description", "")
                                in_ = param["in"]
                                
                                if "schema" not in param:
                                    logger.warning(f"参数 {param_name} 缺少 schema 字段，跳过")
                                    continue
                                
                                schema = param["schema"]
                                if "type" in schema:
                                    if schema["type"] == "string":
                                        paramType = "string"
                                    elif schema["type"] == "array":
                                        paramType = "array"
                                    else:
                                        paramType = schema.get("format", schema["type"])
                                else:
                                    paramType = "string"
                                
                                enum = []
                                if "enum" in schema:
                                    enum = schema["enum"]
                                
                                required = True
                                parameter = Parameter(
                                    name=param_name,
                                    type=paramType,
                                    description=param_description,
                                    enum=enum,
                                    required=required,
                                    in_=in_
                                )
                                params.append(parameter)
                            except Exception as e:
                                logger.error(f"处理参数 {param} 失败: {e}")
                                continue
                    
                    # 处理 requestBody
                    if "requestBody" in api_information:
                        try:
                            request_body = api_information["requestBody"]
                            if "content" in request_body and "application/json" in request_body["content"]:
                                content = request_body["content"]["application/json"]
                                if "schema" in content:
                                    schema = content["schema"]
                                    if "$ref" in schema:
                                        ref = schema["$ref"]
                                        schema_name = ref.split('/')[-1]
                                        if schema_name in schemas:
                                            schema_obj = schemas[schema_name]
                                            if "properties" in schema_obj:
                                                requestParams = schema_obj["properties"]
                                                for param_name, param_schema in requestParams.items():
                                                    try:
                                                        param_description = param_schema.get("description", "")
                                                        if "type" in param_schema:
                                                            if param_schema["type"] == "string":
                                                                paramType = "string"
                                                            elif param_schema["type"] == "array":
                                                                paramType = "array"
                                                            else:
                                                                paramType = param_schema.get("format", param_schema["type"])
                                                        else:
                                                            paramType = "string"
                                                        
                                                        enum = []
                                                        if "enum" in param_schema:
                                                            enum = param_schema["enum"]
                                                        
                                                        format = param_schema.get("format", paramType)
                                                        if len(enum) != 0:
                                                            format = "enum"
                                                        
                                                        required = True
                                                        parameter = Parameter(
                                                            name=param_name,
                                                            type=paramType,
                                                            description=param_description,
                                                            enum=enum,
                                                            required=required,
                                                            format=format,
                                                            in_="requestBody"
                                                        )
                                                        params.append(parameter)
                                                    except Exception as e:
                                                        logger.error(f"处理请求体参数 {param_name} 失败: {e}")
                                                        continue
                        except Exception as e:
                            logger.error(f"处理 requestBody 失败: {e}")
                            continue
                    
                    # 处理 isValidate
                    if "查询" in name_for_human or "获取" in name_for_human:
                        isValidate = False
                    else:
                        isValidate = True
                    
                    # 创建工具对象
                    try:
                        tool = Tool(
                            operationId=operationId,
                            name_for_human=name_for_human,
                            name_for_model=name_for_model,
                            description=description,
                            api_url=url,
                            isValidate=isValidate,
                            path=path,
                            method=method,
                            request_body=params
                        )
                        tools.append(tool)
                    except Exception as e:
                        logger.error(f"创建工具对象失败: {e}")
                        continue
            
            # 插入工具
            if tools:
                try:
                    self.insert_tools(tools)
                    logger.info(f"成功上传 {len(tools)} 个工具")
                except Exception as e:
                    logger.error(f"插入工具失败: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    # 即使嵌入失败，也要保存工具到数据库
                    for tool in tools:
                        try:
                            tool.tool_id = self.get_next_tool_id()
                            tool.save()
                        except Exception as save_error:
                            logger.error(f"保存工具 {tool.name_for_human} 失败: {save_error}")
                    logger.warning(f"工具已保存到数据库，但嵌入失败")
            else:
                logger.warning(f"文件 {filename} 中没有可上传的工具")
            
            return tools
        except Exception as e:
            logger.error(f"上传文件 {filename} 失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    def test_milvus(self,ids):
        return self.milvus.get_all_entity(ids)

    def search_tools_with_rerank(self, query, top_k=20, final_top_n=5):
        """
        使用重排序模型进行工具搜索
        Args:
            query (str): 查询文本
            top_k (int): 向量检索召回数量
            final_top_n (int): 最终返回结果数量
        Returns:
            list: 重排序后的工具列表
        """
        try:
            # 检查Milvus可用性
            if not self.milvus_available:
                logger.warning("Milvus不可用，跳过向量检索")
                return []
            
            # 检查重排序模型可用性
            if not self.reranker_available:
                logger.warning("重排序模型不可用，跳过重排序")
                return []
            
            # 1. 向量检索获取候选工具ID
            candidate_tool_ids = self.milvus.get_docs("tools", query, topk=top_k)
            
            # 2. 从MongoDB获取候选工具详细信息
            candidate_tools = self.get_tools_by_ids(candidate_tool_ids)
            
            # 3. 准备重排序的文本
            candidates_for_rerank = [f"{tool.name_for_human}: {tool.description}" for tool in candidate_tools]
            
            # 4. 重排序
            reranked_indices = self.reranker.rerank(query, candidates_for_rerank)
            
            # 5. 根据重排序结果整理最终工具列表
            final_tools = [candidate_tools[i] for i in reranked_indices]
            
            return final_tools[:final_top_n]
        except Exception as e:
            logger.error(f"重排序搜索失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

if __name__ == "__main__":
    toolManager = ToolManager('localhost', "tools", 27017,"http://127.0.0.1:19530","tool_db")
    toolManager.delete_all_tools()
    toolManager.upload_file("../api_data/dataset_apis.json")
    time.sleep(5)

    tools = Tool.objects.all()
    for tool in tools:
        logger.info(f"tool_id: {tool.tool_id}, tool_name: {tool.name_for_human}")
    # logger.info(f"tool_id: {tools[0].tool_id}, tool_name: {tools[0].name_for_human}")

    operation_tools = toolManager.get_tools_by_operationIds(["getByProductId"])
    logger.info(f"Operation tool_id: {operation_tools[0].tool_id}, tool_name: {operation_tools[0].name_for_human}")
    # toolManager.delete_all_tools()
    # upload_file("../api_data/dataset_apis.json")
