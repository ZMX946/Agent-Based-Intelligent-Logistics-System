from customize_milvus_wrapper import CustomizeMilvus
from models import LargeLanguageModel, RemoteEmbeddingModel
from prompt import PromptModelHub, QwenModelPromptHub, create_prompt_hub
from tools import ToolManager
from utils import logger
import traceback

class ApiSelectionHub:

    def __init__(self, milvus_uri, model_path, milvus_db_name, model, temperature, top_p,
            mongo_host, mongo_db, mongo_port, api_url, api_key):
        """
        ApiSelectionHub 类的初始化方法，用于初始化各类工具和模型实例。
        参数:
            uri (str): Milvus 数据库的连接 URI。
            model_path (str): 模型文件的路径。
            db_name (str): Milvus 数据库的名称。
            model (str): 大语言模型的名称。
            temperature (float): 大语言模型生成文本时的温度参数，控制生成文本的随机性。
            top_p (float): 大语言模型生成文本时的核采样参数。
            host (str): 工具管理服务的主机地址。
            db (str): 工具管理服务使用的数据库名称。
            port (int): 工具管理服务的端口号。
        """
        self.milvus = CustomizeMilvus(milvus_uri, milvus_db_name)
        self.LargeLanguageModel = LargeLanguageModel(api_url, api_key)
        self.PromptModelHub = create_prompt_hub(model)
        self.ToolManager = ToolManager(mongo_host, mongo_db, mongo_port, milvus_uri, milvus_db_name)
        self.model = model
        self.temperature = temperature
        self.top_p = top_p

    #TODO required_argument的作用是什么？
    def get_tool_coarse_and_fine(self, task_desc, required_argument, topK):
        """
        根据任务描述选择合适的工具。
        该函数接收一个查询语句、可选的必需参数和返回工具数量，通过与 Milvus 数据库交互获取相关工具，
        利用大语言模型生成选择结果，最后对结果进行后处理得到最终选择的工具。
        参数:
            task_desc (str): 查询语句，用于检索合适的工具。
            required_argument (str): 必需参数，如果为 None，则使用普通的工具选择逻辑；否则使用带必需参数的选择逻辑。
            topK (int): 从 Milvus 数据库中获取的工具数量。
        返回:
            object: 经过大语言模型选择并后处理后的最终工具对象。
        """
        try:
            # 向量检索获取候选工具（增加检索数量以确保不遗漏相关工具）
            logger.info(f"[向量检索] 查询: {task_desc}")
            tool_ids = self.milvus.get_docs("tools", task_desc, topK * 2)
            vector_search_tools = self.ToolManager.get_tools_by_ids(tool_ids)

            # 打印向量检索结果
            logger.info(f"[向量检索] 候选工具数量: {len(vector_search_tools)}")
            for i, tool in enumerate(vector_search_tools):
                logger.debug(f"[向量检索] 工具 {i+1}: [{tool.tool_id}] {tool.operationId}-{tool.name_for_human}")

            # 使用重排序模型对候选工具进行重排序（增加返回数量）
            reranked_tools = self.ToolManager.search_tools_with_rerank(task_desc, top_k=topK * 2, final_top_n=topK)

            # 打印重排序结果
            logger.info(f"[重排序] 查询: {task_desc}")
            logger.info(f"[重排序] 重排序后工具数量: {len(reranked_tools)}")
            for i, tool in enumerate(reranked_tools):
                logger.debug(f"[重排序] 工具 {i+1}: [{tool.tool_id}] {tool.operationId}-{tool.name_for_human}")

            # 选择工具列表
            tools = reranked_tools if reranked_tools else vector_search_tools
            
            # 如果仍然没有工具，直接从MongoDB获取所有工具
            if not tools:
                logger.warning(f"[向量检索和重排序] 未找到工具，尝试从MongoDB获取所有工具")
                tools = self.ToolManager.get_raw_all_tools()
                logger.info(f"[MongoDB] 获取到工具数量: {len(tools)}")

            if required_argument is None:
                # 使用重排序后的工具
                prompt = self.PromptModelHub.gen_tool_selection_prompt(task_desc, tools)
                model_output = self.LargeLanguageModel.chat_completions(prompt, self.model, self.temperature, self.top_p)
                final_tool = self.PromptModelHub.post_process_tool_selection_result(model_output, tools)
            else:
                # 使用重排序后的工具
                prompt = self.PromptModelHub.gen_required_argument_tool_selection_prompt(task_desc, required_argument, tools)
                model_output = self.LargeLanguageModel.chat_completions(prompt, self.model, self.temperature, self.top_p)
                final_tool = self.PromptModelHub.post_process_tool_selection_result(model_output, tools)
            logger.info(f"查询: [{task_desc}]被选择工具 : [{final_tool.tool_id}] {final_tool.operationId}-{final_tool.name_for_human}")
        except Exception as e:
            logger.error(f"检索并选择工具[{task_desc}：{required_argument}]失败: {e}\n{traceback.format_exc()}")
            return None

        return final_tool


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    load_dotenv("../.env.local", override=True)
    milvus_uri = os.getenv("milvus_uri", "http://localhost:19530")
    logger.info(milvus_uri)
    model_path = os.getenv("model_path", "model")
    milvus_db_name = os.getenv("milvus_db_name", "tool_db")
    model = os.getenv("model", "qwen-max-0919")
    temperature = float(os.getenv("temperature", "0.01"))
    top_p = float(os.getenv("top_p", "0.01"))
    mongo_host = os.getenv("mongo_host", "127.0.0.1")
    mongo_db = os.getenv("mongo_db", "tools")
    mongo_port = int(os.getenv("mongo_port", "27017"))
    topK = int(os.getenv("topK", "5"))
    api_key = os.getenv("api_key", "")
    base_url = os.getenv("base_url", "")
    apiSelectionHub = ApiSelectionHub(milvus_uri, model_path, milvus_db_name, model, temperature, top_p,
                                      mongo_host, mongo_db, mongo_port, base_url,api_key)
    tool = apiSelectionHub.get_tool_coarse_and_fine("查询苹果产品信息", None, topK)
