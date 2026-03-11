from entity import Tool
from models import LargeLanguageModel
from prompt import PromptModelHub, QwenModelPromptHub, create_prompt_hub
from tools import ToolManager
from utils import logger

class ParamExtractionHub:
    """
    ParamExtractionHub 类的初始化方法。

    该方法用于初始化 ParamExtractionHub 类的实例，设置大语言模型、提示模型中心，
    并保存模型名称、温度和采样概率等参数。

    参数:
    model (str): 使用的大语言模型名称。
    temperature (float): 用于控制生成文本随机性的温度参数，值越高输出越随机。
    top_p (float): 核采样概率，用于控制生成文本时的词汇选择范围。

    属性:
    LargeLanguageModel (LargeLanguageModel): 大语言模型实例。
    PromptModelHub (PromptModelHub): 提示词中心实例。
    model (str): 使用的大语言模型名称。
    temperature (float): 温度参数。
    top_p (float): 核采样概率。
    """

    def __init__(self, model, temperature, top_p, api_url, api_key):
        self.LargeLanguageModel = LargeLanguageModel(api_url, api_key)
        self.PromptModelHub = create_prompt_hub(model)
        self.model = model
        self.temperature = temperature
        self.top_p = top_p


    def validate_params(self, tool: Tool, extractionParam):
        """
        验证提取的参数是否符合工具请求体的要求。
        此函数会遍历工具请求体中的每个目标参数，检查提取的参数是否缺失、为空，
        并验证参数类型是否符合要求，若不符合则尝试进行类型转换，转换失败则标记为缺失参数。
        参数:
            tool (Tool): 工具实例，包含请求体信息。
            extractionParam (dict): 提取的参数，键为参数名，值为参数值。
        返回:
            list: 缺失的参数列表，列表元素为 Tool 中定义的参数对象。

        代码流程逻辑:
            1. 初始化一个空列表 `missing_param` 用于存储缺失的参数。
            2. 遍历工具请求体中的每个目标参数。
            3. 检查参数是否为必需参数且未在提取的参数中，若是则添加到缺失列表。
            4. 检查参数值是否为空，若为空则添加到缺失列表。
            5. 对于 int64 或 int32 类型的参数，检查是否为 int 类型，若不是则尝试转换，转换失败则添加到缺失列表。
            6. 对于 double 类型的参数，检查是否为 float 类型，若不是则尝试转换，转换失败则添加到缺失列表。
            7. 返回缺失参数列表。
        """
        missing_param = []
        for target_param in tool.request_body:
            if target_param.name not in extractionParam and target_param.required:
                missing_param.append(target_param)
                logger.debug(f"已添加[{tool.tool_id} - {tool.operationId}] 缺失参数：{target_param.name}")
            else:
                if isinstance(extractionParam[target_param.name], str) and len(extractionParam[target_param.name]) == 0:
                    missing_param.append(target_param)
                    logger.debug(f"已添加[{tool.tool_id} - {tool.operationId}] 缺失参数：{target_param.name}")
                else:
                    if (target_param.type == "int64" or target_param.type == "int32") and not isinstance(
                            extractionParam[target_param.name], int):
                        try:
                            extractionParam[target_param.name] = int(extractionParam[target_param.name])
                        except:
                            extractionParam[target_param.name] = ""
                            missing_param.append(target_param)
                            logger.debug(f"已添加[{tool.tool_id} - {tool.operationId}] 缺失参数：{target_param.name}")
                    if target_param.type == "double" and not isinstance(extractionParam[target_param.name], float):
                        try:
                            extractionParam[target_param.name] = float(extractionParam[target_param.name])
                        except:
                            extractionParam[target_param.name] = ""
                            missing_param.append(target_param)
                            logger.debug(f"已添加[{tool.tool_id} - {tool.operationId}] 缺失参数：{target_param.name}")
        return missing_param


    def extraction_params(self, query, tool: Tool):
        """
        提取工具参数。此函数根据用户查询和工具信息，
        使用大语言模型生成提取参数的提示，并通过模型生成提取参数的结果。
        参数:
            query (str): 用户查询字符串。
            tool (Tool): 工具实例，包含请求体信息。
        返回:
            元组: (提取的参数<键为参数名，值为参数值>,经检查后缺失的参数)
        代码流程逻辑:
            1. 生成提取参数的提示。
            2. 调用大语言模型生成提取参数的结果。
            3. 后处理模型输出，提取参数。
            4. 返回提取的参数。
        """
        request_body = tool.request_body

        prompt = self.PromptModelHub.gen_get_all_parameters_prompt(query, request_body)
        model_output = self.LargeLanguageModel.chat_completions(prompt, self.model, self.temperature, self.top_p)
        logger.debug(f"提取工具参数模型输出：{model_output}")
        results = self.PromptModelHub.post_process_get_all_parameter_result(model_output, tool)
        missing_param = self.validate_params(tool, results)
        return results, missing_param


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    load_dotenv("../.env.local", override=True)
    milvus_uri = os.getenv("milvus_uri", "http://localhost:19530")
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
    paramExtractionHub = ParamExtractionHub(model=model, temperature=0.67, top_p=0.95,api_url=base_url,api_key=api_key)
    toolManager = ToolManager(mongo_host, mongo_db, mongo_port, milvus_uri, milvus_db_name)
    tool = toolManager.get_tools_by_ids([5])[0]
    results, missing_param = paramExtractionHub.extraction_params("请帮我查询名为苹果的产品的情况", tool)
    logger.info(f"{results}，{missing_param}")