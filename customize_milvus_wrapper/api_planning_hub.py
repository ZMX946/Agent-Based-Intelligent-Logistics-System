import json

from apis.api_selection_hub import ApiSelectionHub
from models import LargeLanguageModel
from param_extraction.param_extraction_hub import ParamExtractionHub
from tasks import TaskManager,GenerateTaskHub
from tools import ToolSummaryHub, ToolUseHub, ToolManager
from utils import logger, TASK_ERROR_CODE, TASK_SUCCESS_CODE, RESPONSE_STATUS_CODE_SUCCESS, TASK_STATUS_FINISH, \
    TASK_STATUS_RUNNING, TASK_STATUS_WAIT_CONFIRM, TASK_INIT_TOOL_ID, TASK_TYPE_SINGLE, TASK_TYPE_APIS, \
    TASK_TYPE_UNKNOWN, TASK_TYPE_MAINTAIN, GRAPH_TITLE_SUCESS, GRAPH_TITLE_FAILURE, TASK_SYS_OUTPUT_STOP
import traceback

from utils.config import api_result_max_length, api_result_max_threshold


class ApiPlanningHub:
    def __init__(self, milvus_uri, model_path, milvus_db_name, model, temperature, top_p,
                mongo_host, mongo_db, mongo_port, topK, api_url, api_key,executor):
        """
        初始化 ApiPlanningHub 类的实例。

        :param milvus_uri: 连接地址，通常用于指定服务的访问地址
        :param model_path: 模型文件的路径
        :param milvus_db_name: 数据库的名称
        :param model: 使用的LLM名称
        :param temperature: 采样温度，用于控制生成文本的随机性
        :param top_p: 核采样概率，用于控制生成文本的多样性
        :param mongo_host: 主机地址，通常用于数据库或服务的连接
        :param mongo_db: 数据库标识，可能用于指定具体的数据库
        :param mongo_port: 端口号，用于网络连接
        :param topK: 检索时返回的前 K 个结果
        """
        self.api_selection_hub = ApiSelectionHub(milvus_uri, model_path, milvus_db_name, model, temperature, top_p,
                                                mongo_host, mongo_db, mongo_port, api_url, api_key)
        self.topK = topK
        self.param_extraction_hub = ParamExtractionHub(model, temperature, top_p, api_url, api_key)
        self.tool_summary_hub = ToolSummaryHub(model, temperature, top_p, api_url, api_key)
        self.tool_use_hub = ToolUseHub("")
        self.generate_task_hub = GenerateTaskHub(model, temperature, top_p, api_url, api_key, mongo_host, mongo_db, mongo_port, milvus_uri, milvus_db_name)
        self.task_manager = TaskManager(mongo_host, mongo_db, mongo_port)
        self.tool_manager = ToolManager(mongo_host, mongo_db, mongo_port, milvus_uri, milvus_db_name)
        self.executor = executor
        self.llm = LargeLanguageModel(api_url, api_key)
        self.model = model
        self.temperature = temperature
        self.top_p = top_p

    def generate_output(self, text):
        prompt = " 请将以下句子进行润色成通顺的话，请直接输出结果：\n\n\n" + text
        print(prompt)
        result = self.llm.chat_completions(prompt, self.model, self.temperature, self.top_p)
        return result

    def _set_task_type(self, query, task_id, task_type, system_output):
        logger.debug(f"用户请求[{query}]生成任务[{task_id}]{system_output}")
        self.task_manager.update_task_recorder(task_id, TASK_STATUS_RUNNING, system_output, task_type=task_type)

    def _update_task_curr_desc(self,task_id,task_desc):
        self.task_manager.update_task_recorder(task_id, TASK_STATUS_RUNNING, "正在为您分析中，请稍等......",
                                                graph_title="正在为您分析中，请稍等......", curr_task_desc=task_desc)

    def _update_task_node_edge(self, task, task_new_result, system_output,is_end=False):
        """
        更新显示的知识图谱推理路径
        :param task: 当前任务的Task实体
        :param task_new_result: 任务的当前结果，结果是个字典，包含以下键值对
                {
                "code": ,
                "result": "",
                "tool": "",
                "missing_param": [],
                "param": [],
                "query": "",
                "task_description": ""
            }
        :param system_output: 输出的信息
        :param is_end: 当前任务是否结束
        """
        try:
            nodes = task.nodes
            sub_nodes = []
            edges = task.edges
            index = len(nodes)
            sub_index = index * 10000
            graph_title = GRAPH_TITLE_SUCESS
            # if system_output == "当前API无法实现用户需求":
            #     self.taskManager.update_task_recorder(task.task_id, TASK_STATUS_FINISH, system_output,  is_end,
            #                                         TASK_TYPE_MAINTAIN,[], [], "异常调用链")
            #     return
            if task_new_result["code"] != TASK_SUCCESS_CODE:
                self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_FINISH, TASK_SYS_OUTPUT_STOP+system_output, graph_title=GRAPH_TITLE_FAILURE)
                return
            nodes.append({
                "id": str(index),
                "name": str(index) + "_" + task_new_result["tool"],
                "label": task_new_result["tool"],
                "group": task_new_result["tool"],
                "task_description": task_new_result["query"],
                "params": json.dumps(task_new_result["param"], ensure_ascii=False),
                "result": json.dumps(task_new_result["result"], ensure_ascii=False),
            })
            if(len(nodes) > 1):
                edges.append(
                    {
                        "source": nodes[index-1]["id"], "target": nodes[index]["id"], "value": '正向API规划', "symbolSize": [5, 20],
                        "label": {"show": False}
                    }
                )
            for param_node in task_new_result["missing_param"]:
                sub_nodes.append({
                    "id": str(sub_index),
                    "name": str(sub_index) + "_" + param_node["tool"],
                    "label": param_node["tool"],
                    "group": param_node["tool"],
                    "task_description": param_node["task_description"],
                    "params": json.dumps(param_node["param"], ensure_ascii=False),
                    "result": json.dumps(param_node["result"], ensure_ascii=False),
                })
                edges.append(
                    {
                        "source": str(sub_index), "target": str(index), "value": '缺省参数逆向API规划',
                        "symbolSize": [5, 20],
                        "label": {"show": False}
                    }
                )
                sub_index += 1

            nodes = nodes + sub_nodes
        except Exception as e:
            logger.error(f"任务{task.task_id}图像绘制错误：{e}，但任务继续进行...\n{traceback.format_exc()}")
            self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_RUNNING, "图像绘制错误，但任务继续进行...", graph_title="图像绘制失败")
            return
        logger.debug(f"任务{task.task_id}保存图像的节点[{nodes}]和边[{edges}]")
        if is_end:
            self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_FINISH, system_output, graph_title, nodes=nodes, edges=edges)
        else:
            self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_RUNNING, system_output, graph_title, nodes=nodes, edges=edges)
        return

    def _not_loop_validate(self, task, cur_task_result):
        """
        检测API调用链中是否存在循环调用。主要逻辑如下：
        1. 检测连续相同API调用：当连续3次出现相同API且结果相似时，判定为循环调用
        2. 检测循环调用链：检测是否存在A→B→C→A这样的循环模式
        3. 检测结果相似性：不仅检测完全相同，还检测结果的相似度
        如果没有检测到循环则返回True，否则返回False
        """
        # 构建完整的API调用链
        api_chains = []
        nodes = task.nodes
        for node in nodes:
            api_chains.append({
                "tool": node["label"],
                "result": node["result"],
            })
        api_chains.append({
            "tool": cur_task_result["tool"],
            "result": cur_task_result["result"],
        })
        
        chain_length = len(api_chains)
        
        # 1. 检测连续相同API调用
        current_api_name = ""
        number = 0
        for i in range(chain_length):
            api_response = api_chains[i]
            if api_response["tool"] != current_api_name:
                current_api_name = api_response["tool"]
                number = 1
            else:
                # 检查结果是否相似（长度相近且包含相似内容）
                prev_result = api_chains[i - 1]["result"]
                curr_result = api_response["result"]
                
                # 简单的相似度检查：长度差不超过20%，且包含相同的关键词
                length_similar = abs(len(prev_result) - len(curr_result)) / max(len(prev_result), len(curr_result)) < 0.2
                content_similar = any(keyword in prev_result and keyword in curr_result 
                                    for keyword in ["success", "error", "id", "name", "code"])
                
                if length_similar and content_similar:
                    number += 1
                else:
                    number = 1  # 结果不相似，重置计数器
                
                if number >= 3:
                    logger.warning(f"检测到连续相同API调用循环: {current_api_name}")
                    return False
        
        # 2. 检测循环调用链（最多检测长度为3的循环）
        max_cycle_length = 3
        if chain_length >= 2 * max_cycle_length:
            # 检查最后max_cycle_length个调用是否与前面的某个序列重复
            for cycle_length in range(2, max_cycle_length + 1):
                if chain_length >= 2 * cycle_length:
                    last_sequence = [api["tool"] for api in api_chains[-cycle_length:]]
                    # 检查前面是否有相同的序列
                    for i in range(chain_length - 2 * cycle_length + 1):
                        prev_sequence = [api["tool"] for api in api_chains[i:i+cycle_length]]
                        if last_sequence == prev_sequence:
                            logger.warning(f"检测到循环调用链: {last_sequence}")
                            return False
        
        return True

    def _supplement_parameters(self, new_query, missing_param):
        """
        补充缺失的参数值。
        该方法根据新的查询语句和缺失的参数名，尝试从工具调用的结果中获取缺失的参数值。
        首先通过 `apiSelectionHub` 获取合适的工具，然后使用 `paramExtractionHub` 提取参数，
        调用工具并解析返回结果，若结果中包含缺失的参数，则返回该参数值和工具名称。
        输入：
            :param new_query: 新的查询语句，用于获取缺失参数的值
            :param missing_param: 缺失的参数名
        :return:
            若找到缺失参数的值，返回参数值和工具名称、汇总响应结果；否则返回3个 None
        """
        #缺失的参数有可能是其他工具调用的结果
        tool = self.api_selection_hub.get_tool_coarse_and_fine(new_query, None, topK=self.topK)
        if tool is None:
            return None, None, None
        params, new_missing_param = self.param_extraction_hub.extraction_params(new_query, tool)
        if len(new_missing_param) != 0:
            return None, None, None

        single_tool_response = self.tool_use_hub.tool_use(tool, params)

        if single_tool_response.status_code != RESPONSE_STATUS_CODE_SUCCESS:
            return None, None, None

        if len(single_tool_response.text) != 0:
            results = json.loads(single_tool_response.text)

            if isinstance(results, list) and len(results) != 0:
                results = results[0]
            if missing_param in results:
                return results[missing_param], tool.name_for_human, {
                    "code": TASK_SUCCESS_CODE,
                    "tool": tool.name_for_human,
                    "result": single_tool_response.text,
                    "param": params,
                    "query": new_query,
                    "task_description": new_query
                }
            else:
                return None, None, None
        else:
            return None, None, None

    def _tool_check(self, tool, task_desc, raw_query):
        """
        对工具的检查，此函数接收工具和用户查询语句

        主要工作：
        1. 检查工具是否缺乏参数。
        2. 结合用户查询语句和工具，检查是否存在提示词注入攻击。
        3. 如工具缺乏参数，借助大模型进行参数的补全。

        :param
            tool: 当前被检查的工具
            task_desc: 输入的查询语句，用于描述用户的需求(由大模型生成和改写)
            raw_query：用户的原始查询
        :return:
            一个字典，包含处理结果的相关信息，
            如状态码code、工具名称tool、结果内容result、缺失参数信息missing_param、参数列表param和任务描述task_description等
        """
        params, missing_params = self.param_extraction_hub.extraction_params(task_desc + " " + raw_query, tool)
        missing_params_supplemented = []
        new_params = params.copy()
        if len(missing_params) == 0:
            logger.debug(f"[{raw_query}:{task_desc}]未缺失参数，准备安全检查")
            inject_flag, reason = self.generate_task_hub.gen_judge_task(task_desc, tool, new_params)
            if inject_flag:
                return {
                    "code": TASK_ERROR_CODE,
                    "result": "inject",
                    "tool": "异常调用节点-提示注入攻击",
                    "missing_param": [],
                    "param": {},
                    "query": task_desc,
                    "task_description": reason
                }
        else:
            logger.debug(f"[{raw_query}:{task_desc}]缺失参数，进行参数的补齐....")

            for missing_param in missing_params:
                gen_param_query = self.generate_task_hub.gen_param_task(task_desc,
                                                                        json.dumps(params, ensure_ascii=False, indent=4),
                                                        f'{missing_param.name}: {missing_param.description}')
                logger.debug(f"[{task_desc}]参数[{missing_param.name}:{missing_param.description}]生成或补齐任务的描述词为：{gen_param_query}")
                supplement_param, supplement_param_tool, supplement_param_result = self._supplement_parameters(
                    gen_param_query, missing_param.name)
                if supplement_param is not None:
                    logger.debug(f"[{raw_query}:{task_desc}]的缺失参数{missing_param.name}已补充：\n{supplement_param_tool}\n{supplement_param_result}")
                    params[missing_param.name] = supplement_param
                    missing_params_supplemented.append(supplement_param_result)
                else:
                    return {
                        "code": TASK_ERROR_CODE,
                        "result": "missing_param",
                        "tool": "异常调用节点-缺少必要参数",
                        "missing_param": [],
                        "param": {},
                        "query": task_desc,
                        "task_description": f"通过参数补全Query:{gen_param_query} 无法为 {tool.name_for_human} API 补全缺少参数 {missing_param.name}"
                    }

            logger.debug(f"[{raw_query}:{task_desc}]参数已补全，进行安全检查")
            inject_flag, reason = self.generate_task_hub.gen_judge_task(task_desc, tool, params)
            if inject_flag:
                return {
                    "code": TASK_ERROR_CODE,
                    "result": "inject",
                    "tool": "异常调用节点-提示注入攻击",
                    "missing_param": [],
                    "param": {},
                    "query": task_desc,
                    "task_description": reason
                }

        return {
            "code": TASK_SUCCESS_CODE,
            "result": "",
            "tool": tool.name_for_human,
            "missing_param": missing_params_supplemented,
            "param": params,
            "query": task_desc,
            "task_description": task_desc
        }


    def api_planning_before_human_feedback(self, task_desc, task_id, raw_query):
        """
        单API规划步骤中人类反馈部分前工作内容。此函数接收一个查询语句，根据查询选择合适的工具。
        如果在处理过程中出现问题或找不到合适的工具，则返回相应的错误信息。

        代码流程：
        1. 通过 `apiSelectionHub` 根据查询语句获取合适的工具。
        2. 若未找到工具，返回包含错误信息的结果。
        3. 若找到工具，使用 `paramExtractionHub` 提取参数和缺失参数。
        5. 若有缺失参数，使用 `generateTaskHub` 生成新查询，调用 `supplement_Parameters` 补充缺失参数。
        6. 若所有缺失参数都补充成功，并返回工具和工具参数；若有缺失参数补充失败，返回错误信息。

        :param
            task_desc: 输入的查询语句，用于描述用户的需求，一般多API时使用
            task_id: 任务ID
            raw_query：用户的原始查询
        :return:
            一个字典，包含处理结果的相关信息，如状态码、工具名称、结果内容、缺失参数信息、参数列表和任务描述等
        """
        tool = self.api_selection_hub.get_tool_coarse_and_fine(task_desc, None, topK=self.topK)

        if tool is None:
            txt = f"您的要求[{raw_query}]未找到合适的工具，请换个问法或问题再试试。"
            logger.warning(txt)
            self.task_manager.update_task_recorder(task_id, TASK_STATUS_FINISH, TASK_SYS_OUTPUT_STOP+txt, graph_title=GRAPH_TITLE_FAILURE)
        else:
            result = self._tool_check(tool, task_desc, raw_query)
            if result["code"] == TASK_SUCCESS_CODE:
                system_output = f'''根据您的查询要求{raw_query}，我发现目前需要使用工具{tool.name_for_human}，
                相关参数是[{result["param"]}]。\n请确认该工具是否正确且立即使用。如果您想停止本次任务执行也请告诉我。
                \n如果该工具正确且立即使用，建议回答‘立即执行’;
                \n如果您想停止本次任务执行，建议回答‘不执行’;
                '''
                self.task_manager.update_task_recorder(task_id, TASK_STATUS_WAIT_CONFIRM, system_output, graph_title="请确认",
                                                        curr_task_desc=task_desc,curr_tool_id=tool.tool_id, curr_tool_param=result["param"])

            else:
                self.task_manager.update_task_recorder(task_id, TASK_STATUS_FINISH, TASK_SYS_OUTPUT_STOP+result["task_description"], graph_title=GRAPH_TITLE_FAILURE, )
                # return result

    def api_planning_handle_human_feedback(self, task, human_feedback):
        """
        用以处理人类反馈信息，
        :param
            task: 任务实例
            human_feedback: 人类反馈信息
        """
        try:

            # 获取当前等待确认的工具信息
            curr_tool_id = task.curr_tool_id
            curr_tool_param = task.curr_tool_param
            
            if curr_tool_id == TASK_INIT_TOOL_ID:
                logger.error(f"任务[{task.task_id}]没有等待确认的工具")
                self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_FINISH,
                                                    TASK_SYS_OUTPUT_STOP+"没有等待确认的工具",
                                                    graph_title=GRAPH_TITLE_FAILURE)
                return
            
            # 获取工具对象
            tool = self.tool_manager.get_tools_by_ids([curr_tool_id])[0]
            
            # 进行意图识别
            intent = self._recognize_human_intent(human_feedback, tool, curr_tool_param)
            
            # 根据意图执行不同操作
            if intent == "confirm":
                # 确认执行工具调用
                # 分任务类型处理工具调用结果
                if task.task_type == TASK_TYPE_SINGLE:
                    invoke_result = self._process_single_api_invoke(task.raw_query,task, tool, curr_tool_param)
                    if invoke_result["code"] == TASK_ERROR_CODE:
                        # self._update_task_node_edge(task, invoke_result, invoke_result["task_description"])
                        self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_FINISH,
                                                                TASK_SYS_OUTPUT_STOP + invoke_result["task_description"],
                                                                graph_title=GRAPH_TITLE_FAILURE)
                        return
                    summary = self.tool_summary_hub.tool_summary(task.changed_query, [invoke_result])
                    self._update_task_node_edge(task, invoke_result,summary,True)
                    # self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_FINISH, summary, graph_title="任务完成")
                    logger.info(f"任务[{task.changed_query}]，ID[{task.task_id}]：已完成，任务摘要[{summary}]")
                else:
                    invoke_result = self._process_single_api_invoke(task.curr_task_desc,task, tool, curr_tool_param)
                    result = invoke_result["task_description"]
                    if invoke_result["code"] == TASK_ERROR_CODE:
                        logger.info(f"任务[{task.changed_query}]，ID[{task.task_id}]中止：发生错误，[{result}]")
                        self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_FINISH, result,graph_title="任务错误")
                        return
                    if not self._not_loop_validate(task,invoke_result):
                        logger.info(f"任务[{task.changed_query}]，ID[{task.task_id}]中止：发生调用链错误-循环调用")
                        invoke_result["code"] = TASK_ERROR_CODE
                        self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_FINISH,TASK_SYS_OUTPUT_STOP+"循环调用错误",
                                                                graph_title="调用链错误-循环调用")
                        return
                    logger.info(f"任务[{task.changed_query}]，ID[{task.task_id}]：已处理完成子任务{task.curr_task_desc}")
                    self._update_task_node_edge(task, invoke_result, f"已处理完成子任务 {task.curr_task_desc}")

                    # 任务类型为多API调用任务时，除了处理本次调用结果，还要继续下一个API调用
                    flag, task_description = self.generate_task_hub.gen_from_context_task(task.changed_query, task.nodes)
                    logger.info(f"任务[{task.changed_query}]，ID[{task.task_id}]：任务是否完成：[{flag}]，后续任务[{task_description}]")
                    if task_description is not None:
                        logger.debug(f"任务[{task.changed_query}]，ID[{task.task_id}]继续，任务描述[{task_description}]入库")
                        self._update_task_curr_desc(task.task_id, task_description)
                    # if not flag and task_description is not None:
                        self.api_planning_before_human_feedback(task_description, task.task_id, task.raw_query)
                    # if task_description is not None:
                    #     self.api_planning_before_human_feedback(task_description, task.task_id, task.raw_query)
                    #多API调用任务也完成
                    if flag and task_description is None:
                        context = self._get_summary_from_nodes(task)
                        summary = self.tool_summary_hub.tool_summary(task.changed_query, context)
                        self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_FINISH, summary, graph_title="任务完成")

            elif intent == "abort":
                # 放弃任务执行
                self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_FINISH, TASK_SYS_OUTPUT_STOP+"已为您放弃任务执行")
            elif intent == "unclear":
                # 意图不明确，再次请求确认
                system_output = f'''您的反馈"{human_feedback}"不够明确，请重新确认：
                当前需要使用工具{tool.name_for_human}，相关参数是{curr_tool_param}，
                请明确是否执行该工具调用，或者确认放弃本次任务执行。'''
                self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_WAIT_CONFIRM, system_output)
            else:
                # 其他情况，继续等待确认
                system_output = f'''您的反馈"{human_feedback}"我们暂时无法理解，请重新确认：
                当前需要使用工具{tool.name_for_human}，相关参数是{curr_tool_param}，
                请明确是否执行该工具调用，或者确认放弃本次任务执行。'''
                self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_WAIT_CONFIRM, system_output)
        except Exception as e:
            logger.error(f"处理人类反馈时发生错误: {e}\n{traceback.format_exc()}")
            self.task_manager.update_task_recorder(task.task_id, TASK_STATUS_FINISH, TASK_SYS_OUTPUT_STOP+"处理您的要求时发生系统内部错误，请联系系统管理员", graph_title=GRAPH_TITLE_FAILURE)

    def _process_single_api_invoke(self,query,task,tool,params):
        logger.debug(f"[{query}]准备进行工具{tool.operationId}调用,参数为：{params}")
        single_tool_response = self.tool_use_hub.tool_use(tool, params)
        logger.debug(f"[{query}]工具{tool.operationId}调用完成,响应为：{single_tool_response}")
        
        # 检查HTTP状态码
        if single_tool_response.status_code != RESPONSE_STATUS_CODE_SUCCESS:
            error_message = f"调用外部系统失败，HTTP状态码：{single_tool_response.status_code}"
            if single_tool_response.text:
                error_message += f"，错误信息：{single_tool_response.text[:500]}"  # 只显示前500个字符
            result = {
                "code": TASK_ERROR_CODE,
                "result": "",
                "tool": "异常调用节点-调用外部系统失败",
                "missing_param": [],
                "param": params,
                "query": query,
                "task_description": error_message
            }
            return result
        
        try:
            # 解析API调用结果
            api_result_text = single_tool_response.text
            
            # 检查结果是否为空
            if not api_result_text:
                result = {
                    "code": TASK_ERROR_CODE,
                    "result": "",
                    "tool": "异常调用节点-调用外部系统失败",
                    "missing_param": [],
                    "param": params,
                    "query": query,
                    "task_description": "API调用返回空结果"
                }
                return result
            
            # 解决调用函数时，返回结果过多的问题
            api_result_length_limit = int(api_result_max_length * api_result_max_threshold)
            api_invoke_result_length = len(api_result_text)
            api_result_fact = api_result_text
            
            if api_invoke_result_length >= api_result_length_limit:
                logger.warning(f"[{query}]API调用结果长度超出限制，截取长度{api_result_length_limit}...")
                api_result_fact = api_result_text[:api_result_length_limit]
            
            logger.debug(f"[{query}]工具实际调用结果：{api_result_fact}")
            
            # 检查结果中是否包含错误信息
            try:
                result_data = json.loads(api_result_text)
                if isinstance(result_data, dict):
                    # 检查是否包含错误字段
                    if "error" in result_data or "Error" in result_data or "ERROR" in result_data:
                        error_msg = result_data.get("error", result_data.get("Error", result_data.get("ERROR", "未知错误")))
                        result = {
                            "code": TASK_ERROR_CODE,
                            "result": "",
                            "tool": "异常调用节点-外部系统返回错误",
                            "missing_param": [],
                            "param": params,
                            "query": query,
                            "task_description": f"外部系统返回错误：{error_msg[:500]}"
                        }
                        return result
                    # 检查状态码
                    if "code" in result_data and result_data["code"] != 200:
                        result = {
                            "code": TASK_ERROR_CODE,
                            "result": "",
                            "tool": "异常调用节点-外部系统返回错误",
                            "missing_param": [],
                            "param": params,
                            "query": query,
                            "task_description": f"外部系统返回错误状态码：{result_data['code']}"
                        }
                        return result
            except json.JSONDecodeError:
                # 不是JSON格式，继续处理
                pass
            
            # 正常结果处理
            result = {
                "code": TASK_SUCCESS_CODE,
                "tool": tool.name_for_human,
                "result": api_result_fact,
                "missing_param": [],
                "param": params,
                "query": query,
                "task_description": query
            }
            
            # 如果结果被截断，添加说明
            if api_invoke_result_length >= api_result_length_limit:
                result["result"] = f"API调用结果长度为{api_invoke_result_length}超出限制，进行了截取，保留的内容为：{api_result_fact}"
                
        except Exception as e:
            logger.error(f"[{query}]处理工具{tool.operationId}调用结果时发生错误: {e}\n{traceback.format_exc()}")
            result = {
                "code": TASK_ERROR_CODE,
                "result": "",
                "tool": "异常调用节点-处理结果失败",
                "missing_param": [],
                "param": params,
                "query": query,
                "task_description": f"处理API调用结果时发生错误：{str(e)[:500]}"
            }
        
        logger.debug(f"[{query}]工具{tool.operationId}调用返回值：{result}")
        return result

    def _get_summary_from_nodes(self,task):
        context = []
        nodes = task.nodes
        for node in nodes:
            step = {
                "tool": node["label"],
                "task_description": node["task_description"],
                "result": node["result"]
            }
            context.append(step)
        return context

    def _recognize_human_intent(self, human_feedback, tool, tool_param):
        """
        识别人类反馈的意图
        :param human_feedback: 人类反馈信息
        :param tool: 当前工具
        :param tool_param: 工具参数
        :return: 意图类型
        """
        # 简易关键词匹配，此处仅做示例，实际场景中需要根据实际情况进行修改或者错误率较高时可以直接取消
        confirm_keywords = ["确认执行", "执行任务", "同意执行", "继续执行", "立即执行"]
        abort_keywords = ["放弃执行", "停止执行", "中止执行", "取消执行", "不执行", "不要执行"]

        feedback_lower = human_feedback.lower()
        
        # 确认意图
        for keyword in confirm_keywords:
            if keyword in feedback_lower:
                return "confirm"
        
        # 放弃意图
        for keyword in abort_keywords:
            if keyword in feedback_lower:
                return "abort"
        
        # 使用大模型进行语义理解
        prompt = f"""
        请分析以下人类反馈的意图，从以下选项中选择一个最符合的意图类型：
        1. confirm: 确认执行当前工具调用
        2. abort: 放弃任务执行
        3. unclear: 意图不明确
        
        当前工具: {tool.name_for_human}
        工具参数: {tool_param}
        人类反馈: {human_feedback}
        
        请只回答意图类型，不要包含其他内容。
        """
        
        try:
            response = self.llm.chat_completions(prompt, self.model,
                                            self.temperature, self.top_p)
            intent = response.strip().lower()
            if intent in ["confirm", "abort", "unclear"]:
                return intent
        except Exception as e:
            logger.error(f"使用大模型识别意图时出错: {e}\n{traceback.format_exc()}")
        
        return "unclear"

    def apis_planning(self, query, task_id):
        """
        多API规划函数，用于处理单个或多个查询的API调用流程。
        此函数接收一个查询语句，根据查询判断是否为单任务。若为单任务，直接调用单API规划函数；
        若为多任务，则循环调用单API规划函数，直到任务处理完成。最终返回API调用结果链。

        代码流程：
        1. 调用 `generateTaskHub.gen_root_task` 判断是否为单任务并获取根任务描述。
        2. 若为单任务，调用 `single_api_planning` 处理并将结果添加到结果链中。
        3. 若为多任务，进入循环，不断调用 `single_api_planning` 处理任务，
        并通过 `generateTaskHub.gen_task_from_context` 判断是否还有后续任务。
        4. 若最后还有剩余任务描述，再次调用 `single_api_planning` 处理并添加到结果链中。
        5. 返回完整的API调用结果链。

        :param
            query:输入的查询语句，用于用户描述的需求
            task_id:当前任务内部编号
        :return:
            一个列表，包含一个或多个字典，每个字典表示一次API调用的处理结果，
            包含状态码、工具名称、结果内容、缺失参数信息、参数列表和任务描述等
        """

        is_single_task, root_task_description = self.generate_task_hub.gen_root_task(query)
        logger.debug(f"系统初始处理[{query}]，is_single_task={is_single_task},任务描述为：{root_task_description}")
        if is_single_task:
            self._set_task_type(query, task_id, TASK_TYPE_SINGLE, f"系统判定[{query}]为单工具调用任务")
            self.api_planning_before_human_feedback(query, task_id, query)
        else:
            self._set_task_type(query, task_id, TASK_TYPE_APIS, f"系统判定[{query}]多工具调用任务")
            self.api_planning_before_human_feedback(root_task_description, task_id, query)
        return