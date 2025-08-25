
import json
import uuid
from databricks.sdk import WorkspaceClient
from databricks_openai import UCFunctionToolkit, DatabricksFunctionClient
from typing import Any, Optional

import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse, ChatContext

# Databricksのモデルサービングエンドポイントと通信するために設定されたOpenAIクライアントを取得
# これを使ってエージェント内でLLMに問い合わせる
openai_client = WorkspaceClient().serving_endpoints.get_open_ai_client()

# 以下のスニペットは、Databricksワークスペース内で利用可能な最初のLLM APIを
# 複数の候補から選択しようとします。必要に応じてLLM_ENDPOINT_NAMEを直接指定して簡略化可能です。
LLM_ENDPOINT_NAME = None

def is_endpoint_available(endpoint_name):
  try:
    client = WorkspaceClient().serving_endpoints.get_open_ai_client()
    client.chat.completions.create(model=endpoint_name, messages=[{"role": "user", "content": "AIとは何ですか？"}])
    return True
  except Exception:
    return False
  
for candidate_endpoint_name in ["databricks-claude-3-7-sonnet", "databricks-meta-llama-3-3-70b-instruct"]:
    if is_endpoint_available(candidate_endpoint_name):
      LLM_ENDPOINT_NAME = candidate_endpoint_name
assert LLM_ENDPOINT_NAME is not None, "LLM_ENDPOINT_NAMEを指定してください"

# LLM呼び出しの自動トレースを有効化
mlflow.openai.autolog()

# Databricks組み込みツール（ステートレスなPythonコードインタプリターツール）をロード
client = DatabricksFunctionClient()
builtin_tools = UCFunctionToolkit(function_names=["system.ai.python_exec"], client=client).tools
for tool in builtin_tools:
    del tool["function"]["strict"]

def call_tool(tool_name, parameters):
    if tool_name == "system__ai__python_exec":
        return DatabricksFunctionClient().execute_function("system.ai.python_exec", parameters=parameters)
    raise ValueError(f"不明なツールです: {tool_name}")

def run_agent(prompt):
    """
    ユーザープロンプトをLLMに送信し、LLMの応答メッセージのリストを返す
    必要に応じて、LLMはコードインタプリターツールを呼び出してユーザーに応答可能
    """
    result_msgs = []
    response = openai_client.chat.completions.create(
        model=LLM_ENDPOINT_NAME,
        messages=[{"role": "user", "content": prompt}],
        tools=builtin_tools,
    )
    msg = response.choices[0].message
    result_msgs.append(msg.to_dict())

    # モデルがツールを実行した場合は呼び出す
    if msg.tool_calls:
        call = msg.tool_calls[0]
        tool_result = call_tool(call.function.name, json.loads(call.function.arguments))
        result_msgs.append({"role": "tool", "content": tool_result.value, "name": call.function.name, "tool_call_id": call.id})
    return result_msgs

class QuickstartAgent(ChatAgent):
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        prompt = messages[-1].content
        raw_msgs = run_agent(prompt)
        out = []
        for m in raw_msgs:
            out.append(ChatAgentMessage(
                id=uuid.uuid4().hex,
                **m
            ))

        return ChatAgentResponse(messages=out)

AGENT = QuickstartAgent()
mlflow.models.set_model(AGENT)
