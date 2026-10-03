"""Framework-neutral tool dispatch. Feed in parsed JSON tool calls.

Works at the boundary after any model/framework chooses a tool; no provider SDK
is required. A model's tool_call id is preserved in the result envelope.
"""
from agent_tool_budget import BudgetExceeded


async def dispatch(session, tool_call):
    call_id = tool_call["id"]
    try:
        result = await session.call(tool_call["name"], **tool_call["arguments"])
        return {"tool_call_id":call_id,"status":"ok","result":result}
    except BudgetExceeded as exc:
        # Route this structured outcome to a terminal node in your agent graph.
        return {"tool_call_id":call_id,"status":"budget_exhausted","message":str(exc)}
