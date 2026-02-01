from fastapi import FastAPI
from backend.schemas import ChatRequest
from backend.orchestrator import llm_decider
from backend.query_planner import generate_kql
from backend.adx_client import run_kql
from backend.mcp_server import MCPServer
from backend.formatter import format_response   # ✅ formatter import


# -----------------------
# FastAPI app
# -----------------------
app = FastAPI(title="LLM + ADX + MCP Backend")


# -----------------------
# MCP server (control plane)
# -----------------------
mcp = MCPServer()


# -----------------------
# Intelligent endpoint
# -----------------------
@app.post("/chat")
def chat(req: ChatRequest):
    user_message = req.message

    # -----------------------
    # Step 1: Orchestrator
    # -----------------------
    decision = llm_decider(user_message)

    # -----------------------
    # CHAT PATH
    # -----------------------
    if decision.tool == "chat":
        raw_result = {
            "tool": "chat",
            "reply": "This question does not require database access."
        }

        final_answer = format_response(user_message, raw_result)
        return {"reply": final_answer}

    # -----------------------
    # ADX PATH
    # -----------------------
    if decision.tool == "adx":

        # Stop on ambiguous goal
        if not decision.query_goal.strip():
            raw_result = {
                "tool": "adx",
                "error": "Ambiguous or unsupported query goal"
            }

            final_answer = format_response(user_message, raw_result)
            return {"reply": final_answer}

        try:
            # -----------------------
            # Step 2: Query Planner
            # -----------------------
            kql = generate_kql(decision.query_goal)

            if not kql:
                raw_result = {
                    "tool": "adx",
                    "error": "Query planner could not generate a valid query"
                }

                final_answer = format_response(user_message, raw_result)
                return {"reply": final_answer}

            # -----------------------
            # Step 3: MCP
            # -----------------------
            mcp_result = mcp.process(
                tool="adx",
                kql=kql,
                goal=decision.query_goal
            )

            validated_kql = mcp_result["validated_kql"]

            # -----------------------
            # Step 4: ADX Execution
            # -----------------------
            data = run_kql(validated_kql)

            raw_result = {
                "tool": "adx",
                "rows": len(data),
                "data": data
            }

            final_answer = format_response(user_message, raw_result)
            return {"reply": final_answer}

        except Exception as e:
            raw_result = {
                "tool": "adx",
                "error": str(e)
            }

            final_answer = format_response(user_message, raw_result)
            return {"reply": final_answer}

    # -----------------------
    # FALLBACK (should never hit)
    # -----------------------
    raw_result = {
        "error": "Invalid tool decision"
    }

    final_answer = format_response(user_message, raw_result)
    return {"reply": final_answer}
