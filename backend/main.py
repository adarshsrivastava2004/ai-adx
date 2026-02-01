from fastapi import FastAPI
from backend.schemas import ChatRequest
from backend.orchestrator import llm_decider
from backend.query_planner import generate_kql
from backend.adx_client import run_kql
from backend.mcp_server import MCPServer
from backend.formatter import format_response
from backend.chat_llm import chat_llm

from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="LLM + ADX + MCP Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mcp = MCPServer()


@app.post("/chat")
def chat(req: ChatRequest):
    user_message = req.message.strip()

    # -----------------------
    # Step 1: Orchestrator
    # -----------------------
    decision = llm_decider(user_message)

    print(f"""
================ ROUTER DECISION ================
User Query : {user_message}
Tool       : {decision.tool}
Query Goal : {decision.query_goal}
================================================
""")

    # -----------------------
    # CHAT (greetings only)
    # -----------------------
    if decision.tool == "chat":
        return {"reply": chat_llm(user_message)}

    # -----------------------
    # OUT OF SCOPE (NO FORMATTER)
    # -----------------------
    if decision.tool == "out_of_scope":
        return {
            "reply": (
                "I can help with storm event data questions "
                "like damage, events, states, and time ranges. "
                "This question is outside that scope."
            )
        }

    # -----------------------
    # ADX PATH
    # -----------------------
    if decision.tool == "adx":

        # Guard: empty goal
        if not decision.query_goal.strip():
            return {
                "reply": "I couldn't understand a clear data request. "
                         "Please rephrase your question."
            }

        try:
            # Step 2: Query planner
            kql = generate_kql(decision.query_goal)

            if not kql:
                return {
                    "reply": "I couldn't generate a valid query for this request. "
                             "Please rephrase it more clearly."
                }

            # Step 3: MCP validation
            mcp_result = mcp.process(
                tool="adx",
                kql=kql,
                goal=decision.query_goal
            )

            validated_kql = mcp_result["validated_kql"]

            # Step 4: ADX execution
            data = run_kql(validated_kql)

            # Guard: no data
            if not data:
                return {
                    "reply": "No data was found for your request."
                }

            # ✅ ONLY place where formatter is allowed
            safe_result = {
                "rows": len(data),
                "data": data
            }

            final_answer = format_response(user_message, safe_result)
            return {"reply": final_answer}

        except Exception as e:
            # ❌ NO formatter for system / ADX errors
            print("[ADX ERROR]", str(e))
            return {
                "reply": (
                    "I couldn't retrieve the data right now due to a system issue. "
                    "Please try again later."
                )
            }

    # -----------------------
    # HARD SAFETY FALLBACK
    # -----------------------
    return {
        "reply": "Sorry, I couldn't process your request."
    }
