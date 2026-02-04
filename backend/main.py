# backend/main.py
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
    # Step 1: Orchestrator (The Router)
    # -----------------------
    # The Orchestrator decides if the user wants to:
    # 1. Chat ("chat") -> Simple greeting
    # 2. Query Data ("adx") -> Database interaction
    # 3. Ask unrelated questions ("out_of_scope") -> Polite refusal
    decision = llm_decider(user_message)

    print(f"""
================ ROUTER DECISION ================
User Query : {user_message}
Tool       : {decision.tool}
Query Goal : {decision.query_goal}
================================================
""")

    # -----------------------
    # PATH A: Simple Chat (Greetings)
    # -----------------------
    if decision.tool == "chat":
        return {"reply": chat_llm(user_message)}

    # -----------------------
    # PATH B: Out of Scope
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
    # PATH C: ADX Database Query (Enterprise Self-Healing)
    # -----------------------
    if decision.tool == "adx":
        # Guard: Check if the Orchestrator failed to extract a goal
        if not decision.query_goal.strip():
            return {
                "reply": "I couldn't understand a clear data request. Please rephrase your question."
            }
        # =========================================================
        # 🔄 SELF-HEALING LOOP
        # Instead of trying once and failing, we try up to 3 times.
        # If an error occurs, we feed the error back to the LLM to fix it.
        # =========================================================
        MAX_RETRIES = 2
        attempt = 0
        last_error = None
        while attempt <= MAX_RETRIES:
            try:
                # -----------------------
                # Step 2: Query Generation
                # -----------------------
                # If attempt == 0: Generates fresh KQL from user goal.
                # If attempt > 0 : Uses 'Repair Mode' to fix the 'last_error'.
                kql = generate_kql(decision.query_goal,retry_count=attempt,last_error=last_error)

                if not kql:
                    # If LLM returns nothing, force a retry with a generic error
                    raise ValueError("LLM generated empty or invalid KQL syntax.")

                # -----------------------
                # Step 3: MCP Validation (Guardrails)
                # -----------------------
                # Checks for security risks (e.g., .drop table) or missing limits.
                # If unsafe, it raises ValueError, which triggers the retry loop.
                mcp_result = mcp.process(
                    tool="adx",
                    kql=kql,
                    goal=decision.query_goal
                )
                validated_kql = mcp_result["validated_kql"]

                # -----------------------
                # Step 4: ADX Execution
                # -----------------------
                # Runs the query against Azure.
                # - Raises ValueError for semantic errors (e.g., "Invalid column") -> Retries
                # - Raises ConnectionError for network issues -> Stops loop
                data = run_kql(validated_kql)

                # Guard: no data
                if not data:
                    return {
                        "reply": "No data was found for your request."
                    }
                # --- SUCCESS ---
                # If we reach here, the query worked! We can break the loop.
                
                # -----------------------
                # Step 5: Data Formatting
                # -----------------------
                # We truncate data to 15 rows to prevent crashing the LLM context window
                MAX_ROWS_FOR_LLM = 15
                
                
                # Note: ADX Client already handles datetime serialization
                preview_data = data[:MAX_ROWS_FOR_LLM]
                
                system_result = {
                    "total_rows_found": len(data),
                    "rows_shown_to_ai": len(preview_data),
                    "data_sample": preview_data,
                    "note": "Data truncated for performance" if len(data) > MAX_ROWS_FOR_LLM else "Full data shown"
                }

                # Use the LLM to explain the data to the user
                final_answer = format_response(user_message, system_result)
                return {"reply": final_answer}

            except Exception as e:
                # -----------------------
                # RECOVERABLE ERROR (Logic/Syntax)
                # -----------------------
                # Caught: Semantic Errors (e.g., "Invalid bin size") OR MCP Blocks.
                # We save the error and loop back to let the LLM fix it
                last_error = str(e)
                print(f"⚠️ [Self-Healing] Attempt {attempt+1} Failed: {last_error}")
                attempt += 1
                
            except Exception as e:
                # -----------------------
                # FATAL ERROR (System)
                # -----------------------
                # Caught: Network/Auth Errors. The LLM cannot fix these.
                print(f"❌ [System Error] {str(e)}")
                return {"reply": "I encountered a system error connecting to the database."}


        # -----------------------
        # FAILURE (Loop Exhausted)
        # -----------------------
        # If we exit the while loop, it means we tried 3 times (0, 1, 2) and failed every time.
        return {
            "reply": (
                "I tried to run the query multiple times, but I kept encountering technical errors. "
                "Please try rephrasing your request."
            )
        }
    