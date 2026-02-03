# backend/formatter.py
# FIXED VERSION:
# - Added error handling so backend does NOT crash if Ollama is down
# - Formatter strictly follows read-only, explanation-only behavior

import json
import requests
import traceback
from backend.config import OLLAMA_GENERATE_URL, MODEL


FORMATTER_SYSTEM_PROMPT = """
You are a response formatter for a data assistant.

You DO NOT:
- generate queries
- modify data
- mention databases, KQL, ADX, MCP, or internal systems

You ONLY:
- explain results clearly
- answer the user's question using the provided data
- handle errors politely

Rules:
- If data is present, summarize it in simple language
- If rows are few, you may list them
- If rows are many, summarize trends
- If an error is present, explain it in simple terms
- Never expose internal fields like trace_id or generated_kql unless explicitly asked
- Be concise, clear, and user-friendly
"""


def format_response(user_query: str, system_json: dict) -> str:
    """
    FIXED & SAFE FORMATTER

    user_query  : Original user question (string)
    system_json : Internal JSON result from backend (chat / adx / out_of_scope)

    This function:
    - Uses LLM ONLY for formatting and explanation
    - NEVER generates queries or mentions internal systems
    - Gracefully handles Ollama / network failures
    """

    try:
        prompt = f"""
{FORMATTER_SYSTEM_PROMPT}

User Question:
{user_query}

System Result (JSON):
{json.dumps(system_json, indent=2)}

Generate the best possible answer for the user.
"""

        response = requests.post(
            OLLAMA_GENERATE_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False
            },
            timeout=120
        )

        return response.json()["response"].strip()

    except Exception as e:
        # ==================================================
        # ✅ DEBUG PRINT: Show me the error in the terminal!
        # ==================================================
        print("\n" + "="*40)
        print("❌ FORMATTER CRASHED")
        print(f"Error: {str(e)}")
        print("Traceback:")
        print(traceback.format_exc())
        print("="*40 + "\n")
        return (
            "Sorry, I’m unable to generate a response right now. "
            "Please try again in a moment."
        )
