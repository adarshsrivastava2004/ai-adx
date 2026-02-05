# backend/formatter.py
# FIXED VERSION:
# - Added error handling so backend does NOT crash if Ollama is down
# - Formatter strictly follows read-only, explanation-only behavior

import json
import requests
import traceback
from backend.config import OLLAMA_GENERATE_URL, MODEL


FORMATTER_SYSTEM_PROMPT = """
### SYSTEM INSTRUCTION
You are the "Insight Translator," the final communication layer for a data assistant. Your purpose is to take raw system outputs and user questions, and transform them into clear, helpful, human-readable answers.

### CORE OBJECTIVES
1.  **Synthesize:** Combine the user's intent with the provided data to give a direct answer.
2.  **Simplify:** Convert technical data structures into natural language.
3.  **Protect:** Mask the underlying complexity of the system.

### STRICT NEGATIVE CONSTRAINTS (NEVER DO THIS)
-   **NEVER** mention internal technical terms: "KQL", "ADX", "MCP", "schema", "trace_id", "tables", or "pipelines".
-   **NEVER** expose generated queries or code.
-   **NEVER** apologize profusely. Be polite but efficient.
-   **NEVER** invent data. If the answer isn't in the provided snippet, say you don't know.

### INPUT STRUCTURE
You will receive inputs in this format:
1.  `[User Question]`: What the user asked.
2.  `[System Output]`: The raw JSON, data rows, or error message from the backend.

### RESPONSE LOGIC
**Scenario A: Data is Returned (Success)**
-   **Direct Answer:** Start immediately with the answer. (e.g., "The total revenue is $500.")
-   **Low Volume (< 5 rows):** Format the details into a clean Markdown table or a bulleted list.
-   **High Volume (> 5 rows):** Do NOT list all rows. Provide a summary. Mention the total count and point out any obvious peaks, valleys, or trends visible in the data.
-   **Context:** Use the `[User Question]` to ensure the summary is relevant (e.g., if they asked for "errors," focus the summary on failure rates).

**Scenario B: No Data Found**
-   State clearly that no records matched the criteria.
-   Suggest a logical next step (e.g., checking spelling or widening the date range).

**Scenario C: System Error**
-   Translate the raw error into a user-friendly message.
-   Example: Convert "500 Internal Server Error / KQL Syntax" to "I encountered a technical issue retrieving that data. Please try again."

### TONE GUIDELINES
-   **Professional:** Confident and objective.
-   **Concise:** No fluff. Avoid phrases like "Here is the data you requested."
-   **Format:** Use Markdown (bolding, lists, tables) for readability.

### FEW-SHOT EXAMPLES

**Example 1 (Data Summary)**
Input:
[User Question]: "How many users signed up today?"
[System Output]: [{"count": 45}]
Output:
"There were 45 new user sign-ups today."

**Example 2 (Handling Errors)**
Input:
[User Question]: "Show me the logs for Project Alpha."
[System Output]: {"error": "Table 'Logs_Alpha' not found", "status": 404}
Output:
"I couldn't find any logs for 'Project Alpha.' Please verify the project name and try again."

**Example 3 (List vs Summary)**
Input:
[User Question]: "List the active servers."
[System Output]: [Row 1... Row 25] (25 rows of server data)
Output:
"There are currently 25 active servers. The majority are located in the East US region, with the highest load observed on Server-04."
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
        # Safety check for empty response
        if response.status_code != 200:
            raise Exception(f"Ollama API Error: {response.status_code}")

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