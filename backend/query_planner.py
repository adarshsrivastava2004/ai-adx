# backend/query_planner.py
import requests
import re
from backend.config import OLLAMA_CHAT_URL, MODEL

# ---------------------------------------------------------------------------
# SYSTEM PROMPT
# This is the "brain" of the query generator.
# We must provide context (Schema) and examples (Few-Shot) to get good KQL.
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """
You are an expert Azure Data Explorer (KQL) query generator.

# 1. DATABASE SCHEMA
# We explicitly list the columns so the LLM knows what fields exist.
# Without this, it might hallucinate columns like "StormCategory" or "Cost".
Table: StormEventsCopy
Columns:
- StartTime (datetime)
- EndTime (datetime)
- EpisodeId (int)
- EventId (int)
- State (string)        : e.g., 'TEXAS', 'FLORIDA' (Upper case)
- EventType (string)    : e.g., 'Flood', 'Tornado', 'Hail'
- DamageProperty (real) : Financial damage in USD
- DamageCrops (real)    : Crop damage in USD

# 2. INSTRUCTIONS
- Output ONLY the KQL query. Do not wrap it in markdown (like ```kql).
- Do not provide explanations or sorry messages.
- Always start the query with the table name: 'StormEventsCopy'.
- Use the pipe operator '|' for chaining commands.
- **NEVER use '!= null' or '== null'.** Use `isnotempty(ColumnName)` instead.

# 3. FEW-SHOT EXAMPLES (CRITICAL)
# LLMs struggle with KQL syntax unless we show them examples.
# These examples teach the model how to map "Human Intent" -> "KQL Code".

User Goal: Filter where State is TEXAS and EventType is Flood. Show top 5 by DamageProperty.
KQL:
StormEventsCopy
| where State == "TEXAS" and EventType == "Flood"
| top 5 by DamageProperty desc

User Goal: Show total DamageCrops per State for the last 365 days.
KQL:
StormEventsCopy
| where StartTime > ago(365d)
| summarize TotalCropDamage = sum(DamageCrops) by State

User Goal: Get 10 recent storm events.
KQL:
StormEventsCopy
| top 10 by StartTime desc
"""

def generate_kql(user_goal: str) -> str:
    """
    Takes a sanitized user intent (e.g., "Filter State='TEXAS'")
    and returns executable KQL code.
    """
    print(f"[QueryPlanner] Generating KQL for: {user_goal}")

    try:
        # We use a POST request to Ollama
        response = requests.post(
            OLLAMA_CHAT_URL,
            json={
                "model": MODEL,
                "messages": [
                    # The System Prompt sets the rules and schema
                    {"role": "system", "content": SYSTEM_PROMPT},
                    # The User message is the specific request from the Orchestrator
                    {"role": "user", "content": user_goal}
                ],
                "stream": False,
                "options": {
                    # Temperature 0.1 makes the model very "boring" and precise.
                    # We don't want creativity in code generation; we want accuracy.
                    "temperature": 0.1
                }
            },
            timeout=30  # Don't let the backend hang forever
        )

        data = response.json()
        
        # Safety check: ensure Ollama actually returned a message
        if "message" not in data or "content" not in data["message"]:
            print("[QueryPlanner] Error: Empty response from LLM")
            return ""

        kql = data["message"]["content"].strip()

        # ---------------------------------------------------------
        # POST-PROCESSING / SANITIZATION
        # LLMs often add markdown blocks (```kql ... ```) even if told not to.
        # We must strip these out so the database doesn't crash.
        # ---------------------------------------------------------
        
        # Remove ```kql at the start
        kql = kql.replace("```kql", "")
        # Remove generic code blocks ```
        kql = kql.replace("```", "")
        # Remove leading/trailing whitespace
        kql = kql.strip()

        # Final sanity check: Ensure it starts with the table name
        if not kql.startswith("StormEventsCopy"):
            # If the LLM forgot the table name, we force-prepend it
            # assuming the LLM generated just the filter part (e.g., "| where...")
            if kql.startswith("|"):
                kql = "StormEventsCopy\n" + kql
            else:
                # If it's completely malformed, we reject it to be safe
                print(f"[QueryPlanner] Invalid KQL generated: {kql}")
                return ""

        return kql

    except Exception as e:
        print(f"[QueryPlanner Error] {str(e)}")
        return ""

# --------------------------------------------------
# LOCAL TESTING BLOCK
# This allows you to run `python backend/query_planner.py` directly
# to test if queries are generating correctly without running the full app.
# --------------------------------------------------
if __name__ == "__main__":
    print("=== QUERY PLANNER TEST MODE ===")
    test_q = "Filter State equals 'OHIO' and sort by DamageProperty descending"
    print(f"Input: {test_q}")
    print("Result:")
    print(generate_kql(test_q))