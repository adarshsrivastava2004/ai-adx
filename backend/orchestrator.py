# backend/orchestrator.py

import requests
import logging
import json
from backend.schemas import ToolDecision
from backend.config import OLLAMA_CHAT_URL, MODEL

# 2. Setup Logger
logger = logging.getLogger(__name__)

# NOTE: Switched to /api/chat for better structured output support

# We define the JSON Schema explicitly here.
# This forces the LLM to follow this EXACT structure.
RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "tool": {
            "type": "string",
            "enum": ["adx", "chat", "out_of_scope"] # 🔒 Strict restriction on values
        },
        "query_goal": {
            "type": "string"
        }
    },
    "required": ["tool", "query_goal"]
}
SYSTEM_PROMPT = """
You are a Semantic Query Translator for a Storm Events database.
Your output feeds directly into a SQL/KQL code generator, so you must be precise.

--------------------------------------------------
DATABASE SCHEMA (Source of Truth):
--------------------------------------------------
Table: StormEventsCopy
Columns:
- StartTime (datetime)
- EndTime (datetime)
- EpisodeId (int)       : ID for the episode
- EventId (int)         : ID for the specific event
- State (string)        : Upper case state names (e.g., "TEXAS", "OHIO")
- EventType (string)    : Type of storm (e.g., "Flood", "Tornado", "Hail")
- DamageProperty (real) : Financial damage in USD (use for "cost", "damage", "loss")
- DamageCrops (real)    : Crop damage in USD (use for "crop loss", "agriculture damage")

--------------------------------------------------
YOUR JOB:
1. Analyze the user's request.
2. Map vague terms to specific Columns (e.g., "bad storm" -> DamageProperty).
3. output a precise 'query_goal' that acts as a technical spec.

--------------------------------------------------
TOOL DEFINITIONS:

1. "adx"
   - Trigger: Any request requiring database access.
   - Query Goal Requirements:
     * MUST explicitly name the columns to query.
     * MUST specify filters (e.g., "State equals 'TEXAS'").
     * MUST specify sorting (e.g., "Sort descending by DamageProperty").
     * MUST specify limits (e.g., "Take top 10").
     * Example: "Filter EventType='Flood' AND State='TEXAS'. Sort by DamageProperty DESC. Take 5."

2. "chat"
   - Trigger: Greetings, pleasantries, or closing remarks.
   - Query Goal: MUST be "" (empty string).

3. "out_of_scope"
   - Trigger: Questions not related to storm data.
   - Query Goal: MUST be "" (empty string).

--------------------------------------------------
FEW-SHOT EXAMPLES (Observe the translation to technical specs):

User: "Show me the worst floods in Texas."
Output: {
  "tool": "adx",
  "query_goal": "Filter where EventType is 'Flood' and State is 'TEXAS'. Sort by DamageProperty DESC (to find 'worst'). Return top results."
}

User: "How much did we lose in crops due to Hail last year?"
Output: {
  "tool": "adx",
  "query_goal": "Filter where EventType is 'Hail' and StartTime is within last year. Calculate sum of DamageCrops."
}

User: "List all events."
Output: {
  "tool": "adx",
  "query_goal": "Select all rows from StormEventsCopy. Limit to 100 to prevent overflow."
}

User: "Hi, are you there?"
Output: { "tool": "chat", "query_goal": "" }
--------------------------------------------------

OUTPUT FORMAT (JSON ONLY):
{
  "tool": "adx" | "chat" | "out_of_scope",
  "query_goal": "Technical spec string with columns and logic"
}
"""

def llm_decider(user_input: str) -> ToolDecision:
    """
    SAFE orchestrator LLM call using Structured Outputs.
    """

    try:
        response = requests.post(
            OLLAMA_CHAT_URL,
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_input}
                ],
                "stream": False,
                "format": RESPONSE_SCHEMA,  # 👈 THIS IS THE KEY CHANGE
                "options": {
                    "temperature": 0.0  # Deterministic output
                }
            },
            timeout=30
        )

        data = response.json()

        # 🔒 SAFETY: Check for API errors or missing content
        if "message" not in data or "content" not in data["message"]:
            logger.error("❌ [ORCHESTRATOR] Missing response from LLM")
            return ToolDecision(tool="out_of_scope", query_goal="")

        # The content is guaranteed to be a JSON string due to the schema
        raw_text = data["message"]["content"]

        # Debug log (Hidden by default in INFO mode, visible in DEBUG mode)
        logger.debug(f"[LLM RAW OUTPUT]: {raw_text}")

        parsed = json.loads(raw_text)

        return ToolDecision(
            tool=parsed["tool"],
            query_goal=parsed["query_goal"]
        )

    except Exception as e:
        # 🔒 HARD FAILSAFE
        logger.error(f"❌ [ORCHESTRATOR ERROR]: {str(e)}", exc_info=True)
        return ToolDecision(tool="out_of_scope", query_goal="")