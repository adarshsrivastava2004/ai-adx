# backend/query_planner.py
import requests
import re
import logging
from backend.config import OLLAMA_CHAT_URL, MODEL


# Setup Logger (Production Standard)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ENTERPRISE SYSTEM PROMPT (OPTIMIZED)
# Changes:
# 1. Enforced Case Insensitivity (=~)
# 2. Added "Performance First" rule (Time filters first)
# 3. Explicitly allowed 'let' statements
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """
You are an expert Principal Data Engineer converting natural language to Azure Data Explorer (KQL).
Your goal is to generate HIGH-PERFORMANCE, PRODUCTION-GRADE queries for massive datasets.

# 1. DATABASE SCHEMA
Table: StormEventsCopy
Columns:
- StartTime (datetime), EndTime (datetime)
- EpisodeId (int), EventId (int)
- State (string), EventType (string)
- DamageProperty (real), DamageCrops (real)

# 2. CRITICAL RULES (Strict Enforcement)
- **Output Format:** RAW TEXT ONLY. Do not use markdown backticks (```). Do not write explanations.
- **Case Insensitivity:** ALWAYS use `=~` for string equality and `has` for substring searches. Never use `==` for user input.
- **Performance First:** Put time filters (e.g., `where StartTime > ...`) immediately after the table name whenever a time context is implied.
- **Time Handling:** - NEVER use `format_datetime` inside a `summarize` or `bin` function.
  - ALWAYS use `bin(StartTime, 1d)` directly on the raw datetime column.
- **INVALID UNITS:** `1y`, `1mo` are NOT valid KQL.
- **VALID UNITS:** Use `365d` for years, `30d` for months, `1d` for days, `1h` for hours.
  - Example: Use `bin(StartTime, 365d)` instead of `bin(StartTime, 1y)`.
- **Massive Data Policy:**
  - If the user implies "All data" or "Trends" without filters -> Generate a `summarize count() by bin(StartTime, ...)` query.
  - **NEVER** use `take` or `limit` on broad analytical queries; use aggregations instead.

# 3. STRATEGY PATTERNS

# PATTERN 1: Broad/Massive Request ("Show me all events", "Total data", "Timeline")
StormEventsCopy
| summarize EventCount = count() by bin(StartTime, 1d)
| order by StartTime desc
| render timechart

# PATTERN 2: High Impact/Specific Analysis ("Worst floods in Texas")
StormEventsCopy
| where State =~ "TEXAS" and EventType =~ "Flood"
| top 50 by DamageProperty desc
| project StartTime, State, EventType, DamageProperty

# PATTERN 3: Aggregation by Category ("Total damage per state")
StormEventsCopy
| summarize TotalDamage = sum(DamageProperty) by State
| top 10 by TotalDamage desc
"""

def generate_kql(user_goal: str) -> str:
    """
    Takes a user intent and returns executable KQL code.
    Includes Enterprise-grade sanitization and robust extraction.
    """
    logger.info(f"[QueryPlanner] Generating KQL for: {user_goal}")

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
        response.raise_for_status() # Raise error for HTTP 4xx/5xx
        
        data = response.json()
        
        raw_content = data.get("message", {}).get("content", "").strip()
        
        if not raw_content:
            logger.warning("[QueryPlanner] LLM returned empty content.")
            return ""

        return sanitize_kql_output(raw_content)
        
    except Exception as e:
        logger.error(f"[QueryPlanner Error] {str(e)}")
        return "" # Or re-raise depending on your API needs
    
def sanitize_kql_output(raw_text: str) -> str:
    """
    Extracts valid KQL from LLM noise.
    Handles: Markdown blocks, 'Here is the query' text, and 'let' statements.
    """
    # 1. Clean Markdown backticks immediately
    clean_text = raw_text.replace("```kql", "").replace("```", "").strip()

    # 2. Regex Strategy
    # Look for 'StormEventsCopy' OR 'let' (for variable declarations)
    # This prevents stripping valid variable definitions at the start.
    pattern = r"((?:let\s+.+?;\s*)?StormEventsCopy.*)"
    
    match = re.search(pattern, clean_text, re.DOTALL | re.IGNORECASE)
    
    if match:
        kql = match.group(1).strip()
    else:
        # Fallback: Use the whole text if it looks vaguely like the table query
        # This handles cases where LLM might alias the table: "T | ..." (Rare but possible)
        kql = clean_text

    # 3. Final Integrity Check
    # We check if the table name exists strictly to avoid hallucinations
    if "StormEventsCopy" not in kql:
        # Auto-Correction: If it looks like a pipe chain, prepend the table
        if kql.startswith("|"):
            kql = "StormEventsCopy\n" + kql
        else:
            logger.error(f"[QueryPlanner] Invalid KQL generated: {kql}")
            return ""

    return kql
