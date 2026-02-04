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
- **Duration Math:**
  - KQL does NOT have `DATEDIFF`. 
  - To calculate duration in minutes: `(EndTime - StartTime) / 1m`.
  - To calculate duration in days: `(EndTime - StartTime) / 1d`.
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

def generate_kql(user_goal: str, retry_count: int = 0, last_error: str = None) -> str:
    """
    Generates KQL, with AUTOMATIC SELF-HEALING if a previous attempt failed.
    Args:
        user_goal (str): The user's original natural language request.
        retry_count (int): 
            - 0: Initial attempt (Standard Translation).
            - 1+: Retry attempt (Repair Mode).
        last_error (str): The specific error message from the database/compiler 
                          that caused the previous failure.
    """
    
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    
    # ---------------------------------------------------------
    # MODE 1: STANDARD GENERATION (First Attempt)
    # ---------------------------------------------------------
    # If this is the first time we are seeing this request, we just 
    # ask the LLM to translate the user's goal into KQL.
    if retry_count == 0:
        messages.append({"role": "user", "content": user_goal})
        logger.info(f"[QueryPlanner] Generating initial KQL for: {user_goal}")
        
    
    # ---------------------------------------------------------
    # MODE 2: REPAIR GENERATION (Self-Healing)
    # ---------------------------------------------------------
    # If we are retrying, it means the previous query failed.
    # Instead of asking the same question again (which would likely yield the same wrong answer),
    # we show the LLM the error message and ask it to debug its own code.
    else:
        logger.warning(f"[QueryPlanner] 🚑 Attempting Repair (Try #{retry_count}). Error: {last_error}")
        
        # We explicitly instruct the LLM to look at the error and fix the logic
        repair_prompt = f"""
        USER GOAL: {user_goal}
        
        PREVIOUS ATTEMPT FAILED.
        ERROR MESSAGE: {last_error}
        
        TASK: Fix the KQL query to resolve this specific error.
        - If the error is 'invalid data type', check your aggregations (bin/summarize).
        - If the error is 'syntax', check for missing pipes or brackets.
        - If the error is 'limit injected', rewrite the query to use 'summarize' instead of 'take'.
        - Output ONLY the fixed KQL.
        """
        messages.append({"role": "user", "content": repair_prompt})

    try:
        response = requests.post(
            OLLAMA_CHAT_URL,
            json={
                "model": MODEL,
                "messages": messages,
                "stream": False,
                "options": {"temperature": 0.1} # Strict precision
            },
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        raw_content = data.get("message", {}).get("content", "").strip()
        
        if not raw_content:
            return ""

        return sanitize_kql_output(raw_content)
         
    except Exception as e:
        logger.error(f"[QueryPlanner Error] {str(e)}")
        return "" # Or re-raise depending on your API needs
    
def sanitize_kql_output(raw_text: str) -> str:
    """
    Extracts valid KQL from LLM noise.
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
