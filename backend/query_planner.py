# backend/query_planner.py
import requests
import re

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "qwen2.5:7b-instruct"

SCHEMA_CONTEXT = """
You are a deterministic Azure Data Explorer (Kusto / KQL) query generator.
You behave like a compiler, not a chat assistant.

ABSOLUTE BEHAVIOR:
- You do NOT explain
- You do NOT guess
- You do NOT infer intent
- You do NOT substitute concepts
- You output ONLY raw KQL or NOTHING
- If output is not 100% valid, output NOTHING

HARD FAILURE RULE (OVERRIDES ALL OTHERS):
If the user request contains ANY word, concept, column, function, value, or intent
that cannot be mapped DIRECTLY and EXACTLY to the allowed schema and rules below,
you MUST output NOTHING.

TARGET TABLE:
StormEventsCopy

ALLOWED COLUMNS:
StartTime
EndTime
EpisodeId
EventId
State
EventType
DamageProperty
DamageCrops

STATE RULE:
- State values MUST be uppercase (e.g. "FLORIDA")
- Lowercase or ambiguous state names → output NOTHING

KNOWN EVENTTYPE VALUES (USE ONLY IF USER EXPLICITLY MENTIONS):
Flood
Drought
Tornado
Hail
Thunderstorm
Wildfire

EVENTTYPE RULE:
- Do NOT infer or substitute EventType
- Unknown EventType → output NOTHING

AGGREGATION RULES:
- ALL aggregations MUST use summarize
- sum() → totals
- avg() → averages
- count() → counts
- ALL aggregated expressions MUST have an explicit alias
- Do NOT use max, min, bin, dcount, percentiles, etc.

TOP RULE:
- Use top N ONLY if user explicitly says "top", "highest", or "most"
- If N is missing → output NOTHING

TIME RULES:
- Time filters MUST use StartTime or EndTime
- Time comparisons MUST use ago()
- Do NOT infer time ranges (e.g. "recent")

SYNTAX RULES:
1. First line MUST be exactly: StormEventsCopy
2. Every following line MUST start with '|'
3. Use ONLY allowed columns
4. Do NOT use let, extend, join, render
5. Do NOT use semicolons
6. Do NOT invent syntax
7. If request is ambiguous or unsupported → output NOTHING

ALLOWED TOKENS:
StormEventsCopy
|
where
summarize
project
order
by
top
count
sum
avg
>=
<=
==
and
or
ago
(
)
"
.
"""

ALLOWED_TOKENS = {
    "stormeventscopy", "|", "where", "summarize", "project", "order", "by", "top",
    "count", "sum", "avg", ">=", "<=", "==", "and", "or", "ago",
    "(", ")", "\"", "."
}

BLOCKED_KEYWORDS = [
    ".create", ".drop", ".delete", ".ingest", ".alter", ".set",
    "cluster(", "database(", "let ", "extend", "join", "render",
    "bin(", "year(", "month(", "max(", "min(", ";"
]


def generate_kql(user_goal: str) -> str:
    prompt = f"""
{SCHEMA_CONTEXT}

USER REQUEST:
{user_goal}
"""

    response = requests.post(
        OLLAMA_URL,
        json={
            "model": MODEL,
            "prompt": prompt,
            "stream": False
        },
        timeout=30
    )

    raw_output = response.json().get("response", "").strip()

    if not raw_output:
        return ""

    return validate_kql(raw_output)


def validate_kql(kql: str) -> str:
    lowered = kql.lower()

    # Hard block unsafe keywords
    for word in BLOCKED_KEYWORDS:
        if word in lowered:
            raise ValueError(f"Blocked KQL keyword detected: {word}")

    # Must start with table
    if not kql.startswith("StormEventsCopy"):
        raise ValueError("KQL must start with StormEventsCopy")

    # Line rules
    lines = kql.splitlines()
    if lines[0].strip() != "StormEventsCopy":
        raise ValueError("First line must be exactly StormEventsCopy")

    for line in lines[1:]:
        if not line.strip().startswith("|"):
            raise ValueError("Every line after the first must start with '|'")

    # Token-level enforcement (simple lexer)
    tokens = re.findall(r'[A-Za-z_]+|>=|<=|==|\(|\)|\||"', lowered)

    for token in tokens:
        if token not in ALLOWED_TOKENS and not re.match(r"[a-z_]+", token):
            raise ValueError(f"Disallowed token detected: {token}")

    return kql


if __name__ == "__main__":
    while True:
        user_input = input("User query (or 'exit'): ").strip()
        if user_input.lower() == "exit":
            break

        try:
            result = generate_kql(user_input)
            if result:
                print("\nGenerated KQL:\n")
                print(result)
            else:
                print("\nNO OUTPUT (request unsupported or ambiguous)\n")
        except Exception as e:
            print(f"\nERROR: {e}\n")
