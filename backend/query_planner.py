import requests
import re

# IMPORTANT: CHAT API (NOT generate)
OLLAMA_URL = "http://localhost:11434/api/chat"
MODEL = "qwen2.5:7b-instruct"


# --------------------------------------------------
# SYSTEM PROMPT
# --------------------------------------------------
SYSTEM_PROMPT = """
You are an expert Azure Data Explorer (KQL) query generator.

Table name: StormEventsCopy

Columns:
- StartTime (datetime)
- EndTime (datetime)
- EpisodeId (int)
- EventId (int)
- State (string, values are uppercase like 'FLORIDA')
- EventType (string)
- DamageProperty (real)
- DamageCrops (real)

Rules:
- Always start query with: StormEventsCopy
- Always generate valid KQL
- Never invent syntax
- Use summarize correctly:
    - sum() for totals
    - avg() for averages
    - count() for counts
- For top results use: top N by <metric> desc
- Never use admin commands (.create, .drop, .set, .delete, .ingest)
- Never use cluster(), database(), let, totable, extend, join, render
- Only output raw KQL, no explanation
"""


# --------------------------------------------------
# SAFETY
# --------------------------------------------------
BLOCKED_KEYWORDS = [
    ".create", ".drop", ".delete", ".ingest", ".alter", ".set",
    "cluster(", "database(", "let ", "totable", "extend",
    "join", "render", ";"
]


# --------------------------------------------------
# NORMALIZER
# --------------------------------------------------
def normalize_user_goal(goal: str) -> str:
    g = goal.lower().strip()

    if g in {
        "total events",
        "number of events",
        "total number of events",
        "count of events"
    }:
        return "count of events"

    if g in {
        "total damage",
        "overall damage"
    }:
        return "total damage"

    return goal


# --------------------------------------------------
# MAIN GENERATOR
# --------------------------------------------------
def generate_kql(user_goal: str) -> str:
    normalized_goal = normalize_user_goal(user_goal)

    response = requests.post(
        OLLAMA_URL,
        json={
            "model": MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": SYSTEM_PROMPT
                },
                {
                    "role": "user",
                    "content": normalized_goal
                }
            ],
            "stream": False,
            "options": {
                "temperature": 0
            }
        },
        timeout=30
    )

    data = response.json()

    # CHAT API response
    if "message" not in data or "content" not in data["message"]:
        return ""

    kql = data["message"]["content"].strip()
    return sanitize_kql(kql)


# --------------------------------------------------
# SANITIZER
# --------------------------------------------------
def sanitize_kql(kql: str) -> str:
    lowered = kql.lower()

    for word in BLOCKED_KEYWORDS:
        if word in lowered:
            raise ValueError(f"Unsafe KQL detected: {word}")

    if not kql.startswith("StormEventsCopy"):
        raise ValueError("KQL must start with StormEventsCopy")

    # Fix common model mistake: count → count()
    kql = re.sub(r"\bcount\b(?!\()", "count()", kql)

    return kql


# --------------------------------------------------
# LOCAL TEST
# --------------------------------------------------
if __name__ == "__main__":
    print("=== QUERY PLANNER TEST MODE ===")

    while True:
        q = input("\nUser query (or 'exit'): ").strip()
        if q.lower() == "exit":
            break

        try:
            out = generate_kql(q)
            if out:
                print("\nGenerated KQL:\n")
                print(out)
            else:
                print("\nNO OUTPUT\n")
        except Exception as e:
            print("\nERROR:", e)
