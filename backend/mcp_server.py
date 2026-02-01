import re
import uuid
import time
from typing import Dict


class MCPServer:
    def __init__(self):
        self.allowed_table = "StormEventsCopy"

        self.allowed_columns = {
            "StartTime",
            "EndTime",
            "EpisodeId",
            "EventId",
            "State",
            "EventType",
            "DamageProperty",
            "DamageCrops",
        }

        # Allowed KQL keywords (lowercase)
        self.allowed_keywords = {
            "stormeventscopy",
            "where",
            "summarize",
            "by",
            "top",
            "order",
            "count",
            "sum",
            "avg",
            "and",
            "or",
            "ago",
        }

        # Allowed comparison operators
        self.allowed_operators = {">=", "<=", "=="}

        # Hard-blocked patterns (SECURITY ONLY)
        self.blocked_keywords = [
            ".create", ".drop", ".delete", ".ingest", ".alter", ".set",
            "cluster(", "database(", "external_table",
            "let ", "extend", "join", "render",
            "bin(", "year(", "month(", "max(", "min(", ";"
        ]

    # ---------------------------
    # ENTRY POINT
    # ---------------------------
    def process(self, tool: str, kql: str, goal: str) -> Dict:
        trace_id = str(uuid.uuid4())
        start = time.time()

        self.log(trace_id, "REQUEST", {
            "tool": tool,
            "goal": goal,
            "kql": kql
        })

        if tool != "adx":
            raise ValueError("MCPServer received non-adx tool request")

        if not kql or not kql.strip():
            raise ValueError("Empty KQL is not allowed")

        self.validate_kql(kql)
        self.enforce_structure(kql)
        self.enforce_schema(kql)
        self.enforce_tokens(kql)

        latency = round(time.time() - start, 3)
        self.log(trace_id, "ACCEPTED", {"latency_sec": latency})

        return {
            "trace_id": trace_id,
            "validated_kql": kql
        }

    # ---------------------------
    # BASIC VALIDATION
    # ---------------------------
    def validate_kql(self, kql: str):
        lowered = kql.lower()

        for word in self.blocked_keywords:
            if word in lowered:
                raise ValueError(f"MCP blocked unsafe keyword: {word}")

        if not kql.strip().startswith(self.allowed_table):
            raise ValueError("KQL must start with StormEventsCopy")

    # ---------------------------
    # STRUCTURE ENFORCEMENT
    # ---------------------------
    def enforce_structure(self, kql: str):
        lines = [line.rstrip() for line in kql.splitlines() if line.strip()]

        if lines[0] != self.allowed_table:
            raise ValueError("First line must be exactly StormEventsCopy")

        for line in lines[1:]:
            if not line.lstrip().startswith("|"):
                raise ValueError("Every line after the first must start with '|'")

    # ---------------------------
    # SCHEMA ENFORCEMENT
    # ---------------------------
    def enforce_schema(self, kql: str):
        tokens = re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", kql)

        kql_keywords = {
            "summarize", "count", "sum", "avg", "by",
            "where", "top", "order", "and", "or", "ago"
        }

        for token in tokens:
            if token in kql_keywords:
                continue

            if token == self.allowed_table:
                continue

            if token in self.allowed_columns:
                continue

            if self._is_alias(token, kql):
                continue

            raise ValueError(f"MCP blocked unknown column or identifier: {token}")

    def _is_alias(self, token: str, kql: str) -> bool:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", token):
            return False

        return re.search(
            rf"\b{token}\s*=\s*(sum|avg|count)\(",
            kql
        ) is not None

    # ---------------------------
    # TOKEN ENFORCEMENT
    # ---------------------------
    def enforce_tokens(self, kql: str):
        raw_tokens = re.findall(r">=|<=|==|\b[A-Za-z_]+\b", kql)

        for tok in raw_tokens:
            if tok in self.allowed_operators:
                continue

            if tok.lower() in self.allowed_keywords:
                continue

            if tok == self.allowed_table:
                continue

            if tok in self.allowed_columns:
                continue

            if self._is_alias(tok, kql):
                continue

            raise ValueError(f"MCP blocked invalid token: {tok}")

    # ---------------------------
    # LOGGING
    # ---------------------------
    def log(self, trace_id: str, stage: str, data: Dict):
        print(f"\n[MCP] trace_id={trace_id}")
        print(f"[MCP] stage={stage}")
        print(f"[MCP] data={data}")
