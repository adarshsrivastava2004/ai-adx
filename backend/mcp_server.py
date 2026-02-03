# backend/mcp_server.py
import re
import uuid
import time
from typing import Dict

class MCPServer:
    def __init__(self):
        self.allowed_table = "StormEventsCopy"

        # ---------------------------------------------------------
        # BLOCKLIST: Sirf dangerous patterns ko block karenge
        # ---------------------------------------------------------
        self.blocked_patterns = [
            r"^\s*\.",          # Control commands start with dot (.)
            r"\.drop\b",        # Explicit drops
            r"\.alter\b",       # Explicit alters
            r"\.create\b",      # Explicit creates
            r"\.set\b",         # Setting policies
            r"\.ingest\b",      # Ingesting data
            r";"                # Semicolon (SQL Injection prevention)
        ]

    # ---------------------------
    # ENTRY POINT
    # ---------------------------
    def process(self, tool: str, kql: str, goal: str) -> Dict:
        # 1. Start Tracing
        trace_id = str(uuid.uuid4())
        start_time = time.time()

        # 2. LOG REQUEST (Jaisa aap chahte thay)
        self.log(trace_id, "REQUEST", {
            "tool": tool,
            "goal": goal,
            "kql": kql
        })

        try:
            if tool != "adx":
                raise ValueError("MCPServer received non-adx tool request")

            if not kql or not kql.strip():
                raise ValueError("Empty KQL is not allowed")

            # 3. Basic Cleanup
            clean_kql = kql.strip()

            # 4. Security Check (Block Dangerous Commands)
            self.validate_safety(clean_kql)

            # 5. Table Check (Must start with correct table)
            self.validate_table_access(clean_kql)

            # 6. Success Logging
            latency = round(time.time() - start_time, 3)
            self.log(trace_id, "ACCEPTED", {"latency_sec": latency})

            return {
                "trace_id": trace_id,
                "validated_kql": clean_kql
            }

        except Exception as e:
            # Error Logging
            latency = round(time.time() - start_time, 3)
            self.log(trace_id, "BLOCKED", {"error": str(e), "latency_sec": latency})
            raise e

    # ---------------------------
    # SECURITY LOGIC
    # ---------------------------
    def validate_safety(self, kql: str):
        """
        Block commands starting with dot (.) or containing explicit admin keywords.
        """
        # Rule 1: KQL Admin commands start with dot (.)
        if kql.startswith("."):
            raise ValueError("Security Alert: Control commands (starting with '.') are NOT allowed.")

        # Rule 2: Check regex patterns for dangerous keywords
        for pattern in self.blocked_patterns:
            if re.search(pattern, kql, re.IGNORECASE | re.MULTILINE):
                raise ValueError(f"Security Alert: Blocked pattern detected -> {pattern}")

    def validate_table_access(self, kql: str):
        """
        Ensure the query actually starts with the allowed table.
        Splits by pipe '|' to safely get the first token.
        """
        # Split by pipe to get the first part (e.g., "StormEventsCopy ")
        parts = kql.split("|")
        first_word = parts[0].strip()
        
        # Check if the query starts with the table name
        if first_word != self.allowed_table:
            raise ValueError(f"Access Denied: You can only query table '{self.allowed_table}'. Found: '{first_word}'")

    # ---------------------------
    # LOGGING
    # ---------------------------
    def log(self, trace_id: str, stage: str, data: Dict):
        print(f"\n[MCP] trace_id={trace_id}")
        print(f"[MCP] stage={stage}")
        print(f"[MCP] data={data}")