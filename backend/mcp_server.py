import re
import uuid
import time
from typing import Dict

class MCPServer:
    def __init__(self):
        self.allowed_table = "StormEventsCopy"

        self.allowed_columns = {
            "StartTime", "EndTime", "EpisodeId", "EventId", "State",
            "EventType", "DamageProperty", "DamageCrops"
        }

        # FIX: Added "count_" to the allowed keywords list
        self.allowed_keywords = {
            "stormeventscopy", "where", "summarize", "by", "top", "order",
            "count", "count_", "sum", "avg", "min", "max", "dcount", # <--- Added count_
            "and", "or", "ago", "desc", "asc", "limit", "take", "project",
            "sort", "distinct", "arg_max", "arg_min"
        }

        # Expanded comparison operators
        self.allowed_operators = {
            ">=", "<=", "==", "!=", ">", "<", 
            "in", "contains", "has", "startswith", "endswith"
        }

        # Hard-blocked patterns (SECURITY ONLY)
        self.blocked_keywords = [
            ".create", ".drop", ".delete", ".ingest", ".alter", ".set",
            "cluster(", "database(", "external_table", "plugin",
            "render", "mv-expand", "evaluate"
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

        # Validation Pipeline
        self.validate_safety(kql)        # Step 1: Check for malicious keywords
        self.enforce_structure(kql)      # Step 2: Check | pipe structure
        self.enforce_schema(kql)         # Step 3: Check columns & whitelist

        latency = round(time.time() - start, 3)
        self.log(trace_id, "ACCEPTED", {"latency_sec": latency})

        return {
            "trace_id": trace_id,
            "validated_kql": kql
        }

    # ---------------------------
    # 1. SAFETY CHECKS
    # ---------------------------
    def validate_safety(self, kql: str):
        """Checks for explicitly banned dangerous keywords."""
        lowered = kql.lower()
        for word in self.blocked_keywords:
            if word in lowered:
                raise ValueError(f"MCP blocked unsafe keyword: {word}")

    # ---------------------------
    # 2. STRUCTURE ENFORCEMENT
    # ---------------------------
    def enforce_structure(self, kql: str):
        """
        Validates that the query starts with the allowed table and
        follows the basic KQL pipe structure, ignoring whitespace/newlines.
        """
        clean_kql = kql.strip()
        
        # Split by pipe to separate operations
        segments = clean_kql.split('|')
        
        # Check Table Name (Must be first)
        first_segment = segments[0].strip()
        if first_segment != self.allowed_table:
             raise ValueError(f"Query must start with exactly '{self.allowed_table}'")

        # Check for empty pipes (e.g. "Table || count")
        for i, segment in enumerate(segments[1:]):
            if not segment.strip():
                raise ValueError(f"Empty statement found after pipe #{i+1}")

    # ---------------------------
    # 3. SCHEMA & TOKEN ENFORCEMENT
    # ---------------------------
    def enforce_schema(self, kql: str):
        """
        Validates tokens against whitelist. 
        CRITICAL FIX: Strips string literals ("Texas") so they aren't flagged as columns.
        """
        
        # Step A: Remove String Literals & Comments
        # We replace them with a dummy space so validation ignores user data values
        sanitized_kql = self._strip_literals_and_comments(kql)

        # Step B: Extract potential tokens (words)
        # Matches alphanumeric sequences including underscore
        tokens = re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", sanitized_kql)

        for token in tokens:
            lowered_token = token.lower()

            # 1. Check Keywords (case-insensitive)
            if lowered_token in self.allowed_keywords:
                continue
            
            # 2. Check Operators that look like words (in, contains)
            if lowered_token in self.allowed_operators:
                continue

            # 3. Check Table Name (exact match usually required, but we allow safe fallback)
            if token == self.allowed_table:
                continue

            # 4. Check Column Names (Case sensitive usually matters in KQL, but we check set)
            if token in self.allowed_columns:
                continue

            # 5. Check if it's a declared variable/alias (e.g., "NewCol =" or "NewCol=")
            if self._is_alias_definition(token, kql):
                continue
            
            # 6. Allow numbers (Regex didn't catch them, but just in case)
            if token.isdigit():
                continue

            raise ValueError(f"MCP blocked unknown identifier: '{token}'")

    # ---------------------------
    # HELPERS
    # ---------------------------
    def _strip_literals_and_comments(self, text: str) -> str:
        """
        Removes content inside quotes ('...' or "...") and KQL comments (// ...).
        This allows users to query data values like 'Texas' without validation errors.
        """
        # Remove KQL comments (// ...)
        text = re.sub(r"//.*", " ", text)
        
        # Remove single-quoted strings '...'
        text = re.sub(r"'[^']*'", " ", text)
        
        # Remove double-quoted strings "..."
        text = re.sub(r'"[^"]*"', " ", text)
        
        return text

    def _is_alias_definition(self, token: str, original_kql: str) -> bool:
        """
        Checks if 'token' is being defined as a new column name.
        Pattern: "token =" or "token="
        """
        # Look for the token followed immediately by optional space and =
        pattern = rf"\b{re.escape(token)}\s*="
        return re.search(pattern, original_kql) is not None

    def log(self, trace_id: str, stage: str, data: Dict):
        print(f"\n[MCP] trace_id={trace_id} | stage={stage}")
        # print(f"[MCP] data={data}") # Uncomment for verbose debugging