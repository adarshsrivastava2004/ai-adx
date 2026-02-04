# backend/adx_client.py

import logging
from typing import List, Dict, Any

# We import specific Kusto exceptions to handle "Bad Queries" vs "Network Errors" differently
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError, KustoClientError

from backend.config import (
    ADX_CLUSTER_URL,
    ADX_DATABASE,
    AZURE_CLIENT_ID,
    AZURE_CLIENT_SECRET,
    AZURE_TENANT_ID
)

logger = logging.getLogger(__name__)

class ADXManager:
    def __init__(self):
        # We hold the client in a variable but START as None.
        # This is "Lazy Loading". We won't connect until the first query runs.
        self._client = None

    def _get_client(self) -> KustoClient:
        """
        Retrieves the existing client or creates a new one if it doesn't exist.
        Reason: Prevents the backend from crashing immediately on startup if 
        internet is down or credentials are wrong.
        """
        if self._client:
            return self._client
        
        try:
            # Check for Service Principal (Production) vs Device Login (Local Dev)
            if AZURE_CLIENT_ID and AZURE_CLIENT_SECRET:
                kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                    ADX_CLUSTER_URL, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
                )
            else:
                kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(ADX_CLUSTER_URL)
            
            self._client = KustoClient(kcsb)
            return self._client

        except Exception as e:
            # Critical error: We cannot even create the client object.
            logger.critical(f"[AUTH FAILED] {str(e)}")
            raise

    def run_kql(self, query: str) -> List[Dict[str, Any]]:
        """
        Executes a KQL query and returns a clean list of dictionaries.
        """
        client = self._get_client()
        try:
            response = client.execute(ADX_DATABASE, query)
            
            # Kusto returns multiple tables; we only want the first one (primary results)
            if not response.primary_results:
                return []

            # Serialize results to ensure they are JSON safe (handling Dates)
            results = []
            for row in response.primary_results[0]:
                results.append(self._serialize(row.to_dict()))
            return results
        
        # -------------------------------------------------------
        # CRITICAL: Error differentiation
        # -------------------------------------------------------
        except KustoServiceError as e:
            # This catches "Semantic Errors" (e.g., "Table not found", "Invalid column").
            # We raise ValueError so our App knows it was a logic error, not a system crash.
            logger.error(f"[ADX Semantic Error]: {e}")
            raise ValueError(str(e)) 
            
        except Exception as e:
            # This catches "System Errors" (e.g., Network down, DNS failure).
            logger.error(f"[ADX System Error]: {e}")
            raise ConnectionError(str(e))

    def _serialize(self, row: Dict) -> Dict:
        """
        Helper: Converts Python datetime objects to ISO strings (e.g., "2023-01-01").
        Why? FastAPI/JSON cannot send raw Python datetime objects to the frontend.
        """
        clean = {}
        for k, v in row.items():
            if hasattr(v, 'isoformat'):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        return clean

# Singleton Instance: We create one manager for the whole app
adx_manager = ADXManager()

# Public function used by main.py
def run_kql(query: str):
    return adx_manager.run_kql(query)