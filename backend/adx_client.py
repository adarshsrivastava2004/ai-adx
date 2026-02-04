# backend/adx_client.py
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from backend.config import (
    ADX_CLUSTER_URL,
    ADX_DATABASE,
    AZURE_CLIENT_ID,
    AZURE_CLIENT_SECRET,
    AZURE_TENANT_ID
)

# Use Service Principal if keys exist in .env, otherwise fallback to Device Login
if AZURE_CLIENT_ID and AZURE_CLIENT_SECRET and AZURE_TENANT_ID:
    print("[AUTH] Using Service Principal (Silent Login)")
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        ADX_CLUSTER_URL, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
    )
else:
    print("[AUTH] Using Device Login (Interactive)")
    kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(ADX_CLUSTER_URL)

client = KustoClient(kcsb)

def run_kql(query: str):
    response = client.execute(ADX_DATABASE, query)
    return [row.to_dict() for row in response.primary_results[0]]