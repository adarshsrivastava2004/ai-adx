# backend/adx_client.py
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

CLUSTER = "https://kvc-10puhtpsx6y47qbwwb.australiaeast.kusto.windows.net"
DATABASE = "MyDatabase"   # make sure this matches exactly

# This uses device-code authentication (works without Azure subscription)
kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(CLUSTER)
client = KustoClient(kcsb)

def run_kql(query: str):
    response = client.execute(DATABASE, query)
    return [row.to_dict() for row in response.primary_results[0]]