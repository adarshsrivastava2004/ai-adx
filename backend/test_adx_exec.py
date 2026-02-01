from adx_client import run_kql

query = """
StormEventsCopy
| summarize count() by State
| take 5
"""


rows = run_kql(query)
print(rows)
