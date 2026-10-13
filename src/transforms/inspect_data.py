import duckdb

con = duckdb.connect()

print("--- 1. RECORD COUNT & TIME RANGE ---")
query_summary = """
SELECT 
    COUNT(*) AS total_records,
    MIN(date) AS start_date,
    MAX(date) AS end_date,
    ROUND(SUM(unblended_cost_usd), 2) AS total_cloud_spend_usd
FROM 'data/raw/cloud_billing_raw.parquet'
"""
print(con.execute(query_summary).df())

print("\n--- 2. SPEND BY CLOUD SERVICE ---")
query_service = """
SELECT 
    service_name,
    category,
    COUNT(*) AS total_events,
    ROUND(SUM(unblended_cost_usd), 2) AS total_spend_usd,
    ROUND(AVG(unblended_cost_usd), 2) AS avg_cost_per_event
FROM 'data/raw/cloud_billing_raw.parquet'
GROUP BY 1, 2
ORDER BY total_spend_usd DESC
"""
print(con.execute(query_service).df())

print("\n--- 3. UNTAGGED RESOURCE LEAK CHECK ---")
query_tags = """
SELECT 
    json_extract_string(resource_tags, '$.environment') AS environment,
    json_extract_string(resource_tags, '$.team') AS team,
    COUNT(*) AS record_count,
    ROUND(SUM(unblended_cost_usd), 2) AS total_cost_usd,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_records
FROM 'data/raw/cloud_billing_raw.parquet'
GROUP BY 1, 2
ORDER BY record_count DESC
"""
print(con.execute(query_tags).df())

print("\n--- 4. SYNTHETIC ANOMALY SPIKES DETECTED ---")
query_anomalies = """
SELECT 
    record_id,
    timestamp,
    service_name,
    unblended_cost_usd
FROM 'data/raw/cloud_billing_raw.parquet'
WHERE is_synthetic_anomaly = TRUE
ORDER BY unblended_cost_usd DESC
LIMIT 5
"""
print(con.execute(query_anomalies).df())
