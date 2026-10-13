import duckdb
import os

con = duckdb.connect()

print("Partitioning raw dataset into multi-tier date partitions...")

output_dir = "data/processed/lake"
os.makedirs(output_dir, exist_ok=True)

query = f"""
COPY (
    SELECT 
        *,
        YEAR(CAST(timestamp AS TIMESTAMP)) AS year,
        LPAD(CAST(MONTH(CAST(timestamp AS TIMESTAMP)) AS VARCHAR), 2, '0') AS month
    FROM 'data/raw/cloud_billing_raw.parquet'
) TO '{output_dir}' 
(FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1);
"""

con.execute(query)
print("Data lake partition complete. Directory structure created in data/processed/lake/")
