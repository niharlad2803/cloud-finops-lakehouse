import json
import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

def generate_billing_dataset(num_records=25000, days_back=45):
    services = [
        {"id": "SRV_EC2", "name": "Compute (VM)", "category": "Compute", "unit": "Hrs", "rate": 0.096},
        {"id": "SRV_S3", "name": "Object Storage", "category": "Storage", "unit": "GB-Mo", "rate": 0.023},
        {"id": "SRV_RDS", "name": "Relational DB", "category": "Database", "unit": "Hrs", "rate": 0.350},
        {"id": "SRV_SNOWFLAKE", "name": "Cloud Warehouse", "category": "Analytics", "unit": "Credits", "rate": 2.000},
        {"id": "SRV_NAT", "name": "NAT Gateway", "category": "Networking", "unit": "GB-Data", "rate": 0.045},
        {"id": "SRV_EKS", "name": "Kubernetes Cluster", "category": "Compute", "unit": "Hrs", "rate": 0.100}
    ]
    
    teams = ["Core-Platform", "Data-Engineering", "Customer-Web", "Inference-ML", "DevOps"]
    envs = ["production", "staging", "development"]
    regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"]
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    records = []

    for i in range(num_records):
        random_second = random.randint(0, int((end_date - start_date).total_seconds()))
        log_time = start_date + timedelta(seconds=random_second)
        service = random.choice(services)
        region = random.choice(regions)
        
        # 15% untagged resource leak (FinOps waste scenario)
        has_tags = random.random() > 0.15
        if has_tags:
            tag_dict = {
                "environment": random.choice(envs),
                "team": random.choice(teams),
                "managed_by": "terraform",
                "cost_center": f"CC-{random.randint(100, 105)}"
            }
        else:
            tag_dict = {"environment": "untagged", "team": "unknown"}
        
        usage_amount = round(random.uniform(0.5, 24.0), 3)
        cost = usage_amount * service["rate"]
        
        # Anomaly simulation: 2.5% chance of 12x-25x cost spikes
        is_anomaly = False
        if service["id"] in ["SRV_SNOWFLAKE", "SRV_EC2", "SRV_RDS"] and random.random() < 0.025:
            cost *= random.uniform(12.0, 25.0)
            is_anomaly = True
            
        records.append({
            "record_id": f"REC_{100000 + i}",
            "timestamp": log_time.strftime("%Y-%m-%d %H:%M:%S"),
            "service_id": service["id"],
            "service_name": service["name"],
            "category": service["category"],
            "region": region,
            "usage_unit": service["unit"],
            "usage_amount": usage_amount,
            "unblended_cost_usd": round(cost, 4),
            "resource_tags": json.dumps(tag_dict),
            "is_synthetic_anomaly": is_anomaly
        })
        
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["timestamp"]).dt.date
    output_path = "data/raw/cloud_billing_raw.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Generated {len(df)} billing records -> {output_path}")

if __name__ == "__main__":
    generate_billing_dataset()
