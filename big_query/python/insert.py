from datetime import datetime, timezone
from google.cloud import bigquery

from gcp_config import PROJECT_ID, DATASET_ID, TABLE_ID

def main():
    client = bigquery.Client(project=PROJECT_ID)

    # 3) 插入几行
    rows = [
        {"id": 1, "msg": "hello from BQ", "created_at": datetime.now(timezone.utc).isoformat()},
        {"id": 2, "msg": "konnichiwa",   "created_at": datetime.now(timezone.utc).isoformat()},
    ]
    errors = client.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", rows)
    if errors:
        raise RuntimeError(errors)
    print("Inserted rows.")

if __name__ == "__main__":
    main()
