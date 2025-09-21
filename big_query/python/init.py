# python -m pip install google-cloud-bigquery

from google.cloud import bigquery

from gcp_config import PROJECT_ID, DATASET_ID, TABLE_ID, LOCATION

def main():
    client = bigquery.Client(project=PROJECT_ID)

    # 1) 创建 dataset（若已存在则跳过）
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = LOCATION
    client.create_dataset(dataset_ref, exists_ok=True)

    # 2) 创建表（若已存在则跳过）
    schema = [
        bigquery.SchemaField("id", "INT64"),
        bigquery.SchemaField("msg", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]
    table_ref = bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", schema=schema)
    client.create_table(table_ref, exists_ok=True)

    print("Dataset and table are ready.")

if __name__ == "__main__":
    main()
