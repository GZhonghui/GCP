from google.cloud import bigquery

from gcp_config import PROJECT_ID, DATASET_ID, TABLE_ID, LOCATION

def main():
    client = bigquery.Client(project=PROJECT_ID)

    # 4) 查询验证
    sql = f"""
      SELECT id, msg, created_at
      FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
      ORDER BY created_at DESC
      LIMIT 10
    """
    for row in client.query(sql, location=LOCATION).result():
        print(dict(row))

if __name__ == "__main__":
    main()
