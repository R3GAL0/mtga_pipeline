# Currently reprocesses all csvs, will need to change later
#  test

import os
from make_tables_bigquery import insert_all
from google.cloud import storage
from google.cloud import bigquery

TMP_BASE = "/tmp/data"
DB_BUCKET_NAME = "mtga_pipeline_bucket"
DB_FILE = "mtga_local.duckdb"
LANDING_PREFIX = "landing/"

def main():
    os.makedirs(TMP_BASE, exist_ok=True)

    client_stor = storage.Client()
    bucket = client_stor.bucket(DB_BUCKET_NAME)

    # Pull landing/csvs to /tmp/data
    blobs = client_stor.list_blobs(DB_BUCKET_NAME, prefix=LANDING_PREFIX)

    for blob in blobs:
        if blob.name.endswith(".csv"):
            local_path = os.path.join(TMP_BASE, os.path.basename(blob.name))
            blob.download_to_filename(local_path)
            print(f"Downloaded {blob.name} to {local_path}")


    client_bq = bigquery.Client()

    # Run process
    insert_all(data_dir=TMP_BASE, client=client_bq)


    # Clean up /tmp
    for f in os.listdir(TMP_BASE):
        os.remove(os.path.join(TMP_BASE, f))
    print("ETL job completed.")


if __name__ == "__main__":
    main()