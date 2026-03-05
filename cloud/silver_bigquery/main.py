# Currently reprocesses all csvs, will need to change later
#  test

import os
from make_tables_bigquery import insert_all
from google.cloud import storage
from show_tables import show_tables
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


    # Pull duckdb file to /tmp/data
    # db_blob = bucket.blob(DB_FILE)
    # local_db_path = os.path.join(TMP_BASE, DB_FILE)

    # if db_blob.exists():
    #     db_blob.download_to_filename(local_db_path)
    #     print("Downloaded existing DuckDB")
    # else:
    #     print("No DuckDB found — will create new")
    client_bq = bigquery.Client()

    # Run process
    insert_all(data_dir=TMP_BASE, client=client_bq)

    # show the new row counts
    # show_tables(local_db_path)

    # Push DuckDB back to GCS
    # db_blob.upload_from_filename(local_db_path)

    # Clean up
    for f in os.listdir(TMP_BASE):
        os.remove(os.path.join(TMP_BASE, f))
    print("ETL job completed.")


if __name__ == "__main__":
    main()