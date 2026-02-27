import os
from make_tables import insert_all
from google.cloud import storage

TMP_BASE = os.environ.get("TMP_BASE", "/tmp/data")
DB_BUCKET_NAME = "mtga_pipeline_bucket"
DB_FILE = "mtga_local.duckdb"

def download_from_gcs(bucket_name, file_name, local_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    if blob.exists():
        blob.download_to_filename(local_path)
        print(f"Downloaded {file_name} from {bucket_name} to {local_path}")
    else:
        print(f"No {file_name} found in {bucket_name}, will create new.")

def upload_to_gcs(bucket_name, local_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(local_path))
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to {bucket_name}")

def main():
    os.makedirs(TMP_BASE, exist_ok=True)

    # Pull DuckDB
    local_db_path = os.path.join(TMP_BASE, DB_FILE)
    download_from_gcs(DB_BUCKET_NAME, DB_FILE, local_db_path)

    # Run ETL on all CSVs in TMP_BASE
    insert_all(input_path=TMP_BASE, db_path=local_db_path)

    # Push DuckDB back to GCS
    upload_to_gcs(DB_BUCKET_NAME, local_db_path)

    # Clean up
    for f in os.listdir(TMP_BASE):
        os.remove(os.path.join(TMP_BASE, f))
    print("ETL job completed.")

if __name__ == "__main__":
    main()