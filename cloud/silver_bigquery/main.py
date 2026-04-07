# Currently reprocesses all csvs, will need to change later
#  test

import os
from make_tables_bigquery import insert_all
from google.cloud import storage
from google.cloud import bigquery
import csv

TMP_BASE = "/tmp/data"
DB_BUCKET_NAME = "mtga_pipeline_bucket"
DB_FILE = "mtga_local.duckdb"
LANDING_PREFIX = "landing/"

def main():
    batch_size = 10
    batch_total = 1
    # set batch_total to -1 to process all files

    os.makedirs(TMP_BASE, exist_ok=True)

    client_stor = storage.Client()
    bucket = client_stor.bucket(DB_BUCKET_NAME)

    client_bq = bigquery.Client()

    # checking total files processed
    # getting the max number of files
    bucket.blob("processed_list.csv").download_to_filename("/tmp/processed_list.csv")
    
    with open("/tmp/processed_list.csv") as f:
        processed_files = [line.strip() for line in f]

    # extracting file list to process
    blobs = client_stor.list_blobs(DB_BUCKET_NAME, prefix=LANDING_PREFIX)
    blobs_to_download = [b for b in blobs if b.name not in processed_files]

    max_files = len(blobs_to_download)
    
    if batch_total == -1 or batch_size*batch_total > max_files:
        batch_total = -(- batch_size // max_files)

    print(f"Processing {batch_total} batches")

    for i in range(batch_total):
        batch_process(i, batch_size, bucket, client_stor, client_bq)

    print("All batches completed.")

def batch_process(i, batch_size, bucket, client_stor, client_bq):

    # get list from blobs
    # if item in list is in blobs remove it

    # getting the already processed list of files and removing them from the process
    bucket.blob("processed_list.csv").download_to_filename("/tmp/processed_list.csv")
    
    with open("/tmp/processed_list.csv") as f:
        processed_files = [line.strip() for line in f]

    # extracting file list to process
    blobs = client_stor.list_blobs(DB_BUCKET_NAME, prefix=LANDING_PREFIX)
    blobs_to_download = [b for b in blobs if b.name not in processed_files][:batch_size]

    # downloading the files to process to /tmp/data
    for blob in blobs_to_download:
        if blob.name.endswith(".csv"):
            local_path = os.path.join(TMP_BASE, os.path.basename(blob.name))
            blob.download_to_filename(local_path)
            print(f"Downloaded {blob.name} to {local_path}")

    # Run process
    insert_all(data_dir=TMP_BASE, client=client_bq)

    # Updating processed_list.csv and pushing back to the bucket
    for blob in blobs_to_download:
        processed_files.append(blob.name)

    with open("/tmp/processed_list.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        for item in processed_files:
            writer.writerow([item])

    bucket.blob("processed_list.csv").upload_from_filename("/tmp/processed_list.csv")


    # Clean up /tmp
    for f in os.listdir(TMP_BASE):
        os.remove(os.path.join(TMP_BASE, f))
    
    print(f"Batch {i} completed.")



if __name__ == "__main__":
    main()