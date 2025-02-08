import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
BIGQUERY_CREDENTIALS = os.getenv("BIGQUERY_CREDENTIALS")
BUCKET = os.getenv("BUCKET")
WHEATHER_FILE_NAME = os.getenv("WHEATHER_FILE_NAME")
STATIONS_FILE_NAME = os.getenv("STATIONS_FILE_NAME")
SOURCE_DATASET = os.getenv("SOURCE_DATASET")
SOURCE_WHEATHER_TABLE = os.getenv("SOURCE_WHEATHER_TABLE")
SOURCE_STATIONS_TABLE = os.getenv("SOURCE_STATIONS_TABLE")

def load_from_bucket(
        bucket_name: str, 
        source_blob_path: str, 
        destination_dataset: str,
        destination_table: str,
        credentials_file: str
):
    bq_client = bigquery.Client.from_service_account_json(credentials_file)

    query = f"""
    load data overwrite {destination_dataset}.{destination_table}
    from files (
        format = 'CSV',
        uris = ['gs://{bucket_name}{source_blob_path}']
    );
    """
    job = bq_client.query(query)
    print(job.result())

if __name__ == "__main__":
    #Load wheather data
    load_from_bucket(
        bucket_name=BUCKET,
        source_blob_path="/raw/" + WHEATHER_FILE_NAME,
        destination_dataset=SOURCE_DATASET,
        destination_table=SOURCE_WHEATHER_TABLE,
        credentials_file=BIGQUERY_CREDENTIALS
    )

    #Load stations data
    load_from_bucket(
        bucket_name=BUCKET,
        source_blob_path="/raw/" + STATIONS_FILE_NAME,
        destination_dataset=SOURCE_DATASET,
        destination_table=SOURCE_STATIONS_TABLE,
        credentials_file=BIGQUERY_CREDENTIALS
    )
    