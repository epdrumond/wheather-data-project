from google.cloud import bigquery

def load_from_bucket(
        bucket_name: str, 
        source_blob_path: str, 
        destination_dataset: str,
        destination_table: str,
        destination_table_schema: str,
        credentials_file: str
):
    bq_client = bigquery.Client.from_service_account_json(credentials_file)

    query = f"""
    load data overwrite {destination_dataset}.{destination_table}
    ({destination_table_schema})
    from files (
        format = 'CSV',
        uris = ['gs://{bucket_name}{source_blob_path}']
    );
    """
    job = bq_client.query(query)

if __name__ == "__main__":
    print("Oi")

    