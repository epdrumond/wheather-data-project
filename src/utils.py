import pandas as pd
from google.cloud import storage


def format_json_into_dataframe(data:dict) -> pd.DataFrame:
    """
    Transform Visual Crossing API data into a dataframe

    Parameters:
        data: Whether data formated as a dictionary

    Returns:
        pd.DataFrame: Dataframe with formatted wheather data
    """

    #Load daily wheather data into a dataframe
    main_df = pd.DataFrame(data["days"])

    #Include remaining fields as constant-value columns 
    for key, val in data.items():
        if key != "days":
            if key == "stations":
                stations_df = pd.DataFrame(val).T
                stations_df = stations_df.merge(main_df["datetime"], how="cross")
            else:
                main_df[key] = val


    return main_df, stations_df

def load_into_gcp_bucket(
        bucket_name: str,
        source_file_path: str,
        destination_blob_name: str,
        credentials_file: str
) -> None:
    """
    Load locally stored data file into GCP bucket

    Parameters:
        bucket_name: Name of the bucket in which the file should be storaged
        source_file_path: Path of the source file to be stored
        destination_blob_name: File name at the destination bucket
        credentials_file: File containing the credentials for service account 
            responsible for the transfer

    Returns:
        int
    """

    #Initialize storage client and select destination bucket
    storage_client = storage.Client.from_service_account_json(credentials_file)
    bucket = storage_client.bucket(bucket_name)

    #Upload file 
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)

