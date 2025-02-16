import os
import sys
import requests
import pandas as pd
import urllib.parse as parse

from airflow.models import Variable

from google.cloud import bigquery
from google.cloud import storage

# EXTRACTION FUNCTIONS --------------------------------------------------------------

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


def fetch_weather_data(city: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetchs daily wheater data from the Visual Crossing API, given a specified city, start and end dates.

    Parameters:
        city: City for witch we want to fetch wheather data
        start_date: Beginning of the period for the data extraction formated as a string (YYYY-MM-DD)
        end_date: End of the period for the data extraction formated as a string (YYYY-MM-DD)

    Returns:
        pd.Dataframe: Dataframe with the extracted wheather data
    """

    VISUALCROSSING_API_KEY = Variable.get("VISUALCROSSING_API_KEY")

    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{start_date}/{end_date}?unitGroup=metric&include=days&key={VISUALCROSSING_API_KEY}&contentType=json"
    response = requests.get(url)
    
    if response.status_code!=200:
        print('Unexpected Status code: ', response.status_code)
        sys.exit()  

    return response.json()


def extract_wheather_data(start_date: str, end_date: str, ti: str = None) -> None:
    """
    Extract wheather data through API calls and store it in local files

    Parameters:
        start_date: Start date for the extraction period formatted as a string
        end_date: End date for the extraction period formatted as a string
    """

    #Load required environment variables
    PATH = Variable.get("EXTRACTION_PATH")
    RAW_PATH = "raw/"

    #Transform provided date range to API url format
    start_date_str = start_date.replace("-", "")
    end_date_str = end_date.replace("-", "")

    #Load list of cities for wheather data extraction and prepare
    cities = pd.read_csv(PATH + "cities.txt")

    wheather_df = []
    stations_df = []
    for _, row in cities.iterrows():
        city_name = ','.join([row["city"], row["state"], row["country"]])
        enconded_city_name = parse.quote(city_name)
        
        wheather_data = fetch_weather_data(
            city=enconded_city_name,
            start_date=start_date,
            end_date=end_date
        )

        wheather, stations = format_json_into_dataframe(wheather_data)

        wheather_df.append(wheather)
        stations_df.append(stations)

    wheather_df = pd.concat(wheather_df, ignore_index=True)
    stations_df = pd.concat(stations_df, ignore_index=True)

    wheather_file_name = RAW_PATH + f"wheather_{start_date_str}_{end_date_str}.json"
    stations_file_name = RAW_PATH + f"stations_{start_date_str}_{end_date_str}.json"

    wheather_df.to_json(PATH + wheather_file_name, orient="records", lines=True)
    stations_df.to_json(PATH + stations_file_name, orient="records", lines=True)

    ti.xcom_push(key="wheather_file_name", value=wheather_file_name)
    ti.xcom_push(key="stations_file_name", value=stations_file_name)


# LOAD FUNCTIONS --------------------------------------------------------------------
def load_into_gcp_bucket(
        bucket_name: str,
        source_file_path: str,
        destination_blob_name: str,
        credentials_file: str,
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


def load_from_bucket(
        bucket_name: str, 
        source_blob_path: str, 
        destination_dataset: str,
        destination_table: str,
        credentials_file: str
) -> None:
    """
    Loads an specified table with data from a selected file

    Parameters:
        bucket_name: Name of the bucket in which the source files are stored
        source_blob_path: Path to the file withing the bucket
        destination_dataset: Dataset of the destination table
        destination_table: Destination table name
        credentials_file: Path to the file with the BigQuery credentials 
    """
    bq_client = bigquery.Client.from_service_account_json(credentials_file)

    query = f"""
    load data overwrite {destination_dataset}.{destination_table}
    from files (
        format = 'JSON',
        uris = ['gs://{bucket_name}{source_blob_path}']
    );
    """
    job = bq_client.query(query)

def load_wheather_data(
        source_dataset: str,
        source_wheather_table: str,
        source_stations_table: str,
        ti: str = None
) -> None:
    """
    """
    # Fetch variables and data from previous tasks
    BUCKET = Variable.get("BUCKET")
    PATH = Variable.get("EXTRACTION_PATH")
    STORAGE_CREDENTIALS = Variable.get("STORAGE_CREDENTIALS")
    BIGQUERY_CREDENTIALS = Variable.get("BIGQUERY_CREDENTIALS")
    
    wheather_file_name = ti.xcom_pull(key="wheather_file_name", task_ids="extract_data")
    stations_file_name = ti.xcom_pull(key="stations_file_name", task_ids="extract_data")

    # Load files into a GCP bucket
    load_into_gcp_bucket(
        bucket_name=BUCKET,
        source_file_path=PATH + wheather_file_name,
        destination_blob_name=wheather_file_name,
        credentials_file=STORAGE_CREDENTIALS
    )

    load_into_gcp_bucket(
        bucket_name=BUCKET,
        source_file_path=PATH + stations_file_name,
        destination_blob_name=stations_file_name,
        credentials_file=STORAGE_CREDENTIALS
    )

    # Delete local files
    if os.path.exists(PATH + wheather_file_name) and os.path.exists(PATH + stations_file_name):
        os.remove(PATH + wheather_file_name)
        os.remove(PATH + stations_file_name)

    # Load data from GCP bucket files into tables
    # Load wheather data
    load_from_bucket(
        bucket_name=BUCKET,
        source_blob_path="/raw/wheather_*.json",
        destination_dataset=source_dataset,
        destination_table=source_wheather_table,
        credentials_file=BIGQUERY_CREDENTIALS
    )

    # Load stations data
    load_from_bucket(
        bucket_name=BUCKET,
        source_blob_path="/raw/stations_*.json",
        destination_dataset=source_dataset,
        destination_table=source_stations_table,
        credentials_file=BIGQUERY_CREDENTIALS
    )

