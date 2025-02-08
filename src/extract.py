import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv
import urllib.parse as parse
from unidecode import unidecode

from utils import * 

load_dotenv()
VISUALCROSSING_API_KEY = os.getenv("VISUALCROSSING_API_KEY")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
PATH = os.getenv("PROJECT_PATH")
RAW_PATH = "raw/"

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

    load_dotenv()
    VISUALCROSSING_API_KEY = os.getenv("VISUALCROSSING_API_KEY")
    
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{start_date}/{end_date}?unitGroup=metric&include=days&key={VISUALCROSSING_API_KEY}&contentType=json"
    response = requests.get(url)
    
    if response.status_code!=200:
        print('Unexpected Status code: ', response.status_code)
        sys.exit()  

    return response.json()

def main(extraction_date: str) -> None:

    extraction_date_str = extraction_date.replace("-", "")

    #Load list of cities for wheather data extraction and prepare
    cities = pd.read_csv(PATH + "cities.txt", names=["city_name"])    
    cities["enconded_city_name"] = [parse.quote(city) for city in cities["city_name"]]

    wheather_df = []
    stations_df = []
    for _, row in cities.iterrows():
        city_name = row["city_name"]
        enconded_city_name = row["enconded_city_name"]
        
        wheather_data = fetch_weather_data(
            city=enconded_city_name,
            start_date=extraction_date,
            end_date=extraction_date
        )

        wheather, stations = format_json_into_dataframe(wheather_data)
        wheather["city"] = city_name
        stations["city"] = city_name

        wheather_df.append(wheather)
        stations_df.append(stations)

    wheather_df = pd.concat(wheather_df)
    stations_df = pd.concat(stations_df)

    wheather_file_name = RAW_PATH + f"wheather_{extraction_date_str}.csv"
    stations_file_name = RAW_PATH + f"stations_{extraction_date_str}.csv"
    
    wheather_df.to_csv(PATH + wheather_file_name)
    stations_df.to_csv(PATH + stations_file_name)

    load_into_gcp_bucket(
        bucket_name="wheather-data",
        source_file_path=PATH + wheather_file_name,
        destination_blob_name=wheather_file_name,
        credentials_file=CREDENTIALS_FILE
    )

if __name__ == "__main__":
    extraction_date = sys.argv[1]
    main(extraction_date)