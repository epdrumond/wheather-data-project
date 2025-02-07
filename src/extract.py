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
PATH=os.getenv("PROJECT_PATH")
RAW_PATH="raw_data/"

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

def main():

    extraction_date = "2025-02-01"
    extraction_date_str = extraction_date.replace("-", "")

    #Load list of cities for wheather data extraction and prepare
    cities = pd.read_csv(PATH + "cities.txt", names=["city_name"])    
    cities["enconded_city_name"] = [parse.quote(city) for city in cities["city_name"]]
    cities["file_city_name"] = [unidecode(city).lower().replace(" ", "") for city in cities["city_name"]]

    for city_name, enconded_city_name, file_city_name in cities.head(2).values:
        print(city_name)
        wheather_data = fetch_weather_data(
            city=enconded_city_name,
            # start_date=extraction_date,
            # end_date=extraction_date
            start_date="2025-01-01",
            end_date="2025-01-02"
        )

        df, stations_df = format_json_into_dataframe(wheather_data)

        file_name = RAW_PATH + f"wheather_{file_city_name}_{extraction_date_str}.csv"
        file_path = PATH + file_name
        df.to_csv(file_path)

        load_into_gcp_bucket(
            bucket_name="wheather-data",
            source_file_path=file_path,
            destination_blob_name=file_name,
            credentials_file=CREDENTIALS_FILE
        )

if __name__ == "__main__":
    main()