import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv
import urllib.parse as parse

from utils import * 

load_dotenv()
VISUALCROSSING_API_KEY = os.getenv("VISUALCROSSING_API_KEY")
STORAGE_CREDENTIALS = os.getenv("STORAGE_CREDENTIALS")
PATH = os.getenv("PROJECT_PATH")
BUCKET = os.getenv("BUCKET")
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

    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{start_date}/{end_date}?unitGroup=metric&include=days&key={VISUALCROSSING_API_KEY}&contentType=json"
    response = requests.get(url)
    
    if response.status_code!=200:
        print('Unexpected Status code: ', response.status_code)
        sys.exit()  

    return response.json()

def extract_wheather_data(start_date: str, end_date: str) -> None:

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
        stations["city"] = city_name

        wheather_df.append(wheather)
        stations_df.append(stations)

    wheather_df = pd.concat(wheather_df)
    stations_df = pd.concat(stations_df)

    wheather_file_name = RAW_PATH + f"wheather_{start_date_str}_{end_date_str}.csv"
    stations_file_name = RAW_PATH + f"stations_{start_date_str}_{end_date_str}.csv"
    
    wheather_df.to_csv(PATH + wheather_file_name)
    stations_df.to_csv(PATH + stations_file_name)

