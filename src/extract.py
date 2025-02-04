import os
import sys
import json
import requests
import pandas as pd
from dotenv import load_dotenv



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
    #return url
    response = requests.get(url)
    
    if response.status_code!=200:
        print('Unexpected Status code: ', response.status_code)
        sys.exit()  

    return response.json()

def main():
    print(fetch_weather_data(
        city="Fortaleza",
        start_date="2025-01-01",
        end_date="2025-01-10"
    ))

if __name__ == "__main__":
    main()