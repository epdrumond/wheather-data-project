import pandas as pd


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
    