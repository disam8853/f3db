import pymongo
import pandas as pd
from time import sleep

def basic_data_transform(df:pd.DataFrame) -> pd.DataFrame:

    return df

def long_data_transform(df:pd.DataFrame) -> pd.DataFrame:
    for i in range(5):
        print(f'sleep {i}')
        sleep(1)

    # TODO:
    # send post request to  localhost:8999/model/data
    return df
