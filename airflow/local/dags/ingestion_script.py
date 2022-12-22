import os
import shutil
import time
from tkinter.ttk import Progressbar
from typing import Iterable
from urllib.request import urlretrieve

import tqdm
import pandas as pd
from sqlalchemy import create_engine

def connect_to_postgres():
    # 
    username = os.getenv('PG_USERNAME')
    password = os.getenv('PG_PASSWORD')
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    dbname = os.getenv('PG_DATABASE')
    schema = os.getenv('PG_SCHEMA')
    # 
    return create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
    
def get_url(service: str, year: int, month: int)-> str:
    return f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'

def get_filename(service: str, year: int, month: int)-> str:
    return f'{service}_tripdata_{year}-{month:02d}.parquet'

def get_prefix(service: str, year: int, month: int)-> str:
    prefix = f'data/{service}/{year}/{month:02d}'
    os.makedirs(prefix, exist_ok=True)
    return prefix

def get_filepath(service: str, year: int, month: int)-> str:
    prefix = get_prefix(service, year, month)
    filename = get_filename(service, year, month)
    return os.path.join(prefix, filename)

def get_tablename(service: str, year: int, month: int)-> str:
    return f'staging.{service}_tripdata'



def download_data(url: str, filepath: str):
    """
    Download data from URL and store at location of path.
    """
    print('Downloading data...')
    out, msg = urlretrieve(url, filepath)
    shutil.move(out, filepath)
    print('Done!')
    return filepath if os.path.exists(filepath) else out


def upload_to_postgres(filename: str, tablename: str):
    """
    Load data into PostgreSQL Database.
    """
    print(time.strftime('Execution date: %Y/%m/%d %I:%M:%S %p'), tablename, filename)

    engine = connect_to_postgres()
    engine.connect()

    print('connection established successfully, inserting data...')

    t_start = time.perf_counter()

    df = pd.read_parquet(filename, engine='pyarrow')

    df.to_sql(tablename, con=engine, schema='staging', index=False, chunksize=100_000, method='multi')

    t_end = time.perf_counter()

    print('Inserted another {} chunks, took %.3f second.'.format(len(df)/100_000, t_end - t_start))


def main():
    service = 'yellow'
    year = 2021
    month = 1

    url = get_url(service, year, month)
    filepath = get_filepath(service, year, month)
    tablename = get_tablename(service, year, month)

    # 
    filename = download_data(url, filepath)

    # 
    upload_to_postgres(filename, tablename)



if __name__ == '__main__':
    print()
    main()
    print()