from itertools import product
from multiprocessing.pool import Pool
import os
import wget
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from argparse import ArgumentParser


def get_args():
    """ 
    Returns any command line arguments.
    """
    parser = ArgumentParser()
    # taxi service
    parser.add_argument('-s', '--service', default='yellow,green')
    # years
    parser.add_argument('-y', '--years', default='2019,2020')
    # months
    parser.add_argument('-m', '--months', dest='months', 
                        default=','.join(map(str,range(1,12+1))))
    args = parser.parse_args()
    taxi_service, years, months = args.service.split(','), map(int, args.years.split(',')), map(int, args.months.split(','))
    return taxi_service, years, months

def construct_web_url(service: str, year: int, month: int)-> str:
    return f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'

def construct_filepath(service: str, year: int, month: int)-> str:
    datadir = f'data/{service}'
    os.makedirs(datadir, exist_ok=True)
    return f'{datadir}/{service}_tripdata_{year}-{month:02d}.parquet'

def construct_tablename(service: str, year: int, month: int)-> str:
    return f'{service}_tripdata'

def fetch_dataset(url: str, filepath: str)-> pd.DataFrame:
    """
    Return data from URL in pandas DataFrame.
    """
    # 
    if os.path.exists(filepath):
        print('File already exists, skipping download...')
        return pd.read_parquet(filepath, engine='fastparquet')
    # 
    elif os.path.exists(filepath.replace('.parquet','.parquet.gz')):
        print('File already exists, skipping download...')
        return pd.read_parquet(filepath+'.gz', engine='fastparquet')
    # 
    else:
        wget.download(url, filepath)
        return pd.read_parquet(filepath, engine='fastparquet')

def rename_data_columns(df: pd.DataFrame, service: str):
    # 
    if service == 'yellow':
        old = ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','RatecodeID','PULocationID','DOLocationID']
        new = ['vendorid','pickup_datetime','dropoff_datetime','ratecodeid','pickup_locationid','dropoff_locationid']
    elif service == 'green':
        old = ['VendorID','lpep_pickup_datetime','lpep_dropoff_datetime','RatecodeID','PULocationID','DOLocationID']
        new = ['vendorid','pickup_datetime','dropoff_datetime','ratecodeid','pickup_locationid','dropoff_locationid']
    # 
    else:
        raise ValueError(f'{service} must be either `yellow` or `green`')
    # 
    df.rename(columns=dict(zip(old,new)), inplace=True)
    return df

def upload_to_postgres(df: pd.DataFrame, tablename: str):
    """
    Load dataframe into PostgreSQL database table.
    """
    # PostgreSQL credentials
    username = os.getenv('PG_USERNAME')
    password = os.getenv('PG_PASSWORD')
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    dbname = os.getenv('PG_DATABASE')
    # connecting to PostgreSQL
    print('\n', 'Connecting to PostgreSQL Database...')
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
    # load into database
    df.to_sql(tablename, con=engine, index=False, chunksize=100_000, if_exists='append')


def spark_elt(args):
    # split args
    year, month, taxi_service = args
    print(f' Running ETL Pipeline for: ({taxi_service}, {year}, {month:02d}) '.center(90,'='))
    
    # construct task args
    url = construct_web_url(taxi_service, year, month)
    filepath = construct_filepath(taxi_service, year, month)
    

def elt_run(args):
    # split args
    year, month, taxi_service = args
    print(f' Running ETL Pipeline for: ({taxi_service}, {year}, {month:02d}) '.center(90,'='))
    
    # construct task args
    url = construct_web_url(taxi_service, year, month)
    filepath = construct_filepath(taxi_service, year, month)
    # tablename = construct_tablename(taxi_service, year, month)

    # Extract Data
    print('\n', ' Extracting Data '.center(90,'='), '\n')
    df = fetch_dataset(url, filepath)

    # Transform Data
    print('\n', ' Tranforming Data '.center(90,'='), '\n')
    rename_data_columns(df, taxi_service)
    df.to_parquet(filepath+'.gz', compression='gzip', index=False)
    os.remove(filepath)

    
    # Loading Data
    # print('\n', ' Loading Data '.center(90,'='), '\n')
    # upload_to_postgres(df, tablename)
    
    # End of Pipeline
    print('\n', ''.center(90,'_'), '\n')

    

def elt_web_to_postgres(taxi_services: list[str], years: list[int], months: list[int]):
    """
    """
    # create a pool of workers
    pool = Pool(4)
    # map the inputs to the download function
    pool.map(elt_run, product(years, months, taxi_services))
    # close and join the pool
    pool.close()
    pool.join()


def main():
    # Load environment variables
    load_dotenv()
    taxi_services, years, months = get_args()

    print(f'{taxi_services = }')
    print(f'{years = }')
    print(f'{months = }')
    
    elt_web_to_postgres(taxi_services=taxi_services, years=years, months=months)
    
    
if __name__ == '__main__':
    print()
    main()
    print()