# %%
# Imports
# =======

import itertools
import os, wget, time, shutil
import pandas as pd
from sqlalchemy import create_engine

# %%
# Utility Functions
# =================

def get_url(service: str, year: int, month: int):
    return f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'

def get_filepath(service: str, year: int, month: int, ext: str=None):
    # Default file extension
    ext = ext or 'parquet'
    # Data directory
    datadir = f'data/{service}/{year}/{month:02d}'
    # Create data directory
    os.makedirs(datadir, exist_ok=True)
    return f'{datadir}/{service}_tripdata_{year}-{month:02d}.{ext}'

def get_s3_bucket_name(service: str, year: int, month: int, bucket_name: str):
    return f's3://{bucket_name}/data/{service}/{year}/{month:02d}'

def get_snowflake_stagename(service: str, year: int, month: int, stage_name: str):
    return 

def get_snowflake_stagepath(service: str, year: int, month: int, stage_name: str):
    return 

def get_tablename(service: str, year: int, month: int):
    return f'{service}_tripdata_{year}_{month:02d}'

# %%
# Extract
# =======

def download_data(url: str, filepath: str):
    outfile = wget.download(url, filepath)
    if os.path.exists(outfile) and (outfile == filepath):
        return True
    return False

# %%
# Transform
# =========

# TODO: write transform functions

# %%
# Load
# ====

def connect_to_postgres():
    user = os.getenv('PG_USER')
    password = os.getenv('PG_PASSWORD')
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    dbname = os.getenv('PG_DATABASE')

    uri = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    return create_engine(uri)

def ingest_parquet_file(filename: str, tablename: str):
    df = pd.read_parquet(filename)
    engine = connect_to_postgres()
    df.to_sql(tablename,con=engine,if_exists='replace',index=None,chunksize=100_000,method='multi')

def ingest_many_files(filename: str, tablename: str):
    datadir = os.path.dirname(filename)
    _, service, year, month = datadir.split('/')
    os.system(f'split {filename}')
    datafiles = os.listdir(datadir)
    print()
    print(datafiles)
    print()
    for f in datafiles:
        print(f)
        fp = os.path.join(datadir, f)
        ingest_parquet_file(fp, tablename)
    
def upload_to_postgres(filename: str, tablename: str):
    size = os.path.getsize(filename)
    print(size)
    print(size <= pow(1024,3))
    if size <= pow(1024,3):
        ingest_parquet_file(filename, tablename)
    else:
        ingest_many_files(filename, tablename)
    

# %%
# ELT Pipeline
# ============

def elt_pipeline(start_year: int=2021, end_year: int=2021, bucket_name: str='de-taxi-tripdata-nyc'):
    """
    Run ELT Pipeline.
    """
    # 
    try:
        # 
        print('','Running ELT Pipeline...')
        services = 'yellow','green','fhv'
        for year, month, service in itertools.product(range(start_year, end_year+1), range(1,12+1), services):
            try:
                # Pipeline Args
                url = get_url(service, year, month)
                filename = get_filepath(service, year, month, 'parquet')
                tablename = get_tablename(service, year, month)

                # EXTRACT
                print('\n', ' Extract '.center(90,'='), '\n')
                print('', f'Downloading file `{os.path.basename(url)}` ...')
                download_data(url, filename)

                # TRANSFORM
                print('\n', ' Transform '.center(90,'='), '\n')
                pass

                # LOAD
                print('\n', ' Load '.center(90,'='), '\n')
                print('', f'Loading to table `{tablename}` ...')
                upload_to_postgres(filename, tablename)

                # END
                print('\n', ''.center(90,'-'), '\n')
            
            except Exception as e:
                print(e)
                continue

    
                
    finally:
        # Cleanup      
        print('\n', ' Cleanup '.center(90,'='), '\n')
        shutil.rmtree('data')


def main():
    start_year, end_year = 2019, 2021
    
    # Run ELT Pipeline
    elt_pipeline(start_year, end_year)




if __name__ == '__main__':
    print()
    main()
    print()