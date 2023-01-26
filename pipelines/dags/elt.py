# %%
# Imports
# =======

import itertools
import os
import wget, time, shutil
import pandas as pd
from sqlalchemy import create_engine
import elt_utilities as utils
import connect

# %%
# Extract
# =======

def download_data(url: str, filepath: str):
    outfile = wget.download(url, filepath)
    if os.path.exists(outfile) and (outfile == filepath):
        return True
    return False


def fetch_data(dataset_url: str, service: str)-> pd.DataFrame:
    df = pd.read_parquet(dataset_url)
    df.rename_axis(columns=column_mapper(service), inplace=True)
    return df


# %%
# Transform
# =========

def column_mapper(service: str)-> dict:
    if service == 'yellow':
        mapper = dict([
                ('VendorID','vendorid'),('RatecodeID','ratecodeid'),
                ('PULocationID','pickup_locationid'),('DOLocationID','dropoff_locationid'),
                ('tpep_pickup_datetime','pickup_datetime'),('tpep_dropoff_datetime','dropoff_datetime'),
        ])
    elif service == 'green':
        mapper = dict([
                ('VendorID','vendorid'),('RatecodeID','ratecodeid'),
                ('PULocationID','pickup_locationid'),('DOLocationID','dropoff_locationid'),
                ('lpep_pickup_datetime','pickup_datetime'),('lpep_dropoff_datetime','dropoff_datetime'),
        ])
    else:
        mapper = dict()
        
    return mapper


# %%
# Load
# ====

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
                url = utils.URLs.s3_url(service, year, month)
                # filename = utils.get_filepath(service, year, month, 'parquet')
                # tablename = utils.get_tablename(service, year, month)

                # EXTRACT
                print('\n', ' Extract '.center(90,'='), '\n')
                print('', f'Downloading file `{os.path.basename(url)}` ...')
                fetch_data(dataset_url=url)
                print('Done!')
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