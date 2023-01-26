# %%
# Imports
# =======

import os
import pandas as pd
from pathlib import Path


class URLs:
    @classmethod
    def source_url(cls, service: str, year: int, month: int):
        """Return the data page url: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"""
        return f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'

    @classmethod
    def s3_url(cls, service: str, year: int, month: int):
        """Return the S3 bucket uri: s3://nyc-tlc/trip data/"""
        return f's3://nyc-tlc/trip data/{service}_tripdata_{year}-{month:02d}.parquet'



# %%
# Local Filesystem
# ================

def get_datadir(service: str, year: int, month: int, datadir: str='data'):
    return f'{datadir}/{service}/{year}/{month:02d}'

def get_local_filepath(service: str, year: int, month: int):
    # local path to store file
    datadir = str(Path(__file__).absolute().parent.parent / f'data/{service}/{year}/{month:02d}')
    # create local directory
    os.makedirs(datadir, exist_ok=True)
    # return file location
    return f'{datadir}/{service}_tripdata_{year}-{month:02d}.parquet'

# %%
# Data Lake
# =========

def get_s3_prefix(service: str, year: int, month: int, *, bucket_name: str='nyc-taxi-datalake', subdir: str='data'):
    return f'{bucket_name}/{subdir}/{service}/{year}/{month:02d}'

def get_s3_filepath(service: str, year: int, month: int, *, bucket_name: str='nyc-taxi-datalake', subdir: str='data', absolute: bool=False):
    if absolute:
        return f's3://{bucket_name}/{subdir}/{service}/{year}/{month:02d}'
    return f'{bucket_name}/{subdir}/{service}/{year}/{month:02d}'

# %%
# Data Warehouse
# ==============


def get_tablename(service: str, year: int, month: int):
    return f'{service}_taxi_trips'

def get_stage_path(service: str, year: int, month: int):
    return f'{service}/{year}/{month:02d}'


# %%
# Program
# =======

def main():
    url = URLs.s3_url('yellow', 2021, 1)
    print('Data File:', url)
    
    print()
    df = pd.read_parquet(url, engine='pyarrow')
    df.info()
    print()

if __name__ == '__main__':
    print()
    main()
    print()