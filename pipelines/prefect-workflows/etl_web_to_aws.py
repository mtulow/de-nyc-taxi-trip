import os
import time
import shutil
import pandas as pd
import redshift_connector
from prefect import flow, task
from prefect_aws.s3 import S3Bucket
from sqlalchemy import create_engine



def get_column_names(service: str):
    if service == 'yellow':
        old = ['VendorID','RatecodeID','tpep_pickup_datetime','tpep_dropoff_datetime','PULocationID','DOLocationID']
        new = ['vendorid','ratecodeid','pickup_datetime','dropoff_datetime','pickup_locationid','dropoff_locationid']
    elif service == 'green':
        old = ['VendorID','RatecodeID','lpep_pickup_datetime','lpep_dropoff_datetime','PULocationID','DOLocationID']
        new = ['vendorid','ratecodeid','pickup_datetime','dropoff_datetime','pickup_locationid','dropoff_locationid']
    else:
        raise
    return dict(zip(old,new))
    

@task(retries=3, retry_delay_seconds=5)
def fetch_data(dataset_url: str) -> pd.DataFrame:
    return pd.read_parquet(dataset_url)

@task(retries=3, retry_delay_seconds=5)
def download_data(dataset_url: str, filepath: str):
    if os.path.exists(filepath):
        print('Skipping Download: file already exists...')
        return
    os.system(f"wget {dataset_url} -O {filepath}")
    print()
    return


@task(log_prints=True, retries=3, retry_delay_seconds=5)
def rename_columns(src_filepath: str, service: str, dst_filepath: str=None) -> str:
    # default destination file
    dst_filepath = dst_filepath or src_filepath+'.gz'
    # 
    df = pd.read_parquet(src_filepath)
    df.rename(columns=get_column_names(service), inplace=True)
    print(f'{dst_filepath} columns:\n', '\n '.join(df.columns.tolist()))
    df.to_parquet(dst_filepath, compression='gzip')
    return dst_filepath

@task(retries=3, retry_delay_seconds=5)
def upload_to_s3(src_filepath: str, dst_filepath: str=None):
    # default destination file
    dst_filepath = dst_filepath or src_filepath
    # connect to S3 Bucket
    os.system(f'aws s3 mv {src_filepath} "s3://{dst_filepath}"')
    # s3_bucket = S3Bucket.load("de-nyc-taxi-datalake")
    # s3_bucket.upload_from_path(src_filepath, os.path.dirname(dst_filepath))


@task
def copy_into_redshift(s3_file: str, tablename: str):
    s3_url = f's3://{s3_file}'
    iam_arn = ''
    # Connect to the cluster and create a Cursor
    with redshift_connector.connect(...) as conn:
        with conn.cursor() as cursor:
            # Create a schema
            df = pd.read_parquet(s3_file)
            schema = pd.io.sql.get_schema(df)

            # Create an empty table
            cursor.execute(schema)

            # Use COPY to copy the contents of the S3 bucket into the empty table 
            cursor.execute(f"copy {tablename} from '{s3_url}' iam_role '{iam_arn}' parquet;")

            # Retrieve the contents of the table
            cursor.execute(f"select * from {tablename} limit 100")
            rows = cursor.fetchall()
            data = [row for row in rows]

    return data

@task
def cleanup_data_directory(datadir: str='data'):
    shutil.rmtree(datadir, ignore_errors=True)

@flow
def etl_to_local(service: str, year: int, month: int):
    # Args
    url_prefix = f'https://d37ci6vzurychx.cloudfront.net/trip-data'
    
    filename = f'{service}_tripdata_{year}-{month:02d}'
    
    dataset_url = f'{url_prefix}/{filename}.parquet'
    
    datadir = f'data/{service}/{year}'
    os.makedirs(datadir, exist_ok=True)
    
    raw_file = f'{datadir}/{filename}.parquet'
    local_file = f'{datadir}/{filename}.parquet.gz'
    
    download_data(dataset_url, raw_file)
    
    print()

    rename_columns(raw_file, service, local_file)
    
    



@flow
def etl_web_to_aws(service: str, year: int, month: int, s3_bucket_name: str='nyc-taxi-datalake'):
    # Args
    url_prefix = f'https://d37ci6vzurychx.cloudfront.net/trip-data'
    
    filename = f'{service}_tripdata_{year}-{month:02d}'
    tablename = f'{service}_taxi_trips'
    
    dataset_url = f'{url_prefix}/{filename}.parquet'
    
    datadir = f'data/{service}/{year}'
    os.makedirs(datadir, exist_ok=True)
    
    raw_file = f'{datadir}/{filename}.parquet'
    local_file = f'{datadir}/{filename}.parquet.gz'
    s3_file = f'{s3_bucket_name}/{datadir}/{filename}.parquet.gz'
    
    download_data(dataset_url, raw_file)
    
    print()

    filepath = rename_columns(raw_file, service, local_file)
    
    print()
    
    upload_to_s3(filepath, s3_file)
    
    print()
    
    # copy_into_redshift(s3_file, tablename)

@flow
def local_to_s3(prefix: str, s3_bucket_name: str='nyc-taxi-datalake'):
    datadir = f'{s3_bucket_name}/{prefix}'
    print(f'\nUploading Data Directory:\t{datadir}')
    os.system(f'aws s3 mv "{datadir}/" "s3://{datadir}/"')


@flow
def run_aws_etl_workflow_v1(start_year: int, end_year: int, services: list[str]=['yellow','green'], *, start_month: int=1, end_month: int=12):
    for year in range(start_year,end_year+1):
        for month in range(start_month, end_month+1):    
            for service in services:
                print(f'Running ETL Subflow w/ Args:\t({service}, {year}, {month:02d})')
                etl_web_to_aws(service, year, month)
                print()

    cleanup_data_directory()


@flow
def run_aws_etl_workflow_v2(start_year: int, end_year: int, services: list[str]=['yellow','green'], *, start_month: int=1, end_month: int=12):
    for service in services:
        print(f'Running Subflow for {service}')

        for year in range(start_year,end_year+1):
            for month in range(start_month, end_month+1):
                print()
                etl_to_local(service, year, month)
            
        local_to_s3(prefix=f'data/{service}/{year}', s3_bucket_name='nyc-taxi-datalake')
            
            # print(f'Running ETL for args:\t({year}, {month:02d})')
            # for service in services:
            #     print(f'Running Subflow for {service}')
            #     elt_to_local(service, year, month)
            #     etl_web_to_aws(service, year, month)
            # print()

    cleanup_data_directory()


def main():
    t0 = time.perf_counter()
    run_aws_etl_workflow_v1(2020, 2020, ['yellow', 'green'])
    t1 = time.perf_counter()
    v1_total_time = t1-t0
    
    print()
    os.system(f'aws s3 rm "s3://nyc-taxi-datalake/data/yellow"')
    print()
    os.system(f'aws s3 rm "s3://nyc-taxi-datalake/data/green"')
    print()
    
    t0 = time.perf_counter()
    run_aws_etl_workflow_v2(2020, 2020, ['yellow', 'green'])
    t1 = time.perf_counter()
    v2_total_time = t1-t0

    print(f'version 1 total time:', v1_total_time)
    print(f'version 2 total time:', v2_total_time)
    

if __name__ == '__main__':
    print()
    main()
    print()
