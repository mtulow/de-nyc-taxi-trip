
import shutil
import urllib.request
from snowflake.connector import Connect
from snowflake.snowpark import Session

# %%%%%%%%%%%%%%%%%
# Utility Functions
# =================

def connect_to_snowflake():
    import os
    os.getenv()
    params = dict()
    return Session.builder.configs(params).create()

def get_url(service: str, year: int, month: int):
    return f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'

def get_prefix(service: str, year: int, month: int):
    return f''

def get_filepath(service: str, year: int, month: int):
    return f''

# %%
# Snowflake Functions
# ==

# def ingest_to_snowflake(stage_name: str, session: Session):
#     session.use_role('svc_user')
#     session.use_warehouse('nyc_ingestion_wh')
#     session.use_database('nyc_taxi')
#     session.use_schema('staging')
#     def wrapper(src_filepath: str, dst_filepath: str):
#         session.use_role()
#         resp = session.file.put(src_filepath, f'@{stage_name}/{dst_filepath}')
        
        
# %%%%%%%%%%%%%%%%%
# Extract Functions
# =================

def fetch_url():
    return

# %%%%%%%%%%%%%%
# Load Functions
# ==============


def upload_to_postgres(url: str, filepath: str, stagename: str):
    print('start running...')
    msg, out = urllib.request.urlretrieve(url)
    shutil.move(out, filepath)
    

    print('end running...')



