import os
import getpass
from sqlalchemy import create_engine

def _postgres_uri():
    username = getpass.getpass('Enter PostgreSQL Username: ')
    password = getpass.getpass('Enter PostgreSQL Username: ')
    host     = os.getenv('PG_HOST', '0.0.0.0')
    port     = os.getenv('PG_PORT', 5432)
    dbname   = os.getenv('PG_DATABASE', 'ny_taxi')
    print()
    return f'postgresql://{username}:{password}@{host}:{port}/{dbname}'

def _snowflake_paramaters():
    print('Opening Snowflake in new tab...')
    os.system('open -n https://app.snowflake.com')
    accountid   = getpass.getpass('Enter Snowflake Account ID: ')
    username    = getpass.getpass('Enter Snowflake Username: ')
    password    = getpass.getpass('Enter Snowflake Password: ')
    print()
    return f'snowflake://{username}:{password}@{accountid}'


def connect_to_postgres():
    uri = _postgres_uri()
    return create_engine(uri)

def connect_to_snowflake():
    uri = _snowflake_paramaters()
    return create_engine(uri)



if __name__ == '__main__':
    print()
    engine = connect_to_snowflake()
    print(engine)
    print()