import os
import elt
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator


def extract_data(**kwargs):
    ti = kwargs['ti']
    print()
    print('Running EXTRACT Task', '\nTask Instance:', ti)
    print()


def transform_data(**kwargs):
    ti = kwargs['ti']
    print()
    print('Running TRANSFORM Task', '\nTask Instance:', ti)
    print()


def load_data(**kwargs):
    ti = kwargs['ti']
    print()
    print('Running LOAD Task', '\nTask Instance:', ti)
    print()




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2019,1,1),
    'catchup': True,
    'retries': 0
}

with DAG(default_args=default_args, schedule='') as dag:

    services = 'yellow', 'green', 'fhv'
    
    for service in services:
        # extract service data task
        extract_task = PythonOperator(
            task_id=f'extract_{service}_data_task',
            python_callable=extract_data,
        )

        # transform service data task
        transform_task = PythonOperator(
            task_id=f'extract_{service}_data_task',
            python_callable=extract_data,
        )

        # load service data task
        load_task = PythonOperator(
            task_id=f'load_{service}_data_task',
            python_callable=extract_data,
        )

        extract_task >> transform_task >> load_task

