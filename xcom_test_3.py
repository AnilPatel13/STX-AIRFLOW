from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 1, 0, 0, 0)
}

def find_the_species(**kwargs):

    species = "hello"
    
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key="species", value=species)


def print_the_species(**kwargs):

    ti = kwargs['ti']
    species = ti.xcom_pull(task_ids='find_the_species', key='species')

    print('='*50)
    print(species)
    print('='*50)

with DAG(
    dag_id='xcom_test_v1',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1
) as dag:

    task_find_the_species = PythonOperator(
        task_id='find_the_species',
        python_callable=find_the_species,
        provide_context=True
    )

    task_print_the_species = PythonOperator(
        task_id='print_the_species',
        python_callable=print_the_species,
        provide_context=True
    )

    task_find_the_species >> task_print_the_species
