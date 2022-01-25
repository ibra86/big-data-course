from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0  # for dev, 1 - for prod
}
dag = DAG(
    'l7_sample_dag',
    description='API DAG for lesson #7',
    # schedule_interval='1 12 * * *' # every dat at 12:01
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 19, 11, 30),
    default_args=default_args
)


def app(**kwargs):
    process_date = kwargs['ds']  # date-start in airflow


t1 = PythonOperator(
    task_id='print_date',
    python_callable=app,
    dag=dag
)

t1
