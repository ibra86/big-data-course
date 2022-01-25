from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0  # for dev, 1 - for prod
}
dag = DAG(
    'l7_sample_dag',
    description='Sample DAG for lesson #7',
    # schedule_interval='1 12 * * *' # every dat at 12:01
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 19, 11, 30),
    default_args=default_args
)

t1 = BashOperator(
    task_id='date',
    bash_command='date',
    dag=dag
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    dag=dag
)

t3 = BashOperator(
    task_id='echo',
    bash_command='echo hello',  # python run.py --user=1
    dag=dag
)

t1 >> t2 >> t3
