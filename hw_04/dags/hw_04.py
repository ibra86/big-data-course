import os
import sys
from contextlib import closing
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))
from main import app


def ff_pg(pg_creds, dump_dir_name):
    with closing(psycopg2.connect(**pg_creds)) as pg_connection:
        cursor = pg_connection.cursor()
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        cursor.execute(sql)
        tables = [z[0] for z in list(cursor)]

        os.makedirs(dump_dir_name, exist_ok=True)
        for t in tables:
            with open(os.path.join(dump_dir_name, f'{t}.csv'), 'w') as csv_file:
                cursor.copy_expert(f'COPY {t} TO STDOUT WITH HEADER CSV', csv_file)


with DAG(
        dag_id='hw_04',
        start_date=datetime(2022, 1, 27, 10, 00)
) as dag:
    t1 = PythonOperator(
        task_id='py_app',
        python_callable=app,
        op_kwargs={"date_list": ['2021-09-04', '2021-01-05']})

    t2 = PythonOperator(
        task_id='py_pg',
        python_callable=ff_pg,
        op_kwargs={"pg_creds": {"host": "localhost", "database": "dshop", "user": "pguser", "password": "secret"},
                   "dump_dir_name": "/home/user/airflow/data/dump_db"}
    )

[t1, t2]
