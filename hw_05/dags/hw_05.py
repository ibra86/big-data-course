import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.dump_pg_to_hdfs import dump_pg_to_hdfs

sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))
from main import app

with DAG(
        dag_id='hw_05',
        schedule_interval='@daily',
        start_date=datetime(2022, 2, 6, 00, 00)
) as dag:
    t1 = PythonOperator(
        task_id='py_app',
        python_callable=app,
        provide_context=True,
        op_kwargs={"date_list": []})

    t2 = PythonOperator(
        task_id='py_pg',
        python_callable=dump_pg_to_hdfs,
        op_kwargs={"pg_creds": {"host": "localhost", "database": "dshop_bu", "user": "pguser", "password": "secret"},
                   "dump_hdfs_dir_name": "/hw_05"}
    )

[t1, t2]
