"""
A6_shared_lib_break scenario DAG.
Demonstrates usage of shared library functions.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'include'))

from drill_lib.common import parse_data, process_input


def task1_fetch(**context):
    """Fetch data task"""
    return {"status": "success", "data": "sample data"}


def task2_transform(**context):
    """Transform data task"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='t1_fetch')
    processed = process_input(data.get('data'))
    return {"processed": processed}


def task3_parse(**context):
    """Parse data task"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='t2_transform')
    
    if data:
        result = parse_data(data)
        return {"result": result}
    else:
        result = parse_data(None)
        return {"result": result}


with DAG(
    dag_id='A6_shared_lib_break',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['drill', 'example'],
) as dag:
    
    t1 = PythonOperator(
        task_id='t1_fetch',
        python_callable=task1_fetch,
    )
    
    t2 = PythonOperator(
        task_id='t2_transform',
        python_callable=task2_transform,
    )
    
    t3 = PythonOperator(
        task_id='t3_parse',
        python_callable=task3_parse,
    )
    
    t1 >> t2 >> t3
