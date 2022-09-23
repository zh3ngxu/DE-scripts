from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def _fetch_data():
    print("***fetch_data...***")
    if True:
        return "transform_new_fetched_data"
    else:
        return "do_nothing"

def _transform_data():
    print("***transforming data...")
    return True

def _load_to_db():
    print("***loading to DB...")
    return True

with DAG(
    'my_first_dag',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    t0 = DummyOperator(
        task_id="dag_start"
    )

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BranchPythonOperator(
        task_id="fetch_some_data",
        python_callable=_fetch_data
    )

    t3 = PythonOperator(
        task_id="transform_new_fetched_data",
        python_callable=_transform_data
    )

    t4 = DummyOperator(
        task_id="do_nothing"
    )

    t5 = PythonOperator(
        task_id="load_to_db",
        python_callable=_load_to_db,
        trigger_rule="one_success"
    )

    t0 >> t1 >> t2 >> [t3,t4] >> t5