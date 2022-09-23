from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3

def _fetch_data(ti):
    print("***fetching data***")
    if True:
        ti.xcom_push(key="s3_location",value="s3://raw_file_landing_zone")
        return "transform_new_fetched_data"
    else:
        return "do_nothing"

def _transform_data(ti,**kwargs):
    print("***processing data***")
    s3_location = ti.xcom_pull(key="s3_location")
    print(f"***parsed s3 bucket: {s3_location}")
    print(kwargs)

def _load_to_s3(filename, key, bucketname):
    hook = S3Hook("my_S3_conn")
    hook.load_file(filename, key, bucketname)
    # s3 = boto3.resource('s3')

    # def upload_file_to_S3(filename, key, bucket_name):
    #     s3.Bucket(bucket_name).upload_file(filename, key)
        

with DAG(
    'demo',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'depends_on_past':True,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    },
    description='A simple tutorial DAG',
    schedule_interval="0 8 * * *",
    start_date=datetime(2022, 9, 10),
    catchup=True,
    max_active_runs=1,
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

    t3_1 = BashOperator(
        task_id="output_result_file",
        bash_command="touch /tmp/{{  ds  }}.txt"
    )

    t4 = DummyOperator(
        task_id="do_nothing"
    )

    t5 = PythonOperator(
        task_id="load_to_s3",
        python_callable=_load_to_s3,
        op_kwargs={
            "filename": "/tmp/{{  ds  }}.txt",
            "key":"data/{{  ds  }}.txt",
            "bucketname": "wcddeb4-demo"
        },
        trigger_rule="one_success"
    )

    t0 >> t1 >> t2 >> [t3, t4]
    t3 >> t3_1
    [t3_1, t4] >> t5