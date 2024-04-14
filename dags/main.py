## airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

## Module
from kafka_stream import *
from consumer_in_gcp import *
from consumer_in_image import *

from datetime import datetime, timedelta


default_args = {
    'owner': 'airscholar',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data_json,
        dag=dag
    )

    upload_task = PythonOperator(
        task_id='stream_data_input_bucket',
        python_callable=consumer_to_upload,
        op_kwargs={'max_messages': 50},
        dag=dag
    )

    image_upload = PythonOperator(
        task_id='stream_image_upload_bucket',
        python_callable=upload_to_gcp,
        op_kwargs={'max_messages':50},
        dag=dag
    )

    streaming_task >> upload_task >> image_upload


with DAG('testing_operations',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as test:

    test_oper = PythonOperator(
        task_id='testing_operations',
        python_callable=stream_data_json,
        dag=test
    )


