from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
from load_stock_data import load_koersen_from_ASN

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'mattooren',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 23),
    'schedule_interval': '0 18 * * *',
    'email': ['mattooren@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('ASN_Koersen', default_args=default_args)

# t1, t2, t3 and t4 are examples of tasks created using operators

t5 = PythonOperator(
    task_id='koersen_loader',
    provide_context=True,
    python_callable=load_koersen_from_ASN,
    dag=dag
)
