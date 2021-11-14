import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from reagan import PSQL
from datetime import datetime, timedelta


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 12, 1),
    'email': ['dougals.schuster303@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10)
}

def get_tickers():

    PSQL('scp').execute('''
    
    UPDATE staging.testing_table
    SET date = NOW();
    
    COMMIT;
    
    ''')
    

dag = DAG(
    dag_id='Get_Tickers', default_args=args,catchup=False,
    schedule_interval=timedelta(days=1))

t1 = PythonOperator(
task_id='Get_Tickers',
python_callable=get_tickers,
dag=dag)