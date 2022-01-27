import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from reagan import PSQL, Ihub
from datetime import datetime, timedelta
from time import sleep
from random import sample
import pandas as pd


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
    'schedule_interval': '*/10 * * * *',
    'execution_timeout': timedelta(minutes=10)
}

def pull_most_recent():
    postgres_connector = PSQL('scp')
    ihub = Ihub()
    max_post = postgres_connector.get_scalar('''SELECT value_int FROM items.parameters WHERE name = 'max_post_number' ''')
    
    # Search long first
    for i in [10000, 1000, 100]:
        for post_number in sample(range(max_post+i,max_post+(i*2)),10):
            sleep(5)
            resp = ihub.get_message_data(post_number)
            if resp.get('status','') == 'Active':
                postgres_connector.execute(f'''
                    UPDATE items.parameters 
                    SET value_int = {post_number}
                    WHERE name = 'max_post_number'
                ''')
                return

def add_to_log():
    postgres_connector = PSQL('scp')
    max_added = postgres_connector.get_scalar('''SELECT MAX(message_id) FROM ihub.message_sentiment ''')

    # limit to 10,000
    max_post = min(postgres_connector.get_scalar('''SELECT value_int FROM items.parameters WHERE name = 'max_post_number' '''), max_added + 10000)
    if max_post == max_added:
        return

    df = pd.DataFrame(range(max_added+1,max_post+1), columns=['message_id'], dtype=object)
    postgres_connector.to_sql(df,schema='ihub',table='message_sentiment',if_exists='append')
    return

dag = DAG(
    dag_id='Update_New_Messages', default_args=args,catchup=False,
    schedule_interval=timedelta(minutes=10),
    tags=["etl","ihub"]
    )

t1 = PythonOperator(
task_id='Pull_most_recent_post_number',
python_callable=pull_most_recent,
dag=dag)

t2 = PythonOperator(
task_id='Add_post_numbers_to_the_database',
python_callable=add_to_log,
dag=dag)

t1 >> t2