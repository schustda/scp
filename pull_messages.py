import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from reagan import PSQL, Ihub
from datetime import datetime, timedelta
from time import sleep
from random import randint
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
    'schedule_interval': '@hourly',
}

def add_new_code(message_data, ps):
    if message_data.get('ihub_code',''):
        ihub_code = message_data['ihub_code']

        # fixing an error
        ihub_code = ihub_code.replace('%','%%')
        
        if not ps.get_scalar(f'''SELECT CASE WHEN '{ihub_code}' IN (SELECT ihub_code FROM items.symbol) THEN 1 ELSE 0 END'''):
            try:
                ihub_id = ihub_code.split('-')[-1]
                symbol = ihub_code.split('-')[-2]
                name = ihub_code.replace(f'-{symbol}-{ihub_id}','').replace('-',' ')
                ps.execute(f'''INSERT INTO items.symbol (symbol, name, ihub_code, status, ihub_id) VALUES ('{symbol}','{name}','{ihub_code}','active','{ihub_id}')''')
            except:
                ps.execute(f'''INSERT INTO items.symbol (ihub_code, status) VALUES ('{ihub_code}','active')''')
            return

def pull_new_messages(list_num):
    db = PSQL('scp')
    queue = ps.to_list(f'''SELECT message_id FROM ihub.missing_ids WHERE message_id %% 4 = {list_num}''')
    ihub = Ihub()
    for message_id in queue:
        message_data = ihub.get_message_data(message_id)
        add_new_code(message_data, db)
        to_update = ','.join([f"{k} = '{str(v)}' " for k,v in message_data.items()])
        db.execute(f'''UPDATE ihub.message_sentiment SET {to_update}, updated_date = NOW() WHERE message_id = {message_id}''')
        sleep(randint(2,3))
    return

dag = DAG(
    dag_id='Pull_New_Messages', default_args=args,catchup=False,
    schedule_interval=timedelta(minutes=15))

for i in range(4):
    t2 = PythonOperator(
    task_id=f'pull_new_messages_{i}',
    python_callable=pull_new_messages,
    dag=dag)

