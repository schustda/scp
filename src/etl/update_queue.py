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

def dump_to_missing_ids_table():
    ps = PSQL('scp')
    # ps.conn.execute(
    with ps.conn.connect() as con:
        con.execute('''TRUNCATE TABLE ihub.missing_ids''')
        con.execute('''
            INSERT INTO ihub.missing_ids
            SELECT message_id
            FROM ihub.message_sentiment
            WHERE Status IS NULL
        ''')

def add_new_code(message_data, ps):
    if message_data.get('ihub_code',''):
        ihub_code = message_data['ihub_code']

        # fixing an error
        ihub_code = ihub_code.replace('%','%%').replace("'","''")
        
        if not ps.get_scalar(f'''SELECT CASE WHEN '{ihub_code}' IN (SELECT ihub_code FROM items.symbol) THEN 1 ELSE 0 END'''):
            try:
                ihub_id = ihub_code.split('-')[-1]
                symbol = ihub_code.split('-')[-2]
                name = ihub_code.replace(f'-{symbol}-{ihub_id}','').replace('-',' ')
                ps.execute(f'''INSERT INTO items.symbol (symbol, name, ihub_code, status, ihub_id) VALUES ('{symbol}','{name}','{ihub_code}','active','{ihub_id}')''')
            except:
                ps.execute(f'''INSERT INTO items.symbol (ihub_code, status) VALUES ('{ihub_code}','active')''')
            return

# def pull_new_messages(list_num):
def pull_new_messages():
    db = PSQL('scp')
    # queue = db.to_list(f'''SELECT message_id FROM ihub.missing_ids WHERE message_id %% 4 = {list_num} LIMIT 1000''')
    queue = db.to_list(f'''SELECT message_id FROM ihub.missing_ids''')
    ihub = Ihub()
    for message_id in queue:
        message_data = ihub.get_message_data(message_id)
        if message_data.get('ihub_code',''):
            message_data['ihub_code'] = message_data['ihub_code'].replace('%','%%').replace("'","''")
        add_new_code(message_data, db)
        to_update = ','.join([f"{k} = '{str(v)}' " for k,v in message_data.items()])
        db.execute(f'''
            UPDATE ihub.message_sentiment
            SET {to_update}, updated_date = NOW() 
            WHERE message_id = {message_id};
            COMMIT;''')
        sleep(randint(2,3))
    return

dag = DAG(
    dag_id='Pull_New_Messages', default_args=args,catchup=False,
    schedule_interval=timedelta(minutes=60),
    tags=["etl","ihub"]
    )

t1 = PythonOperator(
task_id='dump_to_missing_ids_table',
python_callable=dump_to_missing_ids_table,
dag=dag)

# for i in range(4):
#     t2 = PythonOperator(
#     task_id=f'pull_new_messages_{i}',
#     python_callable=pull_new_messages,
#     op_kwargs={'list_num': i},
#     dag=dag)
    
#     t1 >> t2

t2 = PythonOperator(
task_id=f'pull_new_messages_{i}',
python_callable=pull_new_messages,
dag=dag)

t1 >> t2