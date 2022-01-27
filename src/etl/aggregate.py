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
    'schedule_interval': '@daily',
}

def insert():

    postgres_connector = PSQL('scp')
    with postgres_connector.conn.connect()as database_connection:
        database_connection.execute('''
    INSERT INTO ihub.board_date (date, ticker, sentiment_polarity, sentiment_subjectivity, posts, daily_ranking, ohlc, dollar_volume, one_wk_avg, two_wk_avg, two_wk_vol, target)
    SELECT bd.date,bd.ticker,sentiment_polarity,sentiment_subjectivity,posts,daily_ranking,ohlc,dollar_volume,one_wk_avg,two_wk_avg,two_wk_vol,t.target
    FROM ihub.vBoard_date bd
    LEFT JOIN ihub.vTarget t
    ON bd.ticker = t.ticker
    AND bd.date = t.date;
    COMMIT;
    ''')
    return
    
def truncate():

    postgres_connector = PSQL('scp')
    with postgres_connector.conn.connect()as database_connection:
        database_connection.execute('''TRUNCATE TABLE ihub.board_date; COMMIT;''')
    return
    
    

dag = DAG(
    dag_id='Aggregate', default_args=args,catchup=False,
    schedule_interval=timedelta(days=1),
    tags=["etl"]
    )

t1 = PythonOperator(
task_id='truncate',
python_callable=truncate,
dag=dag)

t2 = PythonOperator(
task_id='insert',
python_callable=insert,
dag=dag)

t1 >> t2