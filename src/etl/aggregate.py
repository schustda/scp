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

    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute('''
    with ih AS (
        SELECT 
            CAST(ms.message_date AS date) date
            ,ticker
            ,AVG(sentiment_polarity) sentiment_polarity
            ,AVG(sentiment_subjectivity) sentiment_subjectivity
            ,COUNT(*) posts
        FROM ihub.message_sentiment ms
        LEFT JOIN items.symbol s ON ms.ihub_code = s.ihub_code
        WHERE exchange = 'usotc'
        GROUP BY CAST(ms.message_date AS date), ticker
    )
    
    INSERT INTO ihub.board_date (date, ticker, sentiment_polarity, sentiment_subjectivity, posts, ohlc, dollar_volume)
    SELECT 
        ph.date
        ,ph.ticker
        ,sentiment_polarity
        ,sentiment_subjectivity
        ,posts
        ,ohlc
        ,dollar_volume
    FROM market.price_history ph
        
    LEFT JOIN ih
    ON ph.ticker = ih.ticker
    AND ph.date = ih.date;
    COMMIT;
    ''')
    return
    
def truncate():

    ps = PSQL('scp')
    # ps.conn.execute(
    with ps.conn.connect() as con:
        con.execute('''TRUNCATE TABLE ihub.board_date; COMMIT;''')
    return

def update():
    ps = PSQL('scp')
    # ps.conn.execute(
    with ps.conn.connect() as con:
        con.execute('''
    with cte AS (
        SELECT date, ticker, RANK() OVER(PARTITION BY date ORDER BY posts DESC) daily_ranking
        FROM ihub.board_date
    )

    UPDATE ihub.board_date bd
    SET daily_ranking = cte.daily_ranking
    FROM cte
    WHERE bd.date = cte.date
    AND bd.ticker = cte.ticker;
    COMMIT;
    ''')
    return
    

dag = DAG(
    dag_id='Aggregate', default_args=args,catchup=False,
    schedule_interval=timedelta(days=1))

t1 = PythonOperator(
task_id='truncate',
python_callable=truncate,
dag=dag)

t2 = PythonOperator(
task_id='insert',
python_callable=insert,
dag=dag)

t3 = PythonOperator(
task_id='update',
python_callable=update,
dag=dag)

t1 >> t2 >> t3