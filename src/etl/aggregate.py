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
    
    INSERT INTO ihub.board_date (date, ticker, sentiment_polarity, sentiment_subjectivity, posts, ohlc, dollar_volume, one_wk_avg, two_wk_avg, two_wk_vol, target)
    SELECT 
        ph.date
        ,ph.ticker
        ,sentiment_polarity
        ,sentiment_subjectivity
        ,posts
        ,ohlc
        ,dollar_volume
        ,one_wk_avg
        ,two_wk_avg
        ,two_wk_vol
        ,target
    FROM market.price_history ph
        
    LEFT JOIN ih
    ON ph.ticker = ih.ticker
    AND ph.date = ih.date;
    COMMIT;
    ''')
    return
    
def truncate():

    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute('''TRUNCATE TABLE ihub.board_date; COMMIT;''')
    return

def update():
    ps = PSQL('scp')
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
    
def define_target():
    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute('''
    with ticker_dates AS (
        SELECT ticker, MIN(date) ticker_min_date
        FROM ihub.board_date
        GROUP BY ticker
    )

    UPDATE ihub.board_date bd
    SET target = (CASE 
        WHEN one_wk_avg > ohlc * 5 THEN 1
        WHEN one_wk_avg < ohlc THEN 0
        ELSE (one_wk_avg-ohlc)/(ohlc*(5-1))
    END
    +
    CASE
        WHEN two_wk_avg > ohlc * 3 THEN 1
        WHEN two_wk_avg < ohlc THEN 0
        ELSE (two_wk_avg-ohlc)/(ohlc*(3-1))
    END) / 2

    FROM ticker_dates
    WHERE bd.ticker = ticker_dates.ticker

    -- Needs to be 60 days past the first offering. Most of these small cap stocks are crazy volitile at the beginning.
    AND bd.date > ticker_min_date + interval '60 day'

    -- Don't set target on tickers with no price
    AND bd.ohlc > 0

    -- ohlc needs to be greater than .00015 to avoid inactive
    AND two_wk_avg > 0.00015

    -- Ticker is being actively traded
    AND two_wk_vol > 500;
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

t4 = PythonOperator(
task_id='define_target',
python_callable=define_target,
dag=dag)

t1 >> t2 >> t3 >> t4