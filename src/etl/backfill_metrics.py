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
    # 'schedule_interval': '@daily',
    'execution_timeout': timedelta(minutes=10)
}

def metrics_production():
    ps = PSQL('scp')
    tickers = ps.to_list('''
        SELECT DISTINCT ticker 
        FROM market.price_history
        WHERE one_wk_avg IS NULL
        AND date < '2021-11-29'
        LIMIT 10
    ''')
    for ticker in tickers:
        with ps.conn.connect() as con:
            con.execute(f'''
                with cte AS (
                    SELECT date, ticker
                        ,AVG(ohlc) OVER(ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS one_wk_avg
                        ,AVG(ohlc) OVER(ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) AS two_wk_avg
                        ,AVG(dollar_volume) OVER(ORDER BY date ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING) AS two_wk_vol 
                    FROM market.price_history
                    WHERE ticker = '{ticker}'
                )        
                UPDATE market.price_history ph
                SET one_wk_avg =  cte.one_wk_avg
                    ,two_wk_avg = cte.two_wk_avg
                    ,two_wk_vol = cte.two_wk_vol
                FROM cte
                WHERE ph.date = cte.date
                AND ph.ticker = cte.ticker;
                COMMIT;
        ''')
    return


dag = DAG(
    dag_id='Backfill_Metrics', default_args=args,catchup=False,
    schedule_interval=timedelta(hours=1)
    )

t1 = PythonOperator(
task_id='metrics_production',
python_callable=metrics_production,
dag=dag)