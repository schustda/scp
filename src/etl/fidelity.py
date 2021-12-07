import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from reagan import PSQL, Fidelity
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


def ticker_to_staging(f, ps, ticker):

    try:
        df = f.pull_data(ticker)
    except Exception as e:
        print(e, ticker)
        ps.execute(f'''
            UPDATE items.symbol
            SET price_data_available = CAST(0 as bit)
            WHERE ticker = '{ticker}';
            COMMIT;
        ''')
        return
    max_date = ps.get_scalar(f"SELECT MAX(date) FROM market.price_history WHERE ticker = '{ticker}' ")
    if max_date:
        df = df[df.date > max_date]
        df = df[df.date < datetime.today().date()]
    df['ticker'] = ticker
    ps.to_sql(df,schema='staging',table=f'price_history',if_exists='append')
    return

def fidelity_to_staging(list_num):
    
    ps = PSQL('scp')
    f = Fidelity()
    queue = ps.to_list(f'''
        SELECT ticker FROM (
            SELECT ticker, symbol_id %% 4 r
            FROM (
                SELECT ticker, min(symbol_id) symbol_id 
                FROM items.symbol 
                WHERE exchange = 'usotc' 
                AND price_data_available = CAST(1 as bit)
                GROUP BY ticker
            ) x
        ) y
        WHERE r = {list_num} ''')
    
    for ticker in queue:
        ticker_to_staging(f, ps, ticker)

    return

def create_staging():

    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute(f'''
        DROP TABLE IF EXISTS staging.price_history;

        CREATE TABLE staging.price_history (
            date date,
            open double precision,
            high double precision,
            low double precision,
            close double precision,
            volume bigint,
            ticker varchar(100),
            ohlc double precision,
            dollar_volume double precision
        );
        COMMIT;
        
        ''')
    return

def insert():
    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute(f'''
            INSERT INTO market.price_history (date,open,high,low,close,volume,ticker,ohlc,dollar_volume)
            SELECT DISTINCT a.*
			FROM staging.price_history a
			LEFT JOIN market.price_history b
			ON a.ticker = b.ticker
			AND a.date = b.date
			WHERE b.ticker IS NULL;
            COMMIT;
        ''')
    return

def drop_staging():
    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute(f'''
        DROP TABLE staging.price_history;
        COMMIT; ''')
    return

def business_rules_staging():
    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute(f'''
        UPDATE staging.price_history
        SET 
            ohlc = (open+high+low+close)/4
            ,dollar_volume = (open+high+low+close)*volume/4;
        COMMIT;
    ''')
    return

def metrics_production():
    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute(f'''
            with backfilled_tickers AS (
                SELECT ticker
                FROM market.price_history
                WHERE one_wk_avg IS NOT NULL
            )
            ,recent_date AS (
                SELECT MIN(date) min_date
                FROM market.price_history
                WHERE ticker IN (SELECT ticker FROM backfilled_tickers)
                AND one_wk_avg IS NULL
            )
            ,cte AS (
                SELECT date, ticker
                    ,AVG(ohlc) OVER(ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS one_wk_avg
                    ,AVG(ohlc) OVER(ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) AS two_wk_avg
                    ,AVG(dollar_volume) OVER(ORDER BY date ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING) AS two_wk_vol 
                FROM market.price_history
                WHERE ticker NOT IN (SELECT ticker FROM backfilled_tickers)
                OR date > (SELECT min_date FROM recent_date) interval '-12 days'
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
    dag_id='Price_History', default_args=args,catchup=False,
    schedule_interval=timedelta(days=1),
    tags=["etl","stock"]
    )

t1 = PythonOperator(
task_id='create_staging',
python_callable=create_staging,
dag=dag)

t3 = PythonOperator(
task_id='business_rules_staging',
python_callable=business_rules_staging,
trigger_rule='all_done',
dag=dag)

t4 = PythonOperator(
task_id='insert',
python_callable=insert,
dag=dag)

t5 = PythonOperator(
task_id='drop_staging',
python_callable=drop_staging,
dag=dag)

t6 = PythonOperator(
task_id='metrics_production',
python_callable=metrics_production,
dag=dag)

for i in range(4):
    t2 = PythonOperator(
    task_id=f'fidelity_to_staging_{i+1}',
    python_callable=fidelity_to_staging,
    op_kwargs={'list_num': i},
    dag=dag)
    
    t1 >> t2 >> t3

t3 >> t4 >> t5

t4 >> t6