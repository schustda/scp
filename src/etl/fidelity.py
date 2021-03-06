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
    
    postgres_connector = PSQL('scp')
    f = Fidelity()
    queue = postgres_connector.to_list(f'''
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
        ticker_to_staging(f, postgres_connector, ticker)

    return

def create_staging():

    postgres_connector = PSQL('scp')
    with postgres_connector.conn.connect() as database_connection:
        database_connection.execute(f'''
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
    postgres_connector = PSQL('scp')
    with postgres_connector.conn.connect() as database_connection:
        database_connection.execute(f'''
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
    postgres_connector = PSQL('scp')
    with postgres_connector.conn.connect() as database_connection:
        database_connection.execute(f'''
        DROP TABLE staging.price_history;
        COMMIT; ''')
    return

def business_rules_staging():
    postgres_connector = PSQL('scp')
    with postgres_connector.conn.connect() as database_connection:
        database_connection.execute(f'''
        UPDATE staging.price_history
        SET 
            ohlc = (open+high+low+close)/4
            ,dollar_volume = (open+high+low+close)*volume/4;
        COMMIT;
    ''')
    return

def create_staging_for_metrics():
    postgres_connector = PSQL('scp')
    with postgres_connector.conn.connect() as database_connection:
        database_connection.execute(f'''
            DROP TABLE IF EXISTS staging.price_history_metrics;
            CREATE TABLE staging.price_history_metrics (
                date date
                ,ticker varchar(100)
                ,one_wk_avg float
                ,two_wk_avg float
                ,two_wk_vol float
            );
            COMMIT;
        ''')
        database_connection.execute(f'''
            DROP TABLE IF EXISTS staging.backfilled_tickers;
            CREATE TABLE staging.backfilled_tickers (ticker varchar(100));
            COMMIT;
        ''')
        database_connection.execute(f'''
            INSERT INTO staging.backfilled_tickers (ticker)
            SELECT DISTINCT ticker
            FROM market.price_history
            WHERE one_wk_avg IS NOT NULL;
            COMMIT;
        ''')

        database_connection.execute(f'''
            UPDATE items.parameters 
            SET value_date = (
                SELECT MIN(date) + interval '-12 days' min_date
                FROM market.price_history ph
                INNER JOIN staging.backfilled_tickers bt
                ON ph.ticker = bt.ticker
                WHERE one_wk_avg IS NULL
            )
            WHERE name = 'ticker_recent_date';
            COMMIT;
        ''')
    return

def drop_staging_for_metrics():
    postgres_connector = PSQL('scp')
    with postgres_connector.conn.connect() as database_connection:
        database_connection.execute(f'''
            DROP TABLE staging.price_history_metrics;
            DROP TABLE staging.backfilled_tickers;
            COMMIT;
    ''')
    return

def populate_staging_for_metrics():
    postgres_connector = PSQL('scp')
    with postgres_connector.conn.connect() as database_connection:
        database_connection.execute(f'''
            INSERT INTO staging.price_history_metrics
            SELECT date, ph.ticker
                ,AVG(ohlc) OVER(ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS one_wk_avg
                ,AVG(ohlc) OVER(ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) AS two_wk_avg
                ,AVG(dollar_volume) OVER(ORDER BY date ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING) AS two_wk_vol 
            FROM market.price_history ph
            LEFT JOIN staging.backfilled_tickers bt
            ON ph.ticker = bt.ticker
            WHERE bt.ticker IS NULL
            OR date > (SELECT value_date FROM items.parameters WHERE name = 'ticker_recent_date');
            COMMIT; 
    ''')
    return


def metrics_production():
    postgres_connector = PSQL('scp')
    with postgres_connector.conn.connect() as database_connection:
        database_connection.execute(f'''
            UPDATE market.price_history ph
            SET one_wk_avg =  cte.one_wk_avg
                ,two_wk_avg = cte.two_wk_avg
                ,two_wk_vol = cte.two_wk_vol
            FROM staging.price_history_metrics cte
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
task_id='create_staging_for_metrics',
python_callable=create_staging_for_metrics,
dag=dag)

t7 = PythonOperator(
task_id='populate_staging_for_metrics',
python_callable=populate_staging_for_metrics,
dag=dag)

t8 = PythonOperator(
task_id='metrics_production',
python_callable=metrics_production,
dag=dag)

t9 = PythonOperator(
task_id='drop_staging_for_metrics',
python_callable=drop_staging_for_metrics,
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

t6 >> t7 >> t8 >> t9