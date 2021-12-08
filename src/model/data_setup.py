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

def create_staging_tables():
    ps = PSQL('scp')
    with ps.conn.connect() as con:
        for tbl_name in ['idx1','idx2','idx3']:
            con.execute(f'''CREATE TABLE staging.{tbl_name} (idx int);
            COMMIT;''')
    return

def fill_staging_table1():
    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute(f'''INSERT INTO staging.idx1 (idx) SELECT idx FROM ihub.board_date WHERE target IS NOT NULL;
        COMMIT;''')
    return

def fill_staging_table2():
    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute(f'''INSERT INTO staging.idx2 (idx) SELECT idx FROM staging.idx1 WHERE target IS NOT NULL;
        COMMIT;''')
    return

def fill_staging_table3():
    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute(f'''INSERT INTO staging.idx3 (idx) SELECT idx FROM staging.idx2 WHERE target IS NOT NULL;
        COMMIT;''')
    return

def insert():

    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute('''
    INSERT INTO model.combined_data (idx,ohlc,dollar_volume,posts,sentiment_polarity,sentiment_subjectivity,daily_ranking,target,working_train,working_validation,model_development_train,model_development_test)
        
    SELECT
    idx
    ,ohlc
    ,dollar_volume
    ,posts
    ,sentiment_polarity
    ,sentiment_subjectivity
    ,daily_ranking
    ,target
    ,CASE WHEN idx IN (SELECT idx FROM staging.idx2) THEN True ELSE False END AS working_train
    ,CASE WHEN idx NOT IN (SELECT idx FROM staging.idx2) THEN True ELSE False END AS working_validation
    ,CASE WHEN idx IN (SELECT idx FROM staging.idx3) THEN True ELSE False END AS model_development_train
    ,CASE WHEN idx NOT IN (SELECT idx FROM staging.idx3) THEN True ELSE False END AS model_development_test
    FROM ihub.board_date
    WHERE target IS NOT NULL;
    COMMIT;
    ''')
    return
    
def truncate():

    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute('''TRUNCATE TABLE model.combined_data; COMMIT;''')
    return
    
def drop_staging_table():
    ps = PSQL('scp')
    with ps.conn.connect() as con:
        for tbl_name in ['idx1','idx2','idx3']:
            con.execute(f'''DROP TABLE staging.{tbl_name}; COMMIT;''')
    return

dag = DAG(
    dag_id='Model_Data_Setup', default_args=args,catchup=False)

t1 = PythonOperator(
task_id='truncate',
python_callable=truncate,
dag=dag)

t2 = PythonOperator(
task_id='create_staging_tables',
python_callable=create_staging_tables,
dag=dag)

t3 = PythonOperator(
task_id='fill_staging_table1',
python_callable=fill_staging_table1,
dag=dag)

t4 = PythonOperator(
task_id='fill_staging_table2',
python_callable=fill_staging_table2,
dag=dag)

t5 = PythonOperator(
task_id='fill_staging_table3',
python_callable=fill_staging_table3,
dag=dag)

t6 = PythonOperator(
task_id='insert',
python_callable=insert,
dag=dag)

t7 = PythonOperator(
task_id='drop_staging_table',
python_callable=drop_staging_table,
dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7