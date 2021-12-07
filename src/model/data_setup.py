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
    SELECT idx INTO model.idx1 FROM ihub.board_date WHERE target IS NOT NULL;
    SELECT idx INTO model.idx2 FROM model.idx1 TABLESAMPLE SYSTEM (75);
    SELECT idx INTO model.idx3 FROM model.idx2 TABLESAMPLE SYSTEM (75);

    with wk_tr AS (
        SELECT idx FROM ihub.board_date
        TABLESAMPLE SYSTEM (75)
        WHERE Target IS NOT NULL
    )

    ,md_tr AS (
        SELECT idx FROM wk_tr
    )


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
    ,CASE WHEN idx IN (SELECT idx FROM model.idx2) THEN True ELSE False END AS working_train
    ,CASE WHEN idx NOT IN (SELECT idx FROM model.idx2) THEN True ELSE False END AS working_validation
    ,CASE WHEN idx IN (SELECT idx FROM model.idx3) THEN True ELSE False END AS model_development_train
    ,CASE WHEN idx NOT IN (SELECT idx FROM model.idx3) THEN True ELSE False END AS model_development_test
    FROM ihub.board_date
    WHERE target IS NOT NULL;

    DROP TABLE model.idx1;
    DROP TABLE model.idx2;
    DROP TABLE model.idx3;
    COMMIT;
    ''')
    return
    
def truncate():

    ps = PSQL('scp')
    with ps.conn.connect() as con:
        con.execute('''TRUNCATE TABLE model.combined_data; COMMIT;''')
    return
    
    

dag = DAG(
    dag_id='Model_Data_Setup', default_args=args,catchup=False)

t1 = PythonOperator(
task_id='truncate',
python_callable=truncate,
dag=dag)

t2 = PythonOperator(
task_id='insert',
python_callable=insert,
dag=dag)

t1 >> t2