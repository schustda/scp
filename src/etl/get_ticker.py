import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from reagan import PSQL
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests


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
    'schedule_interval': '@weekly',
}

def get_tickers():

    db = PSQL('scp')
    queue = db.to_list('''SELECT ihub_code FROM items.symbol WHERE ticker IS NULL ORDER BY Created_Date DESC''')
    for ihub_code in queue:
        headers = {
                    "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
                }
        url = f"https://investorshub.advfn.com/{ihub_code}"
        r = requests.get(url=url, timeout=60, headers=headers)
        soup = BeautifulSoup(r.content, "lxml")
        data = soup.find(id='ctl00_CP1_bq_fqt_hldq')
        if data:
            try:
                ticker_url = data.get('href','')
                exchange = ticker_url.split('/')[4].lower()
                ticker = ticker_url.split('/')[5].lower()
                db.execute(f'''
                    UPDATE items.symbol 
                    SET exchange = '{exchange}', ticker = '{ticker}', modified_date = NOW()
                    WHERE ihub_code = '{ihub_code}'
                ''')
            except Exception as e:
                print(e)

dag = DAG(
    dag_id='Get_Tickers', default_args=args,catchup=False,
    schedule_interval=timedelta(weeks=1))

t1 = PythonOperator(
task_id='Get_Tickers',
python_callable=get_tickers,
dag=dag)