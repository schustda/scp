import git
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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
    'schedule_interval': '@weekly',
}

def git_pull():
    g = git.cmt.Git('/home/dschuster/airflow/dags/scp')
    g.pull()
    return


dag = DAG(
    dag_id='Refresh_Dags_From_Github', default_args=args,catchup=False,
    schedule_interval=timedelta(hours=1))

t1 = PythonOperator(
task_id='Git_Pull',
python_callable=git_pull,
dag=dag)