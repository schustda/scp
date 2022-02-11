import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

class Email:

    def __init__(self):
        super().__init__()
        with open('src/email_address.txt') as f:
            self.email_address = f.read().strip()
        with open('src/email_password.txt') as f:
            self.email_password = f.read().strip()

    def _email_specifics(self):
        subject = 'Tigers Promtions Available'
        body = 'https://www.mlb.com/tigers/tickets/promotions'
        with open('src/email_recipients.txt') as f:
            to_lst = [f.read().strip()]

        return subject, body, to_lst

    def send_email(self):
        subject, body, distribution_list = self._email_specifics()
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = self.email_address

        for to_email in distribution_list:
            msg['To'] = to_email
            msg.attach(MIMEText(body, 'plain'))
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email_address, self.email_password)
            text = msg.as_string()
            server.sendmail(self.email_address, to_email, text)
            server.quit()

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
    'execution_timeout': timedelta(minutes=10)
}


def tigers():
    url = 'https://www.mlb.com/tigers/tickets/promotions'
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    z = soup.find(class_="p-alert__text")
    try:
        if z.text != 'Please check back for 2022 information.':
            Email().send_email()
    except:
        Email().send_email()

dag = DAG(
    dag_id='Tigers_Check', default_args=args,catchup=False,
    schedule_interval=timedelta(days=1),
    )

t1 = PythonOperator(
task_id='tigers',
python_callable=tigers,
dag=dag)