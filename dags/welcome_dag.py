from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import json

def print_hello_func():
    print('Hello, Welcome to Airflow!')

def print_date_func():
    print('Today is {}'.format(datetime.today().date()))

def print_random_quote_func():
    r = requests.get('https://api.quotable.io/random')
    q = r.json()['content']
    print('Quote of the day: "{}"'.format(q))



dag = DAG(

    'welcome_dag',

    default_args={'start_date': days_ago(1)},

    schedule_interval='0 23 * * *',

    catchup=False

)

print_hello = PythonOperator(
    task_id = 'Print_hello',
    python_callable = print_hello_func,
    dag = dag 
)

print_date = PythonOperator(
    task_id = 'Print_date',
    python_callable = print_date_func,
    dag = dag 
)

print_random_quote = PythonOperator(
    task_id = 'print_random_quote',
    python_callable = print_random_quote_func,
    dag = dag 
)

print_hello >> print_date >> print_random_quote