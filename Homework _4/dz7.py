import json
import os
from datetime import datetime
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',

    'email': ['vadim.panko@outlook.com'],
    'email_on_failure': False,
    'retries': 0
}

dag = DAG(
    'dz7_dag',
    description='dag load from API and Postgres through airflow',
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 26, 10, 00),
    default_args=default_args
)

url = 'https://robot-dreams-de-api.herokuapp.com'
endpoint_auth = '/auth'
headers_auth = {'content-type': 'application/json'}
payload_auth = {'username': 'rd_dreams', 'password': 'djT6LasE'}
endpoint_API = '/out_of_stock'
filename = 'Robot-dreams data'
headers = {'content-type': 'application/json', 'Authorization': 'JWT '}


def app(**kwargs):
    process_date = kwargs['ds']

    r = requests.post(url=url + endpoint_auth, headers=headers_auth,
                      data=json.dumps(payload_auth))
    token = r.json()['access_token']

    headers = {'content-type': 'application/json',
               'Authorization': 'JWT ' + token}

    r = requests.get(url=url + endpoint_API, headers=headers,
                     data=json.dumps({'date': process_date}))

    path_to_dir = os.path.join('/home/user/shared_folder/data/', process_date)
    os.makedirs(path_to_dir, exist_ok=True)

    data = r.json()

    with open(os.path.join(path_to_dir, filename), 'w') as json_file:
        json.dump(data, json_file)


pg_creds = {
    "host": "192.168.0.149",
    "port": 5432,
    "database": "dshop",
    "user": "pguser",
    "password": "secret"
}


def read_pg(**kwargs):
    process_date = kwargs['ds']
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        cursor.execute(sql)
        data = cursor.fetchall()

    tables_to_load = []

    for element in data:
        tables_to_load.append(element[0])

    path_to_dir = os.path.join('/home/user/shared_folder/data/', process_date)
    os.makedirs(path_to_dir, exist_ok=True)

    for table in tables_to_load:
        with psycopg2.connect(**pg_creds) as pg_connection:
            cursor = pg_connection.cursor()

            with open(os.path.join(path_to_dir, f'{table}.csv'), 'w') as csv_file:
                cursor.copy_expert(f'COPY public.{table} TO STDOUT WITH HEADER CSV', csv_file)


t1 = PythonOperator(
    task_id='load_data_from_pg',
    python_callable=read_pg,
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id='load_data_from_API',
    python_callable=app,
    provide_context=True,
    dag=dag,
)
