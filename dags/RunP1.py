
import psycopg2
from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
data_path = '/tmp/data_sample'

# import pymysql.cursors


class Config:
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT"))
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_CHARSET = os.getenv("POSTGRES_CHARSET")
    TABLE_NAME = os.getenv("TAR_GET_TABLE_NAME")


db_params = {
    "dbname": Config.POSTGRES_DB,
    "user": Config.POSTGRES_USER,
    "password": Config.POSTGRES_PASSWORD,
    "host": Config.POSTGRES_HOST,
    "port": Config.POSTGRES_PORT
}
# For PythonOperator


def create_table():

    connection = psycopg2.connect(db_params)
    cur = connection.cursor()
    #  sql = f"SELECT * from {Config.TABLE_NAME}"
    cur.execute(f"DROP TABLE IF EXISTS {Config.TABLE_NAME}")
    cur.execute("""
            CREATE TABLE {Config.TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        department_name VARCHAR(255),
        sensor_serial VARCHAR(255),
        create_at DATE,
        product_name VARCHAR(255),
        product_expire DATE
    );
    CREATE INDEX idx_department_name ON MB_my_sample(department_name);
    CREATE INDEX idx_sensor_serial ON MB_my_sample(sensor_serial);
    """)
    connection.commit()
    cur.close()
    connection.close()


def insert_data():
    import pandas as pd
    files = os.listdir(data_path)
    for i, file in enumerate(files):
        df = pd.read_parquet(f'{data_path}/{file}')
        connection = psycopg2.connect(db_params)
        df.to_sql('MB_my_sample', con=connection,
                  if_exists='append', index=False, chunksize=1000)
    connection.commit()
    connection.close()


default_args = {
    'owner': 'datath',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lnwza007',
    default_args=default_args,
    description='Pipeline for ETL bank_term_deposit data',
    schedule_interval=timedelta(days=1),
)


# t1 = PythonOperator(
#     task_id="create_table",
#     python_callable=create_table,
#     dag=dag,
# )
# tasks = []
# for i, file in enumerate(files):
# Create a new PythonOperator that calls the insert_data function


t1 = PythonOperator(
    task_id=f'insert_data',
    python_callable=insert_data,
    # op_kwargs={'file': file},
    dag=dag,
)
t1
# t1 >> t2

# if i > 0:
#     tasks[i] >> t
# tasks.append(t)
# for task in tasks:
#     t1 >> task
