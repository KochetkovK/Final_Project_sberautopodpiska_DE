import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator


path = os.path.expanduser('~/Документы/Final_Project_sberautopodpiska_DE')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)


from modules.insert_json import clean_json
from modules.insert_json import insert_json_to_sql

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 6, 27),
}

with DAG(
        dag_id='sberautopodpiska_insert_json_to_sql',
        schedule=None,
        default_args=args,
) as dag:
    clean_json = PythonOperator(
        task_id='clean_json',
        python_callable=clean_json,
    )

    insert_json_to_sql = PythonOperator(
        task_id='insert_json',
        python_callable=insert_json_to_sql,
    )

    clean_json >> insert_json_to_sql


