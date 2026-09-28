
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from pendulum import datetime
from airflow import Dataset

cocktail = Dataset('tmp/cocktail.json')

def _get_cocktail():
    import requests

    api = "https://www.thecocktaildb.com/api/json/v1/1/random.php"
    response = requests.get(api)
    with open(cocktail) as f:
        f.write(response.json())

@dag(
    start_date='2026,9,20',
    schedule= '@daily',
    catchup=False
)

def extractor():

    get_cocktail = PythonOperator(
        task_id = 'get_cocktail',
        python_callable = _get_cocktail
    )


extractor()