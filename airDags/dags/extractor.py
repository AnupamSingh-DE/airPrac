
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from pendulum import datetime
from include.datasets import DATASET_COCKTAIL
from include.tasks import _get_cocktail, _check_size

@dag(
    start_date=datetime(2026, 9, 2),
    schedule= '@daily',
    catchup=False
)

def extractor():

    get_cocktail = PythonOperator(
        task_id = 'get_cocktail',
        python_callable = _get_cocktail,
        outlets = [DATASET_COCKTAIL]

    )

    check_size = PythonOperator(
        task_id = 'check_size',
        python_callable = _check_size
    )

    get_cocktail >> check_size

extractor()