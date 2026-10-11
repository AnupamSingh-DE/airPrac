
from airflow.decorators import dag, task_group, task
from airflow.operators.python import PythonOperator
from pendulum import datetime, duration
from include.datasets import DATASET_COCKTAIL
from include.tasks import _get_cocktail, _check_size, _validate_cocktail_fields
from include.extractor.callbacks import _handle_failed_dag_run, _handle_empty_size
from airflow.utils.trigger_rule import TriggerRule
import json 


@dag(
    start_date=datetime(2026, 9, 2),
    schedule= '@daily',
    default_args = {
        "retries" : 3,
        "retry_delay" : duration(seconds=3)
    }, 
    catchup=False,
    # on_success_callback=None,
    on_failure_callback=_handle_failed_dag_run,
    tags=["Extractor"]
)

def extractor():

    get_cocktail = PythonOperator(
        task_id = 'get_cocktail',
        python_callable = _get_cocktail,
        outlets = [DATASET_COCKTAIL],
        retry_exponential_backoff = True,
        max_retry_delay = duration(minutes=20)

    )

    @task_group
    def checks():
        check_size = PythonOperator(
            task_id = 'check_size',
            python_callable = _check_size,
            on_failure_callback = _handle_empty_size
        )

        validate_fields = PythonOperator(
            task_id = 'validate_fields',
            python_callable = _validate_cocktail_fields,
        )
        # get_cocktail()
        check_size >> validate_fields 

    @task.branch()
    def branch_cocktail_type():
        with open(DATASET_COCKTAIL, 'r') as f:
            data = json.load(f)
            print(f"Data loaded from {DATASET_COCKTAIL}: {data}")

        cocktail = data['drinks'][0]
        if cocktail['strAlcoholic'] == 'Alcoholic':
            return 'alcholic_cocktail'
        return 'non_alcholic_cocktail'

    @task 
    def alcholic_cocktail():
        print("This is an alcoholic cocktail.")
    @task
    def non_alcholic_cocktail():
        print("This is a non-alcoholic cocktail.")

    @task(trigger_rule= TriggerRule.ONE_SUCCESS, templates_dict={'the_current_date':'{{ ds }}'})
    def clean_task(templates_dict):
        import os
        if os.path.exists(DATASET_COCKTAIL.uri):
            os.remove(DATASET_COCKTAIL.uri)

        else:
            print(f"The file {DATASET_COCKTAIL.uri} does not exist.")

        print(f"Cleaned data for {templates_dict['the_current_date']}")

    def store_value(ti = None):
        ti.xcom_pull(task_ids="checks.validate_fields")
    get_cocktail >> checks() >> branch_cocktail_type() >> [alcholic_cocktail(), non_alcholic_cocktail()] >> clean_task()

extractor()