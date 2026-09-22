from airflow.decorators import dag
from airflow.operatos.python import PythonOperator
from pendulum import datetime, delta

@dag(
    start_date=datetime(2026, 10, 1),
    schedule='@daily',   #
    catchup=False,
    max_active_runs = 1,
    description="This dags process ecommerce data",
    tags=["TeamAnupam","ecom"]
    default_args={"retries" : 2},
    dagrun_timeout=timedelta(minnutes=30),
    max_consecutive_failed_dag_runs=2
)

def ecom():
    ta = PythonOperator(task_id="ta")

    tb = pythonOperator(task_id="tb")

# airflow dags backfill -s 2026-10-01 -e 2026-10-05 ecom