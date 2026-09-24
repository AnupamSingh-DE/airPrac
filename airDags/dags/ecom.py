from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration

@dag(
    start_date=datetime(2026, 9, 1),
    schedule='@weekly',   #
    catchup=True,
    max_active_runs = 1,
    description="This dags process ecommerce data",
    tags=["TeamAnupam", "ecom"],
    default_args={"retries" : 2},
    dagrun_timeout=duration(minutes=30),
    max_consecutive_failed_dag_runs=2
)

def ecom():
    ta = EmptyOperator(task_id="ta")

# airflow dags backfill -s 2026-10-01 -e 2026-10-05 ecom

ecom()