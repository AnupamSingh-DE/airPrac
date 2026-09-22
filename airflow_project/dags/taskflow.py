from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime


# def _task_a():
#     print("Task A is running")
#     return 42

# def _task_b(ti=None):
#     print("Task B is running")
#     print(f"Task A returned: {ti.xcom_pull(task_ids='_task_a')}")
  
def _task_a():
    print("Task A is running")
    return 42


@dag(
    start_date= datetime(2026,9,4),
    schedule = '@daily',
    catchup = False,
    tags = ['taskflow']
)

def taskflow():

    task_a = PythonOperator(
        task_id= 'task_a',
        python_callable= _task_a
    )
    # @task   
    # def task_a():
    #     print("Task A is running")
    #     return 42
    @task
    def task_b(value):
        print("Task B is running")
        print(value)

    task_b(task_a.output)



taskflow()