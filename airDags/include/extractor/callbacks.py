def _handle_failed_dag_run(context):
    print(
    f"DAG run failed with the task {context['task_instance'].task_id} "
    f"for the data interval between "
    f"{context['data_interval_start']} and {context['data_interval_end']}"
)

def _handle_empty_size(context):
    print(f"There is no cocktail to process for the data between interval {context["data_interval_start"]} and {context["data_interval_end"]}")
