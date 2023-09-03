import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='print_context',
    start_date=airflow.utils.dates.days_ago(3),
    catchup=False,
    schedule_interval= '@daily'
)

def _print_context(**kwargs): # 내부에서만 활용될 함수 이다.
    print(kwargs) #

print_context = PythonOperator(
    task_id = "print_context",
    python_callable=_print_context,
    dag = dag
)