import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner' : 'airim',
}

#DAG 생성
dag = DAG(
    dag_id='wiki_pipeline',
    tags=['wiki'],
    default_args=default_args,
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly",
    catchup=False
)

# 데이터를 받아올 경로
# https://dumps.wikimedia.org/other/pageviews/2023/2023-08/pageviews-20230801-000000.gz
get_data = BashOperator(
    task_id = 'get_data', #task_id 설정 
    bash_command=(
        "curl -o /tmp/wikipageviews.gz 'https://dumps.wikimedia.org/other/pageviews/" # -o : output 의 약자로, 데이터를 저장할 위치를 지정 
        "{{ execution_date.year }}/"                  # url 경로에 맞춰 실행시점의 '연도' 로 치환될 예정 
        "{{ execution_date.year }}-{{ '{:02}'.format(execution_date.month) }}/"  # url 경로에 맞춰 실행시점의 '연도 -' 로 치환될 예정  
        "pageviews-{{ execution_date.year }}"          # 
        "{{ '{:02}'.format(execution_date.month) }}"
        "{{ '{:02}'.format(execution_date.day) }}-"
        "{{ '{:02}'.format(execution_date.hour) }}0000.gz'"
    ),
    dag = dag
)