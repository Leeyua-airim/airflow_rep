from urllib import request

import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# m1 에서만 발생하는 requests.get() 
from _scproxy import _get_proxy_settings
_get_proxy_settings()

dag = DAG(
    dag_id="callable_test",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    # catchup=False
    template_searchpath='/tmp'
)

# 다수의 변수를 받
# https://dumps.wikimedia.org/other/pageviews/2023/2023-08/pageviews-20230801-000000.gz
# https://dumps.wikimedia.org/other/pageviews/2020/2023-09/pageviews-20230903-150000.gz
# https://dumps.wikimedia.org/other/pageviews/2023/2023-09/pageviews-20230903-040000.gz
def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    
    print(f"[확인용] {url}")
    request.urlretrieve(url, output_path) # 데이터를 다운로드하기 위한 

get_data = PythonOperator(
    task_id = "get_data",
    python_callable=_get_data,
    # _get_data 의 ** 를 통해 변수를 딕셔너리롤 제공받게 됨. 글서 다음과 같이 코드를 짤 수 있음
    op_kwargs={
        "year" : "{{ execution_date.year }}",
        "month" : "{{ execution_date.month }}",
        "day" : "{{ execution_date.day }}",
        "hour" : "{{ execution_date.hour-2 }}",
        "output_path" : "/tmp/wikipageviews.gz"
    },
    dag=dag
)

extract = BashOperator(
    task_id = 'extract_gz', 
    # --force 는 파일이 있어도 덮어쓰기 
    # gunzip 압축해제 명령어
    bash_command = 'gunzip --force /tmp/wikipageviews.gz',
    dag = dag 
)

# 페이지 네임은 
def _fetch_pageviews(pagenames):
    # dict.fromkeys() 는 주어진 파라미터를 기반으로 key 를 생성함.
    # 0을 할당하면 value 가 모두 0 이됨 
    # "pagenames" : {"Google", "Amazon", "Apple", "Microsoft", "Facebook" }
    result = dict.fromkeys(pagenames,0)

    print(f"result : {result}")
    # INFO - result : {'Apple': 0, 'Google': 0, 'Microsoft': 0, 'Facebook': 0, 'Amazon': 0}

    # 압축을 해제했으므로 파일명은 그냥 명칭이됨 
    with open(f"/tmp/wikipageviews","r") as f:
        
        # 줄 마다 처리 
        for line in f:

            # 
            domain_code, page_title, view_counts, _ = line.split(" ") # 줄 마다 구분의 기준이 " " 입니다. 

            # 만약 en, 그리고 page_title 에 pagename 이 있는경우 
            if domain_code == "ko" and page_title in pagenames:
                # page_title 을 키로, 
                # view_counts 를 값으로 합니다. 
                result[page_title] = view_counts
    
    print(f"result : {result}")
    # result : {'Apple': '52', 'Google': '466', 'Microsoft': '124', 'Facebook': '607', 'Amazon': '9'}

    # sql 파일 오픈 
    with open("/tmp/postgres_query.sql", 'w') as f:
        for pagename, pageviewcount in result.items() : 
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )

fetch_pageviews = PythonOperator(
    task_id = "fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames" : {
            "유튜브",
            "삼성전자",
            "네이버",
            "우왁굳",
            "구글"
        }
    },
    dag = dag
)

write_to_postgres = PostgresOperator(
    task_id = 'write_to_postgres',
    postgres_conn_id="my_postgres",
    sql="postgres_query.sql",
    dag=dag
)

get_data >> extract >> fetch_pageviews >> write_to_postgres