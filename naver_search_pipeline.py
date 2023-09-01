# 데이터 처리를 위한 라이브러리 
from datetime import datetime 
import json

# m1 에서만 발생하는 requests.get() 
from _scproxy import _get_proxy_settings
_get_proxy_settings()

# 에어플로우 기반 라이브러리
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


from preprocess.naver_preprocess import preprocessing

# 기본 설정 
default_args = {
    'owner' : 'airim',
    "start_date" : datetime(2023,8,31), # DAG 의 시작날짜를 정의 / 8월 31일 부터
    "end_date"   : datetime(2023,9,5) # DAG 의 시작날짜를 정의 / 8월 31일 부터

}
NAVER_CLI_ID = "rkLwUXRItZqJcqxqN2ft"
NAVER_CLI_SECRET = "7hkp_5jlJd"

dag = DAG(
    dag_id="naver-search-pipeline",
    
    # schedule_interval 변수를 기반으로 스케줄 간격을 정의할 수 있다. 
    # 디폴트 값은 None -> 수동실행 
    schedule_interval="@daily",
    # schedule_interval = None,

    # DAG 의 기본 옵션 
    default_args = default_args,

    # 해쉬태그 생각하면 편함, 찾기 쉽게 검색할 수 있음
    tags=["naver", "search", "local", "api", "pipeline"],
    catchup=False
)

creating_tabel = SqliteOperator(
    
    task_id = "Task_ID_creating_tabel",# task id 
    sqlite_conn_id = "db_sqlite",        # 웹 UI 에서 연결할 sqlite id
    
    # 쿼리 : 테이블 만드는데, naver_search_result 가 없을 때 만들어라 
    sql = 
    '''
    CREATE TABLE IF NOT EXISTS naver_search_result(
        title TEXT,
        address TEXT, 
        category TEXT,
        description TEXT,
        link TEXT
    )
    ''',
    
    dag = dag # Dag 연결 
)
def response_def(response):
    print(response.json())
    json_respone = response.json()

    return 'items' in json_respone and len(json_respone['items']) > 0

is_api_avilable = HttpSensor(
    task_id = "Task_ID_is_api_avilable",
    http_conn_id="naver_search_api",
    # 요청 URL 
    # https://openapi.naver.com/v1/search/local.json 이므로, .com 이후의 값을 넣어줍니다. 
    endpoint='v1/search/local.json',

    # API를 요청할 때 다음 예와 같이 HTTP 요청 헤더에 클라이언트 아이디와 클라이언트 시크릿을 추가해야 합니다.
    headers={
        'X-Naver-Client-Id' : f'{NAVER_CLI_ID}',
        'X-Naver-Client-Secret' : f'{NAVER_CLI_SECRET}',
    },

    # API 파라미터 
    request_params={
        "query" : '탕후루', # 필수 파라미터 (String)
        'display' : 5     # 한 번에 표시할 검색 결과 개수 (Integer)
    },

    # http 요청의 응답을 검사하는 콜백 함수를 지정 
    # respone 값을 받아 responese.json() 을 실행
    # httpsensor 가 http 요청을 보내고 받은 응답을 자동으로 response_check 함수의 인자로 전달
    response_check = response_def,

    dag=dag
)

def response_filter_def(res):
    # res 는 HTTP 응답 여부를 보여줍니다. 
    print("res :\n", res),
    # res.text 는 결과에 값을 텍스트 타입으로 반환 
    print("res.text : \n",res.text),
    return json.loads(res.text)

crawl_naver = SimpleHttpOperator(
    task_id = 'Task_ID_crawl_naver', 
    http_conn_id='naver_search_api',
    endpoint='v1/search/local.json',
    headers={
        'X-Naver-Client-Id' : f'{NAVER_CLI_ID}',
        'X-Naver-Client-Secret' : f'{NAVER_CLI_SECRET}',
    },
    data={
        "query" : '탕후루',
        'display' : 5,
    }, 

    # 통신 방식 - GET 은 데이터를 가지고 온다. 
    method="GET",

    # HTTP 응답을 처리하기 위한 콜백함수 지정 
    # 응답 본문 res 를 json 으로 파싱해 변수화
    # response_filter = lambda res : json.loads(res.text),
    response_filter = response_filter_def,
    # http 응답의 내용을 airflow 로그에 기록 
    log_response = True,
    dag=dag
)

# 파이썬 오퍼레이터 
preprocess_result = PythonOperator(
    # 테스크 이름 
    task_id = 'Task_ID_preprocess_result',
    # 파이썬 함수 호출
    python_callable=preprocessing,
    dag = dag
)

store_result = BashOperator(
    task_id = 'Task_ID_store_naver',
    # echo : 유닉스/리눅스 시스템에서 문자열을 출력하는 명령어 
    # -e : 이스케이프 문자를 해석하도록 
    # \n 를 통해 sql 
    bash_command = 
    'echo -e ".separator ", "\n.import /Users/airim/airflow/dags/data/naver_processed_result.csv naver_search_result" | sqlite3 /Users/airim/airflow/airflow.db',
    dag = dag 
)

# 파이프라인 구성하기 
creating_tabel >> is_api_avilable >> crawl_naver >> preprocess_result >> store_result