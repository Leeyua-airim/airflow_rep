import json
import pathlib
from datetime import datetime
import airflow
from datetime import timedelta

# m1 에서만 발생하는 requests.get() 
from _scproxy import _get_proxy_settings
_get_proxy_settings()

import requests 
import requests.exceptions as requests_exceptions

import airflow.utils.dates

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# DAG 클래스객체에 대한 인스턴스 생성 - 워크플로의 시작점 
dag = DAG(
    dag_id="download_recket_launches", # Home 화면상에서 DAG 이름
    start_date = airflow.utils.dates.days_ago(14),# DAG 처음 실행 시작 날짜 
    #schedule_interval = None, # DAG 실행간격 - 데일리로 할 수도 있고 주차별로 할 수도 있다. 
    schedule_interval = "@daily", # DAG 실행간격 - 데일리
)

# 데이터를 받는 배쉬오퍼레이터 
download_launches = BashOperator(
    task_id      = 'download_launches', # 오퍼레이터 출력 화면상에서의 태스크 이름 
    bash_command = "curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'", # 배쉬커멘트를 활용해서 데이터 끌고오기
    dag=dag # 대그 연결
)

# 이미지를 끌고오는 함수
def _get_pictures():

    # [데이터를 저장하기 위한 코드]
    # pathlib.Path("/tmp/images") : /tmp/images 경로를 나타내는 객체 생성
    # mkdir() : 해당 경로에 디렉터리를 생성
    # parents = True : 필요시 상위 경로도 함께 생성
    # exist_ok = True : 만약 상위 폴더가 존재해도  오류를 일으키지 말라는 명령어
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    
    # 파일을 열고 f 라는 이름으로 활용
    with open("/tmp/launches.json") as f:
        
        launches = json.load(f) # 데이터를 로드하여 launches 변수에 할당합니다.
        image_urls = [launch["image"] for launch in launches["results"]] # 해당 데이터에 image url은 따로 변수로 할당합니다.

        for image_url in image_urls :
            try : 
                response = requests.get(image_url) # get() 함수를 통해 image_url 변수화
                print(f"[알림] response : {response}")
                
                image_filename = image_url.split("/")[-1]
                
                # 파일 이름을 우리가 생성한 폴더 경로까지 포함해서 파일명을 바꿔줍니다.
                target_file = f"/tmp/images/{image_filename}"
                
                with open(target_file, "wb") as f:
                    f.write(response.content)
                
                print(f"[안내] Downloaded {image_url} to {target_file}")

            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")   

            # 첫 번째 예외처리] URL 이 유효한 형식이 아닌경우 
            except requests_exceptions.MissingSchema: 
                print(f"[에러 경고] {image_url} appears to be an invalid URL.")
            

            

# 파이썬 오퍼레이터 생성
get_pictures = PythonOperator(
    task_id = "get_pictures",     # 테스크 ID 생성
    python_callable=_get_pictures,# 함수 할당
    dag = dag # DAG 연결
)

# 배쉬오퍼레이터 생성 
# ls 를 통해 목록을 뽑은 후 
# /tmp/images/ 경로가 대상 
# wc 는 word count 의 약자로, 줄을 셉니다. 
# -l 은 라인을 의미합니다. 
notify = BashOperator(
    task_id = "notify",
    # 이미지 총 몇장인지 카운트
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

# 태스크 실행 순서 정의
download_launches >> get_pictures >> notify