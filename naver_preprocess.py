from pandas import json_normalize

# import * 과 같이 한꺼번에 가지고 오는 경우 식별을 위해 
# 만든 특별한 변수. 즉 preprocessing 만 가지고 옴
__all__ = ['preprocessing']


# __all__ 에서 정의한 함수 정의 
def preprocessing(ti):
    # xcom_pull() 은 작은 양의 데이터를 전달 하는데 사용 
    # task_ids 에 기재된 테스크에서 확보한 데이터를 땡겨 search_result 에 저장
    # xcom_pull() 은 리스트를 반환  
    search_result = ti.xcom_pull(task_ids = ['Task_ID_crawl_naver'])[0]
    print(f"search_result : {search_result}")
    print(f"type(search_result) : {type(search_result)}")

    # 만약 search_result 값이 없는 경우
    # not 0 : True / not 1 : False   
    if not len(search_result):
        # 아래와 같은 에러를 일으키도록 
        raise ValueError("검색 결과 없습니다.")

    # 가장 많은 정보를 담고 있는 iteams 를 search_result 에서 따로 뺀 후 변수화 
    items = search_result['items']

    # json을 판다스로 정규화 한다. 
    # 리스트 컴프리헨션
    preprocessing_items = json_normalize([
        {
            'title' : item['title'],
            'address' : item['address'],
            'category' : item['category'],
            'description' : item['description'],
            'link' : item['link']
            } for item in items
    ])

    # to_csv() 는 pandas DataFrame 의 메서드이며, 데이터프레임을 csv 파일로 저장해줌
    # 상위에서 정의한 preprocessing_items 변수가 대상 
    preprocessing_items.to_csv('/Users/airim/airflow/dags/data/naver_processed_result.csv', index=None, header=False, mode='w')