import pandas as pd
import pymysql
from sqlalchemy import create_engine

import requests  # API를 이용해 자료를 받아오기 위해
import json
import time
import math

# MySQL 연결 설정
host = 'da-4-1-db.c1widrnxppoz.ap-northeast-2.rds.amazonaws.com'  # 여기에 MySQL 서버의 호스트 주소 또는 IP 주소를 입력하세요
port = 3306
user = 'root'   
password = '9juvtDV9gG&[!'
database = 'DataDB'
table_name='서울시_날씨_경복궁'

 # MySQL 연결
conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)

def API_Real_time_Data_Save():
    apikey = '6c73687349667870363055546a6748'  # 받은 key 값 입력
    startnum = 1
    endnum = 5
    Data_target='WEATHER_STTS'
    base_url = f'http://openapi.seoul.go.kr:8088/{apikey}/json/citydata'

    url=f'{base_url}/{startnum}/{endnum}/경복궁'
    json_data = requests.get(url).json()
    api_to_dataframe = pd.DataFrame(json_data['CITYDATA'][Data_target])
    api_to_dataframe.to_csv('Data_merge.csv',index=False)

    # JSON 파일로 저장
    with open(f"test.json", "w", encoding="utf-8") as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)

def Create_Table():
    df = pd.read_csv('data_merge.csv')
    ## csv read시 colum에 빈칸이 있으면 mysql에서 query 정상작동이 안됨;;

    # 연결이 제대로 되었는지 확인
    if conn:
        print("MySQL에 성공적으로 연결되었습니다.")
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
        df.to_sql(table_name, con=engine, if_exists='fail', index=False)
        # 연결 종료 
        # conn.close()
        print("Create Table Success.")
    else:
        print("MySQL 연결에 실패하였습니다.")

start_time = time.time()
API_Real_time_Data_Save()
end_time = time.time()
print(f"작업 소요 시간: {end_time - start_time} 초")

start_time = time.time()
Create_Table()
end_time = time.time()
print(f"작업 소요 시간: {end_time - start_time} 초")
