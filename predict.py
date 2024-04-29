#!/usr/bin/env python
# coding: utf-8

# In[ ]:
import os
import json
import requests

def get_token(auth_url):
    tenant_id = os.environ.get('TENANT_ID')
    username = os.environ.get('USERNAME')
    password = os.environ.get('PASSWORD')

    token_url = auth_url + '/tokens'
    req_header = {'Content-Type': 'application/json'}
    req_body = {
        'auth': {
            'tenantId': tenant_id,
            'passwordCredentials': {
                'username': username,
                'password': password
            }
        }
    }

    response = requests.post(token_url, headers=req_header, json=req_body)
    return response.json()


if __name__ == '__main__':
    AUTH_URL = 'https://api-identity-infrastructure.nhncloudservice.com/v2.0'

    token = get_token(AUTH_URL)
    print(json.dumps(token, indent=4))


# In[ ]:

import os
import time
import requests
import schedule

# object download
class ObjectService:
    def __init__(self, storage_url, token_id):
        self.storage_url = storage_url
        self.token_id = token_id

    def _get_url(self, container, object):
        return '/'.join([self.storage_url, container, object])

    def _get_request_header(self):
        return {'X-Auth-Token': self.token_id}
    
    def download(self, container, object, download_path):
        req_url = self._get_url(container, object)
        req_header = self._get_request_header()

        response = requests.get(req_url, headers=req_header)

        dn_path = '/'.join([download_path, object])
        with open(dn_path, 'wb') as f:
            f.write(response.content)

def job():
    print("작업 실행 중..")
    if __name__ == '__main__':
        STORAGE_URL=os.environ.get('STORAGE_URL')
        TOKEN_ID=os.environ.get('TOKEN_ID')
        CONTAINER_NAME = 'TxT-model'
        OBJECT_NAME = 'air_conditional_ai_model.joblib'
        DOWNLOAD_PATH = '/home/ljh'
        obj_service = ObjectService(STORAGE_URL, TOKEN_ID)
        obj_service.download(CONTAINER_NAME, OBJECT_NAME, DOWNLOAD_PATH)

#매 분마다 job 함수 실행
schedule.every(1).minute.do(job)

#스케줄링된 작업이 실행될 수 있도록 주기적으로 확인
while True:
    schedule.run_pending()
    time.sleep(60)


# In[ ]:


import json
import pika
import joblib
import pandas as pd
from datetime import datetime

# RabbitMQ 연결 정보 환경 변수로 설정
rabbitmq_user = os.environ.get('RABBITMQ_USER')
rabbitmq_password = os.environ.get('RABBITMQ_PASSWORD')
rabbitmq_host = os.environ.get('RABBITMQ_HOST')

#모델 저장 경로
model_file_path = os.environ.get('MODEL_FILE_PATH')

#모델 불러오기
with open(model_file_path, 'rb') as f:
  model = joblib.load(f)
print("모델을 성공적으로 불러왔습니다.")

credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host, credentials=credentials))
channel = connection.channel()

channel.queue_declare(queue='txt.predict.queue', durable=True)
print("RabbitMQ에 연결됨")

#큐에서 메시지 소비할 때 호출되는 콜백 함수
def callback(ch, method, properties, body):
    message = body.decode('utf-8')
    data = json.loads(message)
    
    date_time = data.get("time")
    indoor_temperature = data.get("indoorTemperature", {}).get("value")
    indoor_humidity = data.get("indoorHumidity", {}).get("value")
    temperature_message = data.get("outdoorTemperature", {}).get("value")
    humidity_message = data.get("outdoorHumidity", {}).get("value")
    total_people_count_message = data.get("totalPeopleCount", {}).get("value")

    #unix_time > local_time_minute으로 변경
    time_object = datetime.fromtimestamp(date_time / 1000)
    time = time_object.hour*60 + time_object.minute

    data = pd.DataFrame({
        'outdoor_temperature' : [temperature_message],
        'outdoor_humidity': [humidity_message],
        'temperature' : [indoor_temperature],
        'humidity' : [indoor_humidity],
        'people_count': [total_people_count_message],
        'time_in_minutes' : [time]
    })

    data.dropna(axis=0)
    predictions = model.predict(data)

      # 0,1 결과를 언어로 변환
    if predictions[0] == 0:
        result = "OFF" #에어컨 끄기
    else:
        result = "ON" #에어컨 켜기

    print("예측 결과:", result)

    # 반환된 결과 발행
    channel.basic_publish(exchange='predict_result', routing_key='predict', body=result)

# 콜백 함수를 채널에 등록
channel.basic_consume(queue='txt.predict.queue', on_message_callback=callback, auto_ack=True)

# 메시지 소비 시작
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()


# %%
