#!/usr/bin/env python
# coding: utf-8

import os
import io
import json
import pika
import time
import joblib
import requests
import schedule
import pandas as pd
from datetime import datetime

def get_token(auth_url):
    """
    인증 URL에서 토큰 가져오는 함수

    Returns:
    - dict: 토큰을 포함한 JSON 응답
    """
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

class ObjectService:
    def __init__(self, storage_url, token_id):
        """
        Args:
        - storage_url (str): 스토리지 서비스 URL
        - token_id (str): 인증 토큰 ID
        """
        self.storage_url = storage_url
        self.token_id = token_id

    def _get_url(self, container, object):
        """
        스토리지의 객체에 접근하기 위한 URL을 생성하는 함수

        Returns:
        - str: 객체에 접근하기 위한 완전한 URL입니다.
        """
        return '/'.join([self.storage_url, container, object])

    def _get_request_header(self):
        """
        인증 토큰을 포함한 요청 헤더를 가져오는 함수
        
        Returns:
        - dict: 인증 토큰이 포함된 요청 헤더
        """
        return {'X-Auth-Token': self.token_id}
    
    def load_model(self, container, object):
        """
        스토리지에 지정된 컨테이너와 객체에 저장된 모델 로드 함수
        
        Returns:
        - object: 로드된 모델
        """
        req_url = self._get_url(container, object)
        req_header = self._get_request_header()
        response = requests.get(req_url, headers=req_header)
        model_bytes = io.BytesIO(response.content)
        return joblib.load(model_bytes)

def load_model_job():
    """
    모델 로드 함수, 매일 자정에 실행
    """
    print("새 모델 로드...")
    AUTH_URL = 'https://api-identity-infrastructure.nhncloudservice.com/v2.0'
    token = get_token(AUTH_URL)
    TOKEN_ID = token['access']['token']['id']

    STORAGE_URL = os.environ.get('STORAGE_URL')
    CONTAINER_NAME = 'TxT-model'
    OBJECT_NAME = 'air_conditional_ai_model.joblib'
    obj_service = ObjectService(STORAGE_URL, TOKEN_ID)
    model = obj_service.load_model(CONTAINER_NAME, OBJECT_NAME)
    return model

# RabbitMQ 연결 정보 환경 변수로 설정
rabbitmq_user = os.environ.get('RABBITMQ_USER')
rabbitmq_password = os.environ.get('RABBITMQ_PASSWORD')
rabbitmq_host = os.environ.get('RABBITMQ_HOST')

credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host, credentials=credentials))
channel = connection.channel()

channel.queue_declare(queue='txt.predict.queue', durable=True)
print("RabbitMQ에 연결됨")

#큐에서 메시지 소비할 때 호출되는 콜백 함수
def callback(ch, method, properties, body):
    """
    큐로부터 받은 메시지 처리, 로드된 모델로 예측
    """
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
    time = time_object.hour * 60 + time_object.minute

    data = pd.DataFrame({
        'outdoor_temperature' : [temperature_message],
        'outdoor_humidity': [humidity_message],
        'temperature' : [indoor_temperature],
        'humidity' : [indoor_humidity],
        'people_count': [total_people_count_message],
        'time_in_minutes' : [time]
    })

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

#프로그램 시작 모델 로드
model = load_model_job()

# 모델 로드 작업 스케줄링 (매일 자정에 실행)
schedule.every().day.at("00:00").do(load_model_job)

while True:
    # 스케줄링된 작업 실행
    schedule.run_pending()
    time.sleep(60)
