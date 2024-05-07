import os
import io
import json
import pika
import joblib
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import nest_asyncio
nest_asyncio.apply()

# 환경변수 설정
TENANT_ID = os.environ.get('TENANT_ID')
USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD')
STORAGE_URL = os.environ.get('STORAGE_URL')
CONTAINER_NAME = 'TxT-model'
OBJECT_NAME = 'air_conditional_ai_model.joblib'
AUTH_URL = 'https://api-identity-infrastructure.nhncloudservice.com/v2.0'
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')

# RabbitMQ 연결
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST, credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue='txt.predict.queue', durable=True)
print("RabbitMQ에 연결됨")

# 공유 변수 (모델 객체)
model = None

class Auth:
    """인증 관련 클래스"""
    @staticmethod
    async def get_token():
        """토큰을 가져오는 비동기 메서드

        Returns:
            dict: 토큰 정보가 포함된 딕셔너리
        """
        token_url = AUTH_URL + '/tokens'
        req_body = {
            'auth': {
                'tenantId': TENANT_ID,
                'passwordCredentials': {
                    'username': USERNAME,
                    'password': PASSWORD
                }
            }
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(token_url, json=req_body) as response:
                return await response.json()

class ObjectService:
    """객체 스토리지 관련 클래스"""
    @staticmethod
    async def load_model():
        """모델을 로드하는 비동기 메서드

        Returns:
            object: 로드된 모델 객체
        """
        print("새 모델 로드...")
        token = await Auth.get_token()
        TOKEN_ID = token['access']['token']['id']

        req_url = '/'.join([STORAGE_URL, CONTAINER_NAME, OBJECT_NAME])
        
        async with aiohttp.ClientSession() as session:
            async with session.get(req_url, headers = {'X-Auth-Token': TOKEN_ID}) as response:
                response.raise_for_status()
                model_bytes = io.BytesIO(await response.read())
                return joblib.load(model_bytes)

class Message:
    """메시지 처리 관련 클래스"""
    def callback(ch, method, properties, body):
        """메시지 콜백 함수

        Args:
            ch (pika.channel.Channel): RabbitMQ 채널 객체
            method (pika.spec.Basic.Deliver): 배달 메서드
            properties (pika.spec.BasicProperties): 메시지 속성
            body (bytes): 메시지 내용
        """
        async def process_message():
            global model  # 전역 변수 model 사용
            message = body.decode('utf-8')
            data = json.loads(message)
            
            date_time = data.get("time")
            indoor_temperature = data.get("indoorTemperature", {}).get("value")
            indoor_humidity = data.get("indoorHumidity", {}).get("value")
            temperature_message = data.get("outdoorTemperature", {}).get("value")
            humidity_message = data.get("outdoorHumidity", {}).get("value")
            total_people_count_message = data.get("totalPeopleCount", {}).get("value")

            # unix_time > local_time_minute으로 변경(00시 기준)
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

            # 모델이 로드되어 있지 않으면 예측 불가
            if model is None:
                print("모델이 로드되지 않았습니다. 예측 불가능")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # 모델 사용하여 예측
            predictions = model.predict(data)

            # 0,1 결과를 언어로 변환
            if predictions[0] == 0:
                result = "OFF" # 에어컨 끄기
            else:
                result = "ON" # 에어컨 켜기

            print("예측 결과:", result)

            # 예측 결과 발행
            channel.basic_publish(exchange='predict_result', routing_key='predict', body=result)

            # 메시지 처리 완료 후 수동으로 확인(ack)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        # 비동기 작업 이벤트 루프에서 실행
        asyncio.run(process_message())

    # 콜백 함수를 채널에 등록
    channel.basic_consume(queue='txt.predict.queue', on_message_callback=callback, auto_ack=False)

async def load_model_periodically():
    """주기적으로 모델을 로드하는 비동기 함수"""
    global model

    while True:
        # 매일 오전 12:30에 모델 로드
        now = datetime.now()
        load_time = now.replace(hour=0, minute=30)

        if now >= load_time:
            load_time += timedelta(days=1)

        # 목표 시간까지 대기
        time_to_sleep = (load_time - datetime.now()).total_seconds()
        await asyncio.sleep(time_to_sleep)

        model = await ObjectService.load_model()
        print(f"{datetime.now()} 새 모델 로드 완료")

async def main():
    """메인 비동기 함수"""
    task_load_model = asyncio.create_task(load_model_periodically())

    task_callback = asyncio.create_task(asyncio.to_thread(channel.start_consuming))

    # 두 태스크 모두 완료될 때까지 대기
    await asyncio.gather(task_load_model, task_callback)

if __name__ == '__main__':
    asyncio.run(main())
