import os
import io
import json
import pika
import logging
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

#콘솔 로그 생성 및 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# RabbitMQ 연결
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST, credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue='test', durable=True)  #txt.predict.queue
print("RabbitMQ에 연결됨")

# 공유 변수
model = None

class Auth:
    """인증 관련 클래스"""
    @staticmethod
    async def get_token():
        """인증 서버에서 토큰을 가져오는 비동기 메서드

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
        """객체 스토리지에서 모델을 로드하는 비동기 메서드

        Returns:
            object: 로드된 모델 객체
        """
        print("새 모델 로드...")
        token = await Auth.get_token()
        TOKEN_ID = token['access']['token']['id']

        req_url = '/'.join([STORAGE_URL, CONTAINER_NAME, OBJECT_NAME])
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(req_url, headers = {'X-Auth-Token': TOKEN_ID}) as response:
                    response.raise_for_status()
                    model_bytes = io.BytesIO(await response.read())
                    return joblib.load(model_bytes)
            except aiohttp.ClientError as e:
                logger.error(f"클라이언트 연결 오류: {str(e)}")
                return None
            except Exception as e:
                logger.error(f"모델 로드 중 오류 발생: {str(e)}")
                return None

class Message:
    """메시지 처리 관련 클래스"""
    def callback(ch, method, properties, body):
        """RabbitMQ에서 메시지를 수신했을 때 호출되는 콜백 함수

        Args:
            ch (pika.channel.Channel): RabbitMQ 채널 객체
            method (pika.spec.Basic.Deliver): 배달 메서드
            body (bytes): 메시지 내용
        """
        async def process_message():
            global model
            message = body.decode('utf-8')
            data = json.loads(message)
            
            try:
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

                if model is None:
                    logging.info("모델이 로드되지 않았습니다. 예측 불가능")

                predictions = model.predict(data)

                # 0,1 결과를 에어컨 ON/OFF로 변환
                if predictions[0] == 0:
                    result = "OFF"
                else:
                    result = "ON"

                print("예측 결과:", result)

                channel.basic_publish(exchange='predict_result', routing_key='predict', body=result)

            except Exception as e:
                logger.error(f"오류 메시지 발생: {str(e)}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            ch.basic_ack(delivery_tag=method.delivery_tag)
        asyncio.run(process_message())

    channel.basic_consume(queue='test', on_message_callback=callback, auto_ack=False)

async def load_model_periodically():
    """매일 00:30에 새 모델을 로드하는 비동기 함수"""
    global model

    while True:
        now = datetime.now()
        today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        load_time = today_midnight + timedelta(hours=0, minutes=30)

        if now >= load_time:
            load_time += timedelta(days=1)

        # 목표 시간까지 대기
        time_to_sleep = (load_time - now).total_seconds()
        await asyncio.sleep(time_to_sleep)

        try:
            model = await ObjectService.load_model()
            logging.info(f"{datetime.now()} 새 모델 로드 완료")
        except Exception as e:
            logging.error(f"모델 로드 중 오류 발생: {str(e)}")

async def main():
    """프로그램 실행시 초기 모델 로드 및 메시지 처리를 위한 메인 비동기 함수"""
    try:
        global model
        model = await ObjectService.load_model()
        logging.info(f"{datetime.now()} 실행시 초기 모델 로드")
    except Exception as e:
        logging.error(f"초기 모델 로드 중 오류 발생: {str(e)}")
    task_load_model = asyncio.create_task(load_model_periodically())

    task_callback = asyncio.create_task(asyncio.to_thread(channel.start_consuming))

    # 두 태스크 모두 완료될 때까지 대기
    await asyncio.gather(task_load_model, task_callback)

if __name__ == '__main__':
    asyncio.run(main())
