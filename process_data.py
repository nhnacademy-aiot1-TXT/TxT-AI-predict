"""
비동기 방식으로 RabbitMQ 메시지를 처리하고, 데이터를 예측하여 결과를 발행하고 저장하는 스크립트입니다.

이 스크립트는 aio_pika를 사용하여 RabbitMQ 메시지를 비동기적으로 소비하고, 데이터를 처리하여 예측 결과를 생성하며, 결과를 다시 RabbitMQ에 발행하고 Redis에 저장합니다.

기능:
    setup_rabbitmq: RabbitMQ 연결을 설정하고 큐와 익스체인지를 선언하는 비동기 함수입니다.
    consume: 큐에서 메시지를 소비하고 데이터를 처리하여 결과를 발행하고 저장하는 비동기 함수입니다.
    process_data: 메시지에서 데이터를 추출하고 처리하여 예측 결과를 생성하는 비동기 함수입니다.
    publish_result: 처리된 결과를 RabbitMQ에 발행하는 비동기 함수입니다.
    save_data: 처리된 데이터를 Redis에 저장하는 비동기 함수입니다.
    main: 메인 비동기 함수로, RabbitMQ 연결을 설정하고 메시지를 처리합니다.
"""

import json
import redis
import pandas as pd
from datetime import datetime
from aio_pika import connect_robust, Message, ExchangeType
from config import RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_CONSUME_QUEUE, REDIS_PORT, REDIS_HOST, REDIS_PASSWORD

CONTAINER_NAME = 'TxT-model'

# Redis 클라이언트 생성
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=13)

# RabbitMQ 연결 URL 생성
rabbitmq_url = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}/"

async def setup_rabbitmq():
    """
    RabbitMQ 연결을 설정하고 큐와 익스체인지를 선언하는 비동기 함수입니다.

    Returns:
        tuple: RabbitMQ 연결, 큐, 익스체인지 객체를 포함하는 튜플
    """
    connection = await connect_robust(rabbitmq_url)
    channel = await connection.channel()
    queue = await channel.declare_queue(RABBITMQ_CONSUME_QUEUE, durable=True)
    exchange = await channel.declare_exchange('txt.device.control', ExchangeType.DIRECT, durable=True)
    return connection, queue, exchange

async def consume(queue, exchange, model_list):
    """
    큐에서 메시지를 소비하고 데이터를 처리하여 결과를 발행하고 저장하는 비동기 함수입니다.

    Args:
        queue: RabbitMQ 큐 객체
        exchange: RabbitMQ 익스체인지 객체
        model_list (list): 로드된 모델 객체 리스트
    """
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                decode_message = message.body.decode('utf-8')
                data = json.loads(decode_message)
                field_values, pub_json = await process_data(data, model_list)

                await publish_result(exchange, pub_json, data.get("deviceName"))
                await save_data(field_values)

async def process_data(data, model_list):
    """
    메시지에서 데이터를 추출하고 처리하여 예측 결과를 생성하는 비동기 함수입니다.

    Args:
        data (dict): RabbitMQ 메시지에서 추출한 데이터
        model_list (list): 로드된 모델 객체 리스트

    Returns:
        tuple: 처리된 데이터 필드, 발행할 JSON 형태의 결과를 포함하는 튜플
    """
    place_message = data.get("place")
    device_name_message = data.get("deviceName")
    date_time_message = data.get("time")
    indoor_temperature_message = float(data.get("class_a_temperature"))
    indoor_humidity_message = float(data.get("class_a_humidity"))
    temperature_message = float(data.get("outdoor_temperature"))
    humidity_message = float(data.get("outdoor_humidity"))
    total_people_count_message = float(data.get("class_a_total_people_count"))

    # unix_time > local_time_minute으로 변경(00시 기준)
    time_object = datetime.fromtimestamp(int(date_time_message) / 1000)
    time = time_object.hour * 60 + time_object.minute
    float(time)

    hour = time_object.hour
    minute = time_object.minute
    redis_time = f"{hour}시 {minute}분"

    sensor_data = pd.DataFrame({
        'outdoor_temperature' : [temperature_message],
        'outdoor_humidity': [humidity_message],
        'temperature' : [indoor_temperature_message],
        'humidity' : [indoor_humidity_message],
        'people_count': [total_people_count_message],
        'time_in_minutes' : [time]
    })

    if not model_list:
        return None, print({"deviceName": device_name_message, "result": "모델이 로드되지 않았습니다."})

    # 디바이스가 추가에 따른 해당 모델로의 분기문 (ex. airConditioner는 aircon model(list[0]), heater는 heater model(list[1]) 등...)
    if "airconditioner" in device_name_message:
        model = model_list[0]
    # elif "heater" in device_name:
    #     model = model_list[1]

    predictions = model.predict(sensor_data)

    # 0,1 결과를 boolen으로 publish
    if predictions[0] == 0:
        pub_result = False 
    else:
        pub_result = True 

    # 0,1 결과를 ON/OFF로 변환
    if predictions[0] == 0:
        redis_result = "OFF"
    else:
        redis_result = "ON"

    # redis에 필드/ rabbitMQ의 값 및 결과 저장
    field_values = {
        'place': place_message,
        'deviceName': device_name_message,
        'time': redis_time,
        'indoorTemperature' : f"{indoor_temperature_message:0.1f}",
        'indoorHumidity' : f"{indoor_humidity_message:0.1f}",
        'outdoorTemperature' : f"{temperature_message:0.1f}",
        'outdoorHumidity' : f"{humidity_message:0.1f}",
        'totalPeopleCount' : int(total_people_count_message),
        'result' : redis_result
    }

    pub_json = json.dumps({"place": place_message, "deviceName": device_name_message, "value": pub_result})
    return field_values, pub_json

async def publish_result(exchange, pub_json, device_name_message):
    """
    처리된 결과를 RabbitMQ에 발행하는 비동기 함수입니다.

    Args:
        exchange: RabbitMQ 익스체인지 객체
        pub_json (str): 발행할 JSON 형태의 결과
        device_name_message (str): 디바이스 이름 메시지
    """
    if "airconditioner" in device_name_message:
        routing_key = 'txt.airconditioner'
    # elif "heater" in device_name:
    #     routing_key = 'txt.heater'

    message = Message(
        body = pub_json.encode(),
        expiration=5000
    )
    await exchange.publish(
        message,
        routing_key=routing_key
    )

async def save_data(field_values):
    """
    처리된 데이터를 Redis에 저장하는 비동기 함수입니다.

    Args:
        field_values (dict): 처리된 데이터 필드
    """
    r.hset('predict_data', mapping=field_values)

async def main(model_list):
    """
    메인 비동기 함수로, RabbitMQ 연결을 설정하고 메시지를 처리합니다.

    Args:
        model_list (list): 로드된 모델 객체 리스트
    """
    connection, queue, exchange = await setup_rabbitmq()

    try:
        await consume(queue, exchange, model_list)
    finally:
        await connection.close()
