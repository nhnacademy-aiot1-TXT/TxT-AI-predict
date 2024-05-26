import json
import redis
import pandas as pd
from datetime import datetime
from aio_pika import connect_robust, Message, ExchangeType
from config import RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_CONSUME_QUEUE, REDIS_PORT, REDIS_HOST, REDIS_PASSWORD

CONTAINER_NAME = 'TxT-model' #dash 때문에 안들어가짐

# Redis 클라이언트 생성
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=13)

# Redis 연결 test
async def redis_test():
    try:
        response = r.ping()
        if response:
            print("OK")
    except redis.exceptions.ConnectionError:
        print("Fail")
    except redis.exceptions.AuthenticationError:
        print("Redis 인증에 실패했습니다.")

# RabbitMQ 연결 URL 생성
rabbitmq_url = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}/"

# RabbitMQ 연결 및 채널 설정, 큐 및 익스체인지 선언, 바인딩을 처리하는 함수
async def setup_rabbitmq():
    connection = await connect_robust(rabbitmq_url)
    channel = await connection.channel()
    queue = await channel.declare_queue(RABBITMQ_CONSUME_QUEUE, durable=True)
    exchange = await channel.declare_exchange('txt.device.control', ExchangeType.DIRECT, durable=True)
    return connection, queue, exchange

# 큐의 메시지를 소비하고 데이터를 처리한 후, 결과를 발행 및 저장하는 함수
async def consume(queue, exchange, model_list):
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                decode_message = message.body.decode('utf-8')
                data = json.loads(decode_message)
                field_values, pub_json = await process_data(data, model_list)

                await publish_result(exchange, pub_json, data.get("deviceName"))
                await save_data(field_values)

# 데이터를 처리하고 예측 결과를 생성하는 함수
async def process_data(data, model_list):
    # 메시지 처리 로직
    place_message = data.get("place")
    device_name_message = data.get("deviceName")
    date_time_message = data.get("time")
    indoor_temperature_message = data.get("class_a_temperature")
    indoor_humidity_message = data.get("class_a_humidity")
    temperature_message = data.get("outdoor_temperature")
    humidity_message = data.get("outdoor_humidity")
    total_people_count_message = data.get("class_a_total_people_count")

    # unix_time > local_time_minute으로 변경(00시 기준)
    time_object = datetime.fromtimestamp(date_time_message / 1000)
    time = time_object.hour * 60 + time_object.minute
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

    # airCondition은 aircon model(list[0]), heater는 heater model(list[1]) 등...
    # 디바이스가 추가에 따른 해당 모델로의 분기문
    if "airCondition" in device_name_message:
        model = model_list[0]
    # elif "heater" in device_name:
    #     model = model_list[1]

    predictions = model.predict(sensor_data)

    # 0,1 결과를 boolen으로 publish
    if predictions[0] == 0:
        pub_result = False  #"OFF"
    else:
        pub_result = True  #"ON"

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
        'indoorTemperature' : indoor_temperature_message,
        'indoorHumidity' : indoor_humidity_message,
        'outdoorTemperature' : temperature_message,
        'outdoorHumidity' : humidity_message,
        'totalPeopleCount' : total_people_count_message,
        'result' : redis_result
    }

    pub_json = json.dumps({"place": place_message, "deviceName": device_name_message, "value": pub_result})
    return field_values, pub_json

# 메시지 발행
async def publish_result(exchange, pub_json, device_name_message):
    if "airCondition" in device_name_message:
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

# 처리된 데이터를 redis에 저장
async def save_data(field_values):
    r.hset('predict_data', mapping=field_values)

async def main(model_list):
    connection, queue, exchange = await setup_rabbitmq()

    try:
        await consume(queue, exchange, model_list)
    finally:
        await connection.close()
