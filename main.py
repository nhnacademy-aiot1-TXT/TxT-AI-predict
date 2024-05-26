"""
비동기적으로 모델을 로드하고 RabbitMQ에서 데이터를 처리하는 메인 스크립트입니다.

이 스크립트는 asyncio를 사용하여 비동기적으로 모델을 로드하고, RabbitMQ에서 데이터 처리를 담당하는 메인 함수입니다.

기능:
    main: 비동기 함수로, 초기 모델을 로드하고 RabbitMQ에서 데이터 처리를 시작합니다.
"""
import asyncio
from model import load_model
from model import main as model_main
from process_data import main as rabbitmq_main
import nest_asyncio

# 중첩된 asyncio 이벤트 루프 실행을 허용합니다.
nest_asyncio.apply()

async def main():
    """
    비동기 함수로, 초기 모델을 로드하고 RabbitMQ에서 데이터 처리를 시작합니다.
    """
    # 모델을 비동기적으로 로드합니다.
    model_list = await load_model()
    if model_list:
        print("모든 초기 모델 로드")
    else:
        print("초기 모델 로드 실패")

    # RabbitMQ에서 데이터 처리를 시작합니다.
    task_pika = asyncio.create_task(rabbitmq_main(model_list))
    task_load_model = asyncio.create_task(model_main())

    # 두 작업이 완료될 때까지 대기합니다.
    await asyncio.gather(task_load_model, task_pika)

if __name__ == '__main__':
    # 메인 비동기 함수를 실행합니다.
    asyncio.run(main())
