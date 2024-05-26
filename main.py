import asyncio
from model import load_model  # load_model_periodically
from model import main as model_main
from process_data import main as rabbitmq_main
import nest_asyncio

nest_asyncio.apply()

async def main():
    model_list = await load_model()
    if model_list:
        print("모든 초기 모델 로드")
    else:
        print("초기 모델 로드 실패")

    task_pika = asyncio.create_task(rabbitmq_main(model_list))
    task_load_model = asyncio.create_task(model_main())

    # # 두 태스크 모두 완료될 때까지 대기
    await asyncio.gather(task_load_model, task_pika)

if __name__ == '__main__':
    asyncio.run(main())
