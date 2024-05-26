import io
import joblib
import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from config import TENANT_ID, USERNAME, PASSWORD, STORAGE_URL, AUTH_URL
import nest_asyncio

#object_list : 오브젝트 이름 리스트
#model_list : 오브젝트 객체 리스트

nest_asyncio.apply()

CONTAINER_NAME = 'TxT-model' #dash 때문에 os에 안들어감

async def get_token():
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
            response.raise_for_status()
            return await response.json()

async def get_object_list(token_id, container_name):
    req_url = f"{STORAGE_URL}/{container_name}"
    headers = {'X-Auth-Token': token_id}

    async with aiohttp.ClientSession() as session:
        async with session.get(req_url, headers=headers) as response:
            response.raise_for_status()
            object_list = (await response.text()).split('\n')
            object_list = [obj for obj in object_list if obj]  # 빈 문자열 제거
            return object_list

async def load_model():
    token = await get_token()
    TOKEN_ID = token['access']['token']['id']
    container_name = CONTAINER_NAME
    object_list = await get_object_list(TOKEN_ID, container_name)
    model_list = []

    for object_name in object_list:
        req_url = '/'.join([STORAGE_URL, CONTAINER_NAME, object_name])
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(req_url, headers = {'X-Auth-Token': TOKEN_ID}) as response:
                    response.raise_for_status()
                    model_bytes = io.BytesIO(await response.read())
                    model = joblib.load(model_bytes)
                    model_list.append(model)
            except aiohttp.ClientError as e:
                print(f"클라이언트 연결 오류: {str(e)}")
            except Exception as e:
                print(f"모델 로드 중 오류 발생: {str(e)}")

    return model_list

async def load_model_periodically():
    scheduler = AsyncIOScheduler()

    async def loaded_models():
        model_list = await load_model()
        if model_list:
            print("모든 모델 로드 완료")
        else:
            print("모델 로드 실패")

    scheduler.add_job(loaded_models, 'cron', hour='0', minute='30')
    scheduler.start()

async def main():    
    await load_model_periodically()
