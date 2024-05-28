"""
비동기 방식으로 모델을 로드하고 주기적으로 모델을 로드하는 스크립트입니다.

이 스크립트는 aiohttp를 사용하여 비동기적으로 모델을 로드하고, apscheduler를 사용하여 일정한 주기로 모델을 로드합니다.

기능:
    get_token: 인증 토큰을 가져오는 비동기 함수입니다.
    get_object_list: 주어진 컨테이너에서 오브젝트 목록(str)을 가져오는 비동기 함수입니다.
    load_model: 모델(객체)을 비동기적으로 로드하는 함수입니다.
    load_model_periodically: 모델을 주기적으로 로드하는 비동기 함수입니다.
    main: 메인 비동기 함수로, 모델을 주기적으로 로드합니다.
"""

import os
import io
import joblib
import logging
import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from config import TENANT_ID, USERNAME, PASSWORD, STORAGE_URL, AUTH_URL
import nest_asyncio

nest_asyncio.apply()

CONTAINER_NAME = 'TxT-model'

#콘솔 로그 생성 및 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 환경 변수 로딩
TENANT_ID = os.environ.get('TENANT_ID')
USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD') 
STORAGE_URL = os.environ.get('STORAGE_URL')
AUTH_URL = 'https://api-identity-infrastructure.nhncloudservice.com/v2.0'

# 환경 변수 출력
logging.basicConfig(level=logging.INFO)
logging.info(f'TENANT_ID: {TENANT_ID}')
logging.info(f'USERNAME: {USERNAME}')
logging.info(f'PASSWORD: {PASSWORD}')
logging.info(f'STORAGE_URL: {STORAGE_URL}')

async def get_token():
    """
    인증 토큰을 가져오는 비동기 함수입니다.

    Returns:
        dict: 인증 토큰을 담은 딕셔너리
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
            response.raise_for_status()
            return await response.json()    

async def get_object_list(token_id, container_name):
    """
    주어진 컨테이너에서 오브젝트 목록을 가져오는 비동기 함수입니다.

    Args:
        token_id (str): 인증 토큰
        container_name (str): 컨테이너 이름

    Returns:
        object_list (list): 오브젝트 목록
    """
    req_url = f"{STORAGE_URL}/{container_name}"
    headers = {'X-Auth-Token': token_id}

    async with aiohttp.ClientSession() as session:
        async with session.get(req_url, headers=headers) as response:
            response.raise_for_status()
            object_list = (await response.text()).split('\n')
            object_list = [obj for obj in object_list if obj]  # 빈 문자열 제거
            return object_list

async def load_model():
    """
    모델을 비동기적으로 로드하는 함수입니다.

    Returns:
        model_list (list): 로드된 모델 객체의 리스트
    """
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
    """
    모델을 주기적으로 로드하는 비동기 함수입니다.
    """
    scheduler = AsyncIOScheduler()

    async def loaded_models():
        """
        모델을 로드하는 내부 비동기 함수입니다.
        """
        model_list = await load_model()
        if model_list:
            print("모든 모델 로드 완료")
        else:
            print("모델 로드 실패")

    scheduler.add_job(loaded_models, 'cron', hour='0', minute='30')
    scheduler.start()

async def main():
    """
    메인 비동기 함수로, 모델을 주기적으로 로드합니다.
    """
    await load_model_periodically()
