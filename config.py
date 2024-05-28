import os
from dotenv import load_dotenv

# .env file load
load_dotenv()

# 환경 변수 설정
#RABBITMQ 설정
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_CONSUME_QUEUE = os.environ.get('RABBITMQ_CONSUME_QUEUE')

#REDIS 설정
REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD')

#인증 및 MODEL_LOAD 설정
TENANT_ID = os.environ.get('TENANT_ID')
USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD') 
STORAGE_URL = os.environ.get('STORAGE_URL')
