import os

# 환경 변수 설정
#RABBITMQ 설정
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_CONSUME_QUEUE = os.getenv('RABBITMQ_CONSUME_QUEUE')

#REDIS 설정
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

#인증 및 MODEL_LOAD 설정
TENANT_ID = os.getenv('TENANT_ID')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD') 
STORAGE_URL = os.getenv('STORAGE_URL')
AUTH_URL = 'https://api-identity-infrastructure.nhncloudservice.com/v2.0'
