'''
웹 로그 발생 및 카프카 프로듀서 전송
'''
# 1. 모듈 가져오기
from kafka import KafkaProducer
import json
import time
import random
import logging
from datetime import datetime
from faker import Faker
import pendulum

# 2. 카프카 연결
producer = KafkaProducer(
    # docker 상에 존재하는 kafka 주소, localhost:9092
    bootstrap_servers = ['127.0.0.1:9092'], 
    # DICT->문자열->인코딩, 객체직렬화
    value_serializer  = lambda x: json.dumps(x).encode('utf-8')      
)

# 3. 전송
def send_msg():
    '''
        가상의 웹 접속(요청) 로그 생성
    '''
    fake = Faker()
    while True:
        # 3-1. 가상 웹 접속 로그
        web_log = {
            "ip"            : fake.ipv4(),
            "timestamp"     : pendulum.now("Asia/Seoul").format("YYYY-MM-DDTHH:mm:ss"),#fake.iso8601(),
            "method"        : fake.http_method(),
            "url"           : fake.uri(),
            "status_code"   : fake.random_int(min=200, max=503),
            "user_agent"    : fake.user_agent()
        }
        
        # 3-2. 토픽("bk-web_logs")을 구성하여 전송
        producer.send('web_logs', value=web_log)
        print(f'전송:   {web_log}')

        # 3-3. 강제 전송(버퍼 비움)
        #producer.flush()

        # 3-4. 잠시 대기 -> 주문 간격 랜덤하게 조정
        #interval = 1 + random.random()/10
        #print( interval )
        time.sleep(1)
        pass

if __name__ == '__main__':
    send_msg()