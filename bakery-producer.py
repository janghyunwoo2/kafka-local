'''
일반 고객이 특정 매장의 메뉴를 주문한다. 
주문 발생(로그) 
- 스마트폰 앱에 탑재
'''
# 1. 모듈 가져오기
from kafka import KafkaProducer
import json
import time
import random
import logging
from datetime import datetime

# 2. 카프카 연결
producer = KafkaProducer(
    bootstrap_servers = ['127.0.0.1:9092'], # docker 상에 존재하는 kafka 주소
    value_serializer  = lambda x: json.dumps(x).encode('utf-8')  # DICT->문자열 직열화
)

menus = ['빅맥세트','김밥세트','돈까스세트','식빵','케이크']

# 3. 전송
def send_msg():
    '''
    더미로 가상 주문 데이터 생성    
    '''
    while True:
        # 3-1. 가상 주문 생성
        order = {
            "order_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "store_cd"  : 'BK-2026-00001',
            "menu"      : random.choice(menus),
            "count"     : random.randint(1,5)
        }
        # 3-2. 토픽("bk-orders")을 구성하여 전송
        producer.send('bk-orders', value=order)
        print(f'주문:{order['menu']} 수량:{order['count']}')

        # 3-3. 강제 전송(버퍼 비움)
        producer.flush()

        # 3-4. 잠시 대기 -> 주문 간격 랜덤하게 조정
        interval = 1 + random.random()/10
        #print( interval )
        time.sleep(interval)
        pass

if __name__ == '__main__':
    send_msg()