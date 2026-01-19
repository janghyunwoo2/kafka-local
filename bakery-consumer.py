'''
언제 올지 모르는 주문을 대기(시스템  x)상태
고객이 주문을 진행->메세지발생->카프카->전송됨
메세지를 수신하면 주문 확정후 조리/생산 시작됨
- 설치된 단말 기계(포스, 프린터)

- airflow 상에서 구동->1회 가동 무한루프등등 방번은 다양(텀조정) 혹은 데몬처럼 구동중일수도 있음
- 로그를 획득 => 매장에 알림!! (현재) -> 모니터로 띠움, 프린터 출력, 사운드 재생
- 메세징 => 큐(선입선출)로 관리 => 반드시 먼저 입력된 주문이 존재함

- 데이터 파이프라인 관점 : 후속 작업 => s3 저장(raw 데이터 저장), opensearch(가공후 저장)
'''
# 1. 모듈 가져오기
from kafka import KafkaConsumer
import json # 역직렬화 하여 메세지 복원
import time

# 2. 카프카 연결
consumer = KafkaConsumer(
    'bk-orders',    # 구독할 토픽 이름 설정
    bootstrap_servers = ['127.0.0.1:9092'], # 카프카 서버
    auto_offset_reset = 'latest',           # 가장 최신 메세지부터 읽기
    enable_auto_commit = True,
    group_id           = 'factory_group',   # 컨슈머 그룹 ID 지정 -> 여러 커슈머가 같이 수신
    value_deserializer = lambda x: json.loads(x.decode('utf-8')) # 역직렬화
)

print('매장 주문 모니터 가동 -> 주문 대기중')
# 이 프로그램이 가동되면 -> 배민등 플랫폼에서 주문 가능한 상태가 된다!! (여기서는 시그널/이벤트 생략)
today_order_no = 1

# 3. 메세지 수신
def recv_msg():    
    global today_order_no
    while True:
        for msg in consumer:
            data = msg.value            
            print( f'주문 수신 : 주문번호:{today_order_no} {data['menu']} {data['count']}개 주문 접수되었습니다.' )
            today_order_no += 1 
        time.sleep(3)
        print('주문 대기중 ...')

        # 차후 발전 사항 (데이터 엔지니어 관점) -> s3 : 로그 데이터 원본 형태로 저장
        # 차후 발전 사항 (데이터 엔지니어 관점) -> etl : 로그 데이터 가공 -> s3/opensearch
        # kafka -> s3 or ELK or EFK 진입 파이프라인
        # 차후 하루 단위로 결산 -> airflow -> athena or redshift -> 분석 업무 가동
        # 차후 발전 사항 (데이터 엔지니어 관점) -> etl -> s3 -> 모델 학습 -> 모델 업데이트(MLOPS)

        # 로그 : 웹 클릭, 사이트 방문:이커머스, 센서 데이터:스마트팩토리/IOT 등등 

# 4. 프로그램 가동
if __name__ == '__main__':
    recv_msg()




