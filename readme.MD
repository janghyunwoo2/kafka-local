# 설치
## 카프카
```
    docker compose up -d
```
## 파이썬 패키지 설치
```
    pip install kafka-python
```

# 기본 구성
/
L docker-compose.yaml
L bakery-producer.py
L bakery-consumer.py

# 카프카 내에서 CLI 확인
```
# 토픽 생성
kafka-topic.sh --create --topic 토픽명 --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1
# 파티션 1개, 복제본 1개로 준비되는 토픽이다

# 토픽 전송
kafka-console-producer.sh --topic bk-orders --bootstrap-server 127.0.0.1:9092
>> 프럼프트 열림
>> 메세지 입력 (엔터)

# 해당 토픽으로 전달되는 메세지 확인
kafka-console-consumer.sh --topic bk-orders --bootstrap-server 127.0.0.1:9092


```