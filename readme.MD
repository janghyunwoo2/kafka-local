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
- docker상 컨테이너 내부의 카프카 위치
- cd /opt/kafka/bin
- ls
    - 각종 쉘 프로그램 확인 가능함
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

# 향후 파이프라인
- 센서|웹|IOT/로그,데이터 발생 -> kafka producer 전송 -> kafka 서버 적제
    - kafka 서버 -> kafka consumer 수신
    - kafka 서버 -> kafka consumer 수신 -> s3 : 수신후 업로드 -> 약간의 지연 발생
    - kafka 서버 -> kafka connect -> s3 : 즉시 업로드 -> 패턴 설계 (10개가 쌓이면 업로드등)
    - kafka 서버 -> Logstash(ELK활용) -> s3 : 즉시 업로드 -> 패턴 설계 (10개가 쌓이면 업로드등)
                                     -> 전처리(ETL) -> opensearch
                                     -> xxxx
                                     -> xxxx
    - kafka 서버 -> Fluent bit(EFK활용) -> s3
                                       -> ...
    - kafka 서버 -> Spark Structured Streamming(대규모 복잡한 가공처리,전처리) -> s3
        - 단순하게 적제하는 것이면 오버스펙임
        - kafka 서버 -> Spark Streamming (AWS EMR) -> s3
