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


# kafka > kafka connect > s3 : xx 데이터 실시간 업로드
## 특징
- kafka producer가 전송하는 데이터를 별도의 코드 없이 설정으로 s3에 업로드
- 설정에서 패턴 정의
- 데이터는 페이크데이터를 활용 (패키지 faker를 사용)
    ```
    pip install Faker
    ```
- 로그 발생, 전송등 포지션상 airflow 내부에 포함 x (별도 프로젝트로 진행)
- 데이터 - 웹로그 - json 포멧
    ```
        ip, timestamp, method, url, status_code, user_agent
    ```

## 데이터 파이프라인
- 로그 발생 -> kafka producer 메세지 전송 -> kafka 서버 도달 - kafka connect -> s3
    - docker-compose.yaml 서비스 추가 필요
        - kafka connect 추가 필요
    - 패텉 정의 -> 설정 파일 -> s3_conn_config.json (이름은 커스텀)
        - 데이터가 10개가 모이면 s3에 업로드 (버퍼링 10개로 잡음, 컨셉)
        - 로그가 10개가 아주 늦게 모이거나, 도달하지 않으면 전송이 않되는 상황을 고려하여 일정 시간 지나면 자동 업로드(인터벌)
        - 버킷준비 (본인 계정)

- kafka connect
    - S3에 적제 담당