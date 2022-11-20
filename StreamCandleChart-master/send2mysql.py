from kafka import KafkaConsumer
import json
import pymysql

DATATOPIC = "DATATOPIC"
BROKERS = ["localhost:9091", "localhost:9092", "localhost:9093"]

consumer = KafkaConsumer(DATATOPIC, bootstrap_servers=BROKERS)

conn = pymysql.connect(host = "ec2"
                       , user = "kuno", passwd = "1111", db = "uprise", charset = "utf8")
cursor = conn.cursor()

for message in consumer:
    msg = json.loads(message.value.decode())
    query = f"INSERT INTO transactiondata(time_exchange, time_coinapi, guid, price,base_amount, taker_side) VALUES {tuple(msg)}"
    cursor.execute(query)
    print(query)
    conn.commit()  


