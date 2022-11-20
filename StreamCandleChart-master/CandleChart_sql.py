from kafka import KafkaConsumer
import json
import pymysql
from datetime import datetime, timedelta

DATATOPIC = "DATATOPIC"
BROKERS = ["localhost:9091", "localhost:9092", "localhost:9093"]

consumer = KafkaConsumer(DATATOPIC, bootstrap_servers=BROKERS)

conn = pymysql.connect(host = "ec2", user = "", passwd = "", db = "", charset = "utf8")
cursor = conn.cursor()

def getCandleData(checkingtime):
    window_open = checkingtime
    window_close = checkingtime + timedelta(minutes=1)
    query = f'''SELECT "{window_open}" AS window_open, 
                	   "{window_close}" AS close_open,
                       (SELECT price from transactiondata 
                        WHERE time_exchange >= "{window_open}" AND time_exchange < "{window_close}" 
                        ORDER BY time_exchange LIMIT 1) AS open,
                       max(price) AS high,
                       min(price) AS low,
                       (SELECT price from transactiondata 
                        WHERE time_exchange >= "{window_open}" AND time_exchange < "{window_close}" 
                        ORDER BY time_exchange DESC LIMIT 1) AS close,
                       sum(base_amount) AS volume
                FROM transactiondata
                WHERE time_exchange >= "{window_open}" AND time_exchange < "{window_close}";
            '''
    cursor.execute(query)  
    return cursor.fetchone()

def deleteOldData(checkingtime):
    window_open = checkingtime
    window_close = checkingtime + timedelta(minutes=1)
    query = f'DELETE FROM transactiondata WHERE time_exchange >= "{window_open}" AND time_exchange < "{window_close}";'
    cursor.execute(query)
    conn.commit()  


start_time = str(datetime.now())
checkingtime = datetime.strptime(start_time[:start_time.rfind(':')+1] + '00', '%Y-%m-%d %H:%M:%S')
for message in consumer:
    msg = json.loads(message.value.decode())
    new_time = datetime.strptime(msg[0][:msg[0].rfind(':')+1] + '00', '%Y-%m-%d %H:%M:%S')
    if checkingtime != new_time:
        candleData = getCandleData(checkingtime)
        deleteOldData(checkingtime)
        checkingtime = new_time
        print(candleData)

