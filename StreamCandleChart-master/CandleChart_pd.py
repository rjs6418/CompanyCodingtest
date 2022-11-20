from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

DATATOPIC = "DATATOPIC"
BROKERS = ["localhost:9091", "localhost:9092", "localhost:9093"]

consumer = KafkaConsumer(DATATOPIC, bootstrap_servers=BROKERS)

class CandleData:
    
    def __init__(self, checkingtime):
        self.checkingtime = checkingtime
        self.transaction_df = pd.DataFrame(columns=['time_exchange', 'time_coinapi', 'guid', 'price', 'base_amount', 'taker_side'])

    def collectData(self, transactiondata):
        self.transaction_df = pd.concat([self.transaction_df, pd.DataFrame([transactiondata], columns=self.transaction_df.columns)])
    
    def makeCandleData(self):
        df_high_low = list(self.transaction_df.aggregate({'price':['max','min'], 'base_amount':np.sum}).stack())
        open, close = list(self.transaction_df.head(1)['price']), list(self.transaction_df.tail(1)['price'])
        return (str(self.checkingtime), str(self.checkingtime + timedelta(minutes=1))) + tuple(open + df_high_low[:2] + close + [df_high_low[-1]])

start_time = str(datetime.now())
checkingtime = datetime.strptime(start_time[:start_time.rfind(':')+1] + '00', '%Y-%m-%d %H:%M:%S')
candleData = CandleData(checkingtime)
for message in consumer:
    msg = json.loads(message.value.decode())
    new_time = datetime.strptime(msg[0][:msg[0].rfind(':')+1] + '00', '%Y-%m-%d %H:%M:%S')
    if checkingtime != new_time:
        candleRecord = candleData.makeCandleData()
        print(candleRecord)
        checkingtime = new_time
        candleData = CandleData(checkingtime)
    candleData.collectData(msg)
