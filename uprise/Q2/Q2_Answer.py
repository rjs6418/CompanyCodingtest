from numpy import NaN
import pandas as pd
import numpy as np
from datetime import datetime

'''
    데이터 누락 구분
    
    1. 조치가 필요없는 경우
        taker_side가 UNKNOWN인 경우
        
    2. 데이터를 삭제 해야하는 경우
        SERVER IS DOWN
        CONNECTION FAIL
        price, base_amount, taker_side 모두 누락된 경우
        time이 2021-10-21이 아닌 경우
        
    3. 전처리가 가능한 경우
        time_coinapi만 누락된 경우
        guid만 누락된 경우
        time_coinapi, guid 모두 누락된 경우
        price만 누락된 경우
        base_amount만 누락된 경우 
'''
'''
    데이터 레코드 수 를 맞추기 위해 빈데이터 프레임을 생성하여 진행
'''

df = pd.read_csv('./OKX_SPOT_DOGE_USDT.csv')

# SERVER IS DOWN
index_SID = df[df['time_exchange'] == 'SERVER IS DOWN'].index
df = df.drop(index=index_SID)

# CONNECTION FAIL
index_CF = df[df['time_exchange'] == 'CONNECTION FAIL'].index
df = df.drop(index=index_CF)

# price, base_amount, taker_side 모두 누락된 경우
df = df.dropna(subset = ['price'])

# time이 2021-10-21이 아닌 경우
def check_date(x):
    return x if '2021-10-21' in x else NaN
df['time_exchange'] = df['time_exchange'].apply(check_date)
df['time_coinapi'] = df['time_coinapi'].apply(check_date)
df = df.dropna(subset = ['time_exchange', 'time_coinapi'])

# 전처리가 가능한 경우
# taker_side 기준으로 재정렬
index_trs = df[df['taker_side'].isnull()].index
for index_tr in index_trs:
    df.loc[index_tr] = list(df.loc[index_tr][:1]) + [NaN] + list(df.loc[index_tr][1:-1])

index_trs2 = df[df['taker_side'].isnull()].index
for index_tr2 in index_trs2:
    df.loc[index_tr2] = list(df.loc[index_tr2][:1]) + [NaN] + list(df.loc[index_tr2][1:-1])

# price base_amount float
def is_price(x):
    try:
        return float(x)
    except:
        return NaN
df['price'] = df['price'].apply(is_price)
df['base_amount'] = df['base_amount'].apply(is_price)

# timestamp -> 결과물 형식으로 변환
def to_datetime(x):
    try:
        return datetime.strptime(x[:x.rfind('.') - 2] + '00', '%Y-%m-%dT%H:%M:%S')
    except:
        return datetime.strptime('2021' + x[x.find('-'):x.rfind('.') - 2] + '00', '%Y-%m-%dT%H:%M:%S')
df['window_open'] = df['time_exchange'].apply(to_datetime)

# 기준 데이터 프레임
open_index = list(pd.date_range('2021-10-21 00:00:00', '2021-10-21 23:59:59', freq='T'))
close_index = list(pd.date_range('2021-10-21 00:01:00', '2021-10-22 00:00:00', freq='T'))
result_df = pd.DataFrame({'window_open':open_index, 'window_close':close_index})

# high, low, volume 집계
df_high_low = df.groupby('window_open').aggregate({'price':['max','min'], 'base_amount':np.sum})

# open, close 집계
df_open_close = []
for window_open in open_index:
    try:
        df_agg_min = df[df['window_open'] == window_open]
        df_open_close.append({'window_open' : window_open, 'price_open' : df_agg_min.iloc[0]['price'], 'price_close' : df_agg_min.iloc[-1]['price']})
    except:
        pass
df_open_close = pd.DataFrame(df_open_close)       

# 집계 프레임 join
data_df = pd.merge(df_open_close, df_high_low, on='window_open')

# 결과 프레임 join 및 컬럼 정렬
result_df = pd.merge(result_df, data_df, on='window_open', how='left')
result_df.columns = ['window_open', 'window_close', 'open', 'close', 'high', 'low', 'volume']
result_df = result_df[['window_open', 'window_close', 'open', 'high', 'low', 'close', 'volume']]

result_df.to_csv("./RESULT.csv", mode='w', index=False)
print(result_df)
        


