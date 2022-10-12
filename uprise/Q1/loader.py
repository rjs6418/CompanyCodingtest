import numpy as np
import pandas as pd
import time

start = time.time()
# S&P 500 Ticker 리스트
with open('./data/ticker.csv') as f:
    ticker_list = f.read().strip().split('\n')
# 2021년 날짜 리스트
with open('./data/date.csv') as f:
    date_list = f.read().strip().split('\n')
matrix = np.empty((len(date_list), len(ticker_list))) * np.nan

with open('./data/data.csv') as f:
    header = f.readline()
    line = f.readline()
    while line:
        date, ticker, volume = line.strip().split(',')
        for ii, t in enumerate(ticker_list):
            if ticker == t:
                break
        for di, d in enumerate(date_list):
            if date == d:
                break
        matrix[di, ii] = float(volume)
        line = f.readline()
df = pd.DataFrame(matrix, columns = ticker_list, index=date_list)
print(df)
print("Calculate Time: ", time.time() - start)

## Calculate Time:  10.295642137527466