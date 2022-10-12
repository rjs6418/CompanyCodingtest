import pickle as pkl
import numpy as np
import pandas as pd
import time

'''
    .csv: 3.7M / .pkl: 2.0M / .npy: 3.2M
    단축 시간: 10.813000917434692 -> 1.288088083267212
'''
''' 
     기존 코드의 경우 라인 한줄 당 index, column 모두 스캔하며 위치를 할당 하였다.
     => 먼저 numpy 배열을 date, ticker 순으로 정렬하여 (lexsort 함수 사용)두가지 컬럼에 대한 정렬 인덱스 출력
     => date에 대한 스캔은 필요없어지며, ticker 또한 범위를 축소해가며 스캔하기에 시간이 크게 감소 
     => 전 데이터 입력 후 데이터프레임 컬럼 순서 재정렬
     /여러번의 sort가 있으나, 데이터 양이 많아 질수록 효율 증가
'''
'''
    .csv, .pkl, .npy 중 데이터 호출 속도는 npy가 가장 빠르나, pkl이 가장 큰폭으로 메모리가 감소하였고 속도 또한 크게 차이 나지 않아 .pkl채택 
'''
'''
    참고: https://numpy.org/doc/stable/reference/generated/numpy.lexsort.html
        https://stackoverflow.com/questions/9619199/best-way-to-preserve-numpy-arrays-on-disk
'''
start = time.time()

with open('./data/ticker.csv') as f:
    ticker_list = f.read().strip().split('\n')
with open('./data/date.csv') as f:
    date_list = f.read().strip().split('\n')
matrix = np.empty((len(date_list), len(ticker_list))) * np.nan
ticker_list_sort = sorted(ticker_list)

with open('./data/data.pkl', 'rb') as f:
    data_pkl = pkl.load(f)
data = np.array(data_pkl)
ind = np.lexsort((data[:,1],data[:,0]))
di, tc = 0, 0
for i in ind:
    date, ticker, volume = data[i]
    if date_list[di] != date:
        di += 1
        tc = 0
    for ii, t in enumerate(ticker_list_sort[tc:], start=tc):
        if ticker == t:
            break
    tc = ii
    matrix[di, ii] = float(volume)
df = pd.DataFrame(matrix, columns = ticker_list_sort, index=date_list)[ticker_list]

print(df)
print("Calculate Time: ", time.time() - start)




