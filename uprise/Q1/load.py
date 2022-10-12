import time

'''
    .csv: 3.7M
    .pkl: 2.0M
    .npy: 3.2M
'''

# pandas 
# readCsv Time:  0.06675386428833008
# readPkl Time:  0.5270810127258301

import pandas as pd
start_pd = time.time()

pd_df = pd.read_csv('./data/data.csv')
pd_df = pd.read_pickle('./data/data.pkl')
print(pd_df)

print("Calculate Time: ", time.time() - start_pd)



#numpy
# readCsv Time:  0.3350710868835449
# readPkl Time:  0.09715008735656738
# readNpy Time:  0.036226749420166016

import csv
import numpy as np
import pickle as pkl
start_np = time.time()

with open('./data/data.csv', 'r') as f:
    data = list(csv.reader(f, delimiter=";"))

with open('./data/data.pkl', 'rb') as f:
    data = pkl.load(f)
np_df = np.array(data)

data = np.load('./data/data.npy', allow_pickle=True)
print(data)

print("Calculate Time: ", time.time() - start_np)



# pyarray 
# readCsv Time:  0.030627012252807617

from pyarrow import csv
start_pyarrow = time.time()

pyarrow_df = csv.read_csv('./data/data.csv').to_pandas()
print(pyarrow_df)

print("Calculate Time: ", time.time() - start_pyarrow)



# dask 
# readCsv Time:  0.005658864974975586
import dask.dataframe as dd
start_dask = time.time()

dask_df = dd.read_csv('./data/data.csv')
print(dask_df)

print("Calculate Time: ", time.time() - start_dask)


