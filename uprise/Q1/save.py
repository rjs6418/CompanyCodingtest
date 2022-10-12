import dask.dataframe as dd
import numpy as np
import pickle as pkl

class SaveNewTypeFile:
    
    def __init__(self, file):
        self.file = file
        self.dask_df = []
    
    def readCsvDask(self):
        self.dask_df = dd.read_csv(self.file)

    def SavePickle(self):
        with open(self.file[:self.file.rfind('.')] + '.pkl', 'wb') as f:
            pkl.dump(self.dask_df, f)

    def SaveNpy(self):
        np.save(self.file[:self.file.rfind('.')], self.dask_df)

def packageNewTypeFile(file):
    classFile = SaveNewTypeFile(file)
    classFile.readCsvDask()
    classFile.SavePickle()
    classFile.SaveNpy()

file_list = ['./data/data.csv', './data/date.csv', './data/ticker.csv']

for file in file_list:
    packageNewTypeFile(file)