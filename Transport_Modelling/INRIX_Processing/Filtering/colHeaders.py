import numpy as np
import pandas as pd
import os

import File_scripts.directories as DR

def addColHeaders(colHeadsFile,dataDirectory):
    directory = dataDirectory

    ColumnHeaders = pd.read_csv(colHeadsFile)
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            with open(os.path.join(directory, filename)) as f:
                df=pd.read_csv(os.path.join(directory, filename),header=None)
                ColHead = ColumnHeaders.copy()

                
                df.columns =ColumnHeaders.columns
                DataWithHeads = df

                DR.makeNewDir(dataDirectory+'_Hdrs')
                DataWithHeads.to_csv(dataDirectory+'_Hdrs/'+filename,index=False)
                print(DataWithHeads)
    return DataWithHeads