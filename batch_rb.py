'''
Created on Nov 22, 2017

@author: ethan

Python3 64bit pandas v0.20
'''
import multiprocessing
import os
import pandas as pd
from datetime import datetime
import sys



def psrun(symbol, contract, date):

    os.system('python3 clean.py '+ symbol + ' ' + contract + ' ' + date)

if __name__ == '__main__':
    p = multiprocessing.Pool(processes = 6)
    symbol = sys.argv[1]
    contract = sys.argv[2]
    date_range = pd.Series(pd.date_range(start=datetime(2017,10,1), end=datetime(2017,11,30), freq='D')).dt.date
    date_range = date_range.apply(lambda x: x.strftime('%Y%m%d'))
    for date in date_range:
        p.apply_async(psrun,(symbol, contract, date))
    print('Start processing...')
    p.close()
    p.join()
    print('Done...')
