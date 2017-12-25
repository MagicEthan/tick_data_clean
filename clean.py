'''
Created on Nov 16, 2017

@author: ethan

Tick data cleaner for China commodities futures and commodities options

Python3 64bit pandas v0.20
'''

import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import os
import sys

# Contracts traded at Shanghai Futures Exchange

# Exchanges
symbols_SHFE = ['rb','ru']
symbols_DCE = ['m','']
symbols_CZCE = []
symbols_CFFEX = []

# For RB only

# Main cleaning function
def cleaning(symbol, date, contract, fill_null=False):

    ts_standard = calculate_ts_standard(symbol, date)
    try:
        data = pd.read_csv('./data/'+str(date)+'/'+str(date)+'/'+symbol+contract+'_'+str(date)+'.csv', index_col=0, header=None)
        print('Processing data for ' + symbol + contract + 'at ' + date)
    except:
        print('No data for ' + symbol + contract + ' at ' + date)
        os._exit(0)
    data_conformed = conform(data,date)
    data_cleaned = align(data_conformed,ts_standard)
    data_cleaned = data_cleaned.iloc[:,1:]
    if fill_null==True:
        data_cleaned.fillna(method='bfill',inplace=True)
        data_cleaned.fillna(method='ffill',inplace=True)
    data_cleaned.index.name = 'timestamp'
    data_cleaned.columns = ['ask','ask_size','bid','bid_size','close','volume','open_interest']

    return data_cleaned


# Generate the standard timestamp
def calculate_ts_standard(symbol, date):
    if symbol in symbols_SHFE:
        if type(date) != str:
            date = str(date)
        date_last = datetime.strftime((datetime.strptime(date, '%Y%m%d') - timedelta(days=1)),'%Y%m%d')

        ts_standard_pt1 = pd.date_range(start= date_last+' 210000',
                                    end = date_last+' 230000',
                                    freq='500ms')

        ts_standard_pt2 = pd.date_range(start= date+' 090000',
                                    end = date+' 101500',
                                    freq='500ms')

        ts_standard_pt3 = pd.date_range(start= date+' 103000',
                                    end = date+' 113000',
                                    freq='500ms')

        ts_standard_pt4 = pd.date_range(start= date+' 133000',
                                    end = date+' 150000',
                                    freq='500ms')

        ts_standard = ts_standard_pt1.union(ts_standard_pt2).\
                        union(ts_standard_pt3).union(ts_standard_pt4)



    elif symbol in symbols_DCE:
        if type(date) != str:
            date = str(date)
        date_last = datetime.strftime((datetime.strptime(date, '%Y%m%d') - timedelta(days=1)),'%Y%m%d')

        ts_standard_pt1 = pd.date_range(start= date_last+' 210000',
                                    end = date_last+' 233000',
                                    freq='500ms')

        ts_standard_pt2 = pd.date_range(start= date+' 090000',
                                    end = date+' 101500',
                                    freq='500ms')

        ts_standard_pt3 = pd.date_range(start= date+' 103000',
                                    end = date+' 113000',
                                    freq='500ms')

        ts_standard_pt4 = pd.date_range(start= date+' 133000',
                                    end = date+' 150000',
                                    freq='500ms')

        ts_standard = ts_standard_pt1.union(ts_standard_pt2).\
                        union(ts_standard_pt3).union(ts_standard_pt4)

    return ts_standard






# Standardizing data
def conform(data, date):
    """Processing the raw data to conform to standardized format.

    The raw data will be prefixed with specific date, and dropped from
    abnormal timestamps and duplicated timestamps.

    Args:
        data: the raw data to be used.
        date: the date to be prefixed.

    Returns:
        data_conformed: the conformed data.

    """

    ts_original = data.index.values

    # Prefix the raw data with the date and drop the abnormal timestamps
    # If not in accordance with the following format it will return a null timestamps
    ts_conformed = pd.Series(pd.to_datetime(date +' ' + ts_original, format='%Y%m%d %H:%M:%S.%f',errors='coerce'))
    ts_conformed = ts_conformed.apply(lambda x: x - timedelta(days=1) if x.hour >= 20 else x)
    data.set_index(ts_conformed,inplace=True)

    # Drop row with null timestamps, thus dropped out the abnormal timestamps
    # pandas version > 0.20
    data = data[data.index.notnull()]
    # Else try the following:
    # data = data.reset_index().dropna().set_index('index')
    # data.index.names = [None]

    # Drop duplicate index
    data_conformed = data[~data.index.duplicated(keep='last')]


    return data_conformed

# Merge and align
def align(data_conformed, ts_standard):
    """Align data according to standard timestamps.

    Args:
        data_conformed: the conformed data to be used.
        ts_standard: the standard timstamps.

    Returns:
        data_left_join: the data aligned.
    """


    df_standard = pd.DataFrame(np.zeros(len(ts_standard)), index=ts_standard)
    df_left_join = pd.merge(df_standard,data_conformed,left_index=True, right_index=True, how='left')

    return df_left_join

def run(symbol, contract, date):
    outdir='./data/clean'
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    data = cleaning(symbol,date,contract,fill_null=True)
    data.to_csv(os.path.join(outdir,symbol+contract+'_'+str(date)+'.csv'))

if __name__ == '__main__':

    if len(sys.argv)<4:
        print ("usage: python3 " + sys.argv[0] + " symbol(i.e. 'rb') contract(i.e.'1801')  date(i.e.'20171113')")
        exit()
    else:
        run(symbol=str(sys.argv[1]), contract=str(sys.argv[2]), date=str(sys.argv[3]))
