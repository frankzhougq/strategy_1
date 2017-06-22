#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 16:02:41 2017

@author: frankzhougq
"""
import tushare as ts
import pandas as pd
import datetime as dt
import numpy as np
#%% # function
def trans_timestamp2datestr(datetime):
    date_str = str(datetime)[:10]
    return date_str

def pre_download_stock_data(code, start = None, end = None, ktype = '15'):
    df = ts.get_k_data(code, start, end, ktype = ktype, autype = 'hfq', retry_count = 10)
    if (ktype == '15') & (df.shape[0] < 10):
        return 1

    return df

def trans_download_data(df):
    df['datetime'] = pd.to_datetime(df['date'])
    df['date'] = df.datetime.dt.date
    df['time'] = df.datetime.dt.time

    return df

def pre_tickdata(codelist, ktype = '15'):

    res = pd.DataFrame()
    for code in codelist:
        df = pre_download_stock_data(code, ktype)
        if isinstance(df, int):
            continue
        df = trans_download_data(df)
        res = pd.concat([df, res], axis = 0)

    return res

def pre_dailydata(codelist, start, end, ktype = 'D'):
    
    res = pd.DataFrame()
    for code in codelist:
        df = pre_download_stock_data(code, start, end, ktype)
        df['date'] = pd.to_datetime(df.date)
        res = pd.concat([df, res], axis = 0)
        
    return res
        
#%% # tick data
stock_basics = ts.get_stock_basics()
codelist = stock_basics.index

df = pre_tickdata(codelist, ktype = '15')

df['date'] = pd.to_datetime(df['date'])

df.to_csv('data/data_tick.csv')
df.to_pickle('data/data_tick.pkl')
#%% # price data
stock_basics = ts.get_stock_basics()
codelist = stock_basics.index

start_date = '2015-11-01'
end_date = '2017-06-21'

record = list()
df = pd.DataFrame()
for code, i in zip(codelist, np.arange(1, len(codelist)+1)):
    tmp = pre_download_stock_data(code, start = start_date, end = end_date, ktype = 'D')
    print ('No ' + str(i) + '  ' + code + ' has ' + str(tmp.shape[0]) + ' observations')
    record.append([code, tmp.shape[0]])
    df = pd.concat([df, tmp], axis = 0)

df['date'] = pd.to_datetime(df['date'])
# df is the data we want
# record: records the [code, observations]   

df.to_pickle('data/data_daily_price.pkl') 
    