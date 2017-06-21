#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 16:02:41 2017

@author: frankzhougq
"""
import tushare as ts
import pandas as pd

#%% # function
def trans_timestamp2datestr(datetime):
    date_str = str(datetime)[:10]
    return date_str

def pre_download_single_tickdata(code, ktype = '15'):
    df = ts.get_k_data(code, ktype = ktype, autype = 'hfq', retry_count = 10)
    if df.shape[0] < 10:
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
        df = pre_download_single_tickdata(code, ktype)
        if isinstance(df, int):
            continue
        df = trans_download_data(df)
        res = pd.concat([df, res], axis = 0)

    return res
#%%
stock_basics = ts.get_stock_basics()
codelist = stock_basics.index

df = pre_tickdata(codelist, '15')

df['date'] = df.date.dt.date

df.to_csv('data/data_tick.csv')
df.to_pickle('data/data_tick.pkl')

# this is an additional text
