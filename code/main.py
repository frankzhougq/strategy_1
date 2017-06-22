#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 22 13:01:12 2017

@author: frankzhougq
"""

import pandas as pd
import numpy as np
import datetime as dt

#%% # function
def cal_return_early(sk):
    # calculate the return of each stock in the first hour of each trading day
    # sk : stock
    # the first hour
    start_time = dt.time(9, 30)
    end_time = dt.time(10, 30)
    
    con = (sk.time <= end_time) & (sk.time >= start_time)
    sk_early = sk.loc[con, ['open', 'close', 'time']]
    
    if sk_early.shape[0] <= 1:
        return None
    
    ret_early = (sk_early.close.iloc[-1] - sk_early.open.iloc[0]) / sk_early.open.iloc[0]
    
    return ret_early

def cal_return_late(sk):
    # calculate the return of each stock in the last half hour of each trading day
    # sk : stock
    start_time = dt.time(14,30)
    end_time = dt.time(15,0)
    
    con = (sk.time >= start_time) & (sk.time <= end_time)
    sk_late = sk.loc[con, ['open', 'close', 'time']]
    
    if sk_late.shape[0] <= 1:
        return None
    
    ret_late = (sk_late.close.iloc[-1] - sk_late.open.iloc[0]) / sk_late.open.iloc[0]
    
    return ret_late

def cal_stock_future_return(sk_ts, datelist, new_datelist, forward_days = 5):
    '''
    cal stock future return
    ========
    sk_ts: stock time series
    datelist: sample period (all the trading days)
    new_datelist: datelist.shift(- forward_days)
    forward_days: the future time window
    '''
    date_table = pd.concat([datelist, new_datelist], axis = 1).rename(columns = {0 : 'today', 1 : 'future'})
    
    y = pd.merge(sk_ts, date_table, left_on = 'date', right_on = 'today').drop(['close', 'date'], axis = 1)
    y = y.dropna()
    y = pd.merge(y, sk_ts[['date', 'close']], left_on = 'future', right_on = 'date').drop('date', axis = 1).rename(columns = {'close': 'close_f', 'today': 'date'})
    y = y.dropna()
    y['ret_f'] = (y.close_f - y.open) / y.open
    y = y.set_index('date')
    
    return y

def pre_return_future(price, datelist, forward_days = 5):
    '''
    prepare for the future return data
    ================
    price: [df]
    datelist: all the trading days during the sample
    forward_days: the future return calculation window
    '''
    new_datelist = datelist.shift(- forward_days)
    gp = price.groupby('code')
    ret_f = gp['date', 'open', 'close'].apply(cal_stock_future_return, datelist, new_datelist, forward_days)
    ret_f = ret_f.reset_index().set_index(['date', 'code'])
    
    return ret_f
#%%
#==============================================================================
# Prepare data for analysis.
# ==============
# ret_future: future return
# ret_early: return during the first hour of each trading day
# ret_late: return during the last half hour of each trading day

# df: combinations of data above 
#==============================================================================

forward_days = 5
#------------
data_tick = pd.read_pickle('data/data_tick.pkl')
data_price = pd.read_pickle('data/data_daily_price.pkl')

datelist = pd.Series(np.unique(data_tick.date))    # datelist of data_tick
datelist = datelist.dt.date
price = data_price.copy()                          # price
price['date'] = price.date.dt.date


ret_future = pre_return_future(price, datelist, forward_days)

gp = data_tick.groupby(['date', 'code'])
ret_early = gp.apply(cal_return_early).rename('ret_early') # name the variable
ret_early = ret_early.dropna()
ret_late = gp.apply(cal_return_late).rename('ret_late')
ret_late = ret_late.dropna()

df = pd.concat([ret_future, ret_late, ret_early], axis = 1, join = 'inner').dropna()
#%%
#==============================================================================
#calculate correlations between future returns and explanatory variables (return in the early stage of today, return in the last half hour today).
#
#res_corr: correlations
#==============================================================================

res_corr = pd.Series()
for i in np.arange(8):
    ret_future_tmp = pre_return_future(price, datelist, i) # 有点麻烦可以计算之后存储
    
    df = pd.concat([ret_future_tmp, ret_late, ret_early], axis = 1, join = 'inner')
    x = df.corr().loc['ret_f'].rename(i)
    
    if len(res_corr) == 0:
        res_corr = x
    else:
        res_corr = pd.concat([res_corr, x], axis = 1)

  


