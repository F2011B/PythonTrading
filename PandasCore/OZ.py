import pandas as pd
import datetime

import matplotlib as plt
import sys
import os
import inspect
import unittest

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)


def insert_module(module_folder):
    cmd_subfolder = os.path.realpath(
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],
                                     '..', module_folder)))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)


insert_module('Constants')
insert_module('DataProviderAccess')

import Oanda
import Constants
import CandleHelper
import logging


my_logger = None


def set_logger(logger):
    my_Logger = logger


plt.interactive(False)


def open_zero(symbol):
    value_zero = calc_ozv(symbol)
    return value_zero


def calc_ozv(symbol):
    numdays_back = 2
    date, open_price, high, close, low, volume = CandleHelper.get_DOHLCV(numdays_back, symbol)
    new_close = CandleHelper.ensure_correct_ascending_timeseries(date, close)
    ma_h, ma_l, w_open, w_date = calc_oz_series(symbol)
    value_zero = new_close[len(new_close)-1] - w_open[len(w_open)-1]
    if value_zero < 0:
        value_zero = value_zero / ma_l[len(ma_l)-1]
    else:
        value_zero = value_zero / ma_h[len(ma_h)-1]
    return value_zero
    
    
def get_weekly_df(num_weeks_back, symbol):
    if symbol in Oanda.OandaSymbols:
        weekly_df = Oanda.get_weekly_dohlcv_pandas(num_weeks_back, symbol)
    else:
        weekly_df = CandleHelper.get_weekly_dohlcv_pandas(num_weeks_back, symbol)
    return weekly_df


def get_intra_day_df(num_weeks_back, symbol, time_frame):
    if symbol in Oanda.OandaSymbols:
        return Oanda.get_intraDay_DOHLCV_pandas(num_weeks_back, symbol)
    else:
        if time_frame == '1d':
            return CandleHelper.get_daily_DOHLCV_pandas(symbol, num_weeks_back)

        return CandleHelper.get_intraDay_DOHLCV_pandas(symbol, num_weeks_back)


def calc_oz_series_pandas(symbol, num_weeks_back=20, average_tf='W'):
    time_frame_map = {'W': (1 * num_weeks_back),
                      '3M': (num_weeks_back * 15),
                      'Q': (num_weeks_back * 15),
                      'M': (num_weeks_back * 4)}
    print(Constants.StockHDF)
    store = pd.HDFStore(Constants.StockHDF)
    symbol_key = symbol + '_' + average_tf

    today = datetime.datetime.now()
    day_of_week = today.weekday()
    week_start = today - datetime.timedelta(days=day_of_week + 1)

    if not (symbol_key in store.keys()):
        print('Symbol:'+symbol)
        weekly_df = get_weekly_df(time_frame_map[average_tf], symbol)
        new_df = calc_oz_pandas(weekly_df, average_tf=average_tf)
        store[symbol_key] = new_df
        store.flush()

    len_store = len(store[symbol_key]) - 1
    if not (store[symbol_key].index[len_store].date() == week_start.date()):
        weekly_df = get_weekly_df(time_frame_map[average_tf], symbol)
        new_df = calc_oz_pandas(weekly_df, average_tf=average_tf)
        store[symbol_key] = new_df
        store.flush()

    return store[symbol_key]


def calc_oz_pandas(df, average_tf='W'):
    print('test')
    print(average_tf)
    new_df = Oanda.resample_DOHLCV_pandas(df, average_tf)
    new_df['DHi'] = new_df['High'] - new_df['Open']
    new_df['DLo'] = new_df['Low'] - new_df['Open']
    new_df['MLo'] = new_df['Open'] + new_df['DLo'].rolling(window=10).mean().shift(1)
    new_df['MHi'] = new_df['Open'] + new_df['DHi'].rolling(window=10).mean().shift(1)
    return new_df


def generate_overlay_oz_pandas(interval, symbol, num_weeks_back=20, average_tf='W'):
    ozw = calc_oz_series_pandas(symbol, num_weeks_back=20, average_tf=average_tf)
    ozw = ozw.shift(1, freq=average_tf).resample(CandleHelper.intervalMapPandas[interval]).bfill()
    intra_df = get_intra_day_df(num_weeks_back, symbol, interval)
    if interval == '1d':
        intra_df.reset_index(inplace=True)
        del intra_df['index']
        intra_df['Time'], intra_df['Date'] = intra_df['Date'].apply(
            lambda x: x.time()), intra_df['Date'].apply(lambda x: x.date())
        intra_df.set_index('Date', inplace=True)

    ozw.rename(columns={'Open': 'wOpen'}, inplace=True)
    intra_df = pd.concat([intra_df, ozw['MLo'], ozw['MHi'], ozw['wOpen']],
                         axis=1, join_axes=[intra_df.index])
    return intra_df


def generate_overlays_oz_pandas(interval, symbol, num_weeks_back=20):
    pandas_interval = CandleHelper.intervalMapPandas[interval]

    intra_df = get_intra_day_df(num_weeks_back, symbol, interval)
    if interval == '1d':
        intra_df.reset_index(inplace=True)
        del intra_df['index']
        intra_df['Time'], intra_df['Date'] = intra_df['Date'].apply(
            lambda x: x.time()), intra_df['Date'].apply(lambda x: x.date())
        intra_df.set_index('Date', inplace=True)

    df_w = calc_oz_series_pandas(symbol, num_weeks_back=num_weeks_back, average_tf='W')
    df_w.rename(columns={'Open': 'wOpen', 'MLo': 'wLo', 'MHi': 'wHi'}, inplace=True)
    df_w = df_w.shift(1, freq='W').resample(pandas_interval).bfill()
    df_m = calc_oz_series_pandas(symbol, num_weeks_back=num_weeks_back, average_tf='M')
    df_m = df_m.shift(1, freq='M').resample(pandas_interval).bfill()
    df_m.rename(columns={'Open': 'mOpen', 'MLo': 'mLo', 'MHi': 'mHi'}, inplace=True)
    df_q = calc_oz_series_pandas(symbol, num_weeks_back=num_weeks_back, average_tf='Q')
    df_q.rename(columns={'Open': 'qOpen', 'MLo': 'qLo', 'MHi': 'qHi'}, inplace=True)
    df_q = df_q.shift(1, freq='Q').resample(pandas_interval).bfill()
    df = pd.concat([intra_df, df_w['wOpen'], df_w['wLo'], df_w['wHi'],
                   df_m['mOpen'], df_m['mLo'], df_m['mHi'],
                   df_q['qOpen'], df_q['qLo'], df_q['qHi']],
                   axis=1, join_axes=[intra_df.index])

    return df


class MyTestCase(unittest.TestCase):
    def test_calc_multiple(self):
        endDate = datetime.datetime.now()
        startDate = endDate - datetime.timedelta(weeks=200)
        #DF=getDataFromStartDate(startDate, 'WTICO_USD', 'H1', 0)
        print('test')

        df = generate_overlays_oz_pandas('1h', 'CORN_USD', num_weeks_back=20).dropna()
        print(df)
    def test_HDF_Path(self):
        print(Constants.home)
        print(Constants.Database)
