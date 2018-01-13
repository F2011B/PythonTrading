import pandas as pd
import datetime

import matplotlib as plt
import sys
import os
import inspect
import unittest

#This is a comment
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

def insertModule(ModuleFolder):
    cmd_subfolder = os.path.realpath(
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],'..', ModuleFolder)))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)

insertModule('Constants')
insertModule('DataProviderAccess')

import Oanda
import Constants
import CandleHelper
import logging


Logger=None

def setLogger(logger):
    Logger=logger




plt.interactive(False)

def open_zero(symbol):
    value_zero = calc_ozv(symbol)
    return value_zero

def calc_ozv(symbol):
    numdays_back=2
    date, open_price, high, close, low, volume = CandleHelper.get_DOHLCV(numdays_back, symbol)
    new_close = CandleHelper.ensure_correct_ascending_timeseries(date, close  );
    ma_h, ma_l, w_open, w_date = calc_oz_series(symbol)
    value_zero = new_close[len(new_close)-1] - w_open[len(w_open)-1]
    if value_zero < 0:
        value_zero = value_zero / ma_l[len(ma_l)-1]
    else:
        value_zero = value_zero / ma_h[len(ma_h)-1]
    return value_zero
    return ma_h, ma_l, w_open, w_date
    
    
def get_weekly_df(num_weeks_back, symbol):
    if symbol in Oanda.OandaSymbols:
        weekly_df = Oanda.get_weekly_dohlcv_pandas(num_weeks_back, symbol)
    else:
        weekly_df = CandleHelper.get_weekly_dohlcv_pandas(num_weeks_back, symbol)
    return weekly_df

def getIntradDayDF(num_weeks_back, symbol, timeFrame):
    if symbol in Oanda.OandaSymbols:
        return Oanda.get_intraDay_DOHLCV_pandas(num_weeks_back, symbol)
    else:
        if timeFrame== '1d':
            return CandleHelper.get_daily_DOHLCV_pandas(symbol, num_weeks_back)

        return  CandleHelper.get_intraDay_DOHLCV_pandas(symbol, num_weeks_back)

def calc_oz_series_pandas(symbol, num_weeks_back=20, averageTf='W'):
    timeFrameMap={'W':(1 * num_weeks_back),
                  '3M':(num_weeks_back * 15),
                  'Q':(num_weeks_back * 15),
                  'M':(num_weeks_back * 4)}
    print(Constants.StockHDF)
    store = pd.HDFStore(Constants.StockHDF)
    symbolKey = symbol + '_'+ averageTf

    today = datetime.datetime.now()  # - datetime.timedelta(days=1)
    day_of_week = today.weekday()
    weekStart = today - datetime.timedelta(days=day_of_week + 1)

    if not (symbolKey in store.keys()):
        print('Symbol:'+symbol)
        weekly_DF = get_weekly_df(timeFrameMap[averageTf], symbol)
        #print(weekly_DF)
        newDF=calc_oz_pandas(weekly_DF, average_tf=averageTf)
        store[symbolKey] = newDF
        store.flush()
        #print('READ')


    lenStore = len(store[symbolKey]) - 1
    if not (store[symbolKey].index[lenStore].date() == weekStart.date()):
        weekly_DF = get_weekly_df(timeFrameMap[averageTf], symbol)
        newDF=calc_oz_pandas(weekly_DF, average_tf=averageTf)
        store[symbolKey] = newDF
        store.flush()

    return store[symbolKey]


def calc_oz_pandas(df, average_tf='W'):
    print('test')
    print(average_tf)
    newDF=Oanda.resample_DOHLCV_pandas(df, average_tf)
    #print(newDF['High'][0])
    newDF['DHi'] = newDF['High'] - newDF['Open']
    #print(newDF['DHi'])
    newDF['DLo'] = newDF['Low'] - newDF['Open']
    newDF['MLo'] = newDF['Open'] + newDF['DLo'].rolling(window=10).mean().shift(1)
    newDF['MHi'] = newDF['Open'] + newDF['DHi'].rolling(window=10).mean().shift(1)
    return newDF


def generate_overlay_oz_pandas(interval, symbol, num_weeks_back=20, averageTf='W'):
    ozw = calc_oz_series_pandas(symbol, num_weeks_back=20, averageTf=averageTf)
    ozw = ozw.shift(1, freq=averageTf).resample(CandleHelper.intervalMapPandas[interval]).bfill()
    #print(OZW)
    intra_df = getIntradDayDF(num_weeks_back, symbol, interval)
    if interval == '1d' :
        intra_df.reset_index(inplace=True)
        del intra_df['index']
        intra_df['Time'], intra_df['Date'] = intra_df['Date'].apply(lambda x: x.time()), intra_df['Date'].apply(lambda x: x.date())
        intra_df.set_index('Date', inplace=True)

    ozw.rename(columns={'Open':'wOpen'},inplace=True)
    intra_df = pd.concat([intra_df, ozw['MLo'], ozw['MHi'],ozw['wOpen']],axis=1,join_axes=[intra_df.index])
    #print(intra_DF)
    return intra_df

def generate_overlays_oz_pandas(interval, symbol,numWeeksBack=20, averageTf='W'):
    pandasInterval=CandleHelper.intervalMapPandas[interval]
    OZW=calc_oz_series_pandas(symbol, num_weeks_back=numWeeksBack, averageTf=averageTf)
    OZW=OZW.shift(1, freq=averageTf).resample(pandasInterval).bfill()
    #print(OZW)
    intra_DF = getIntradDayDF(numWeeksBack, symbol, interval)
    if interval == '1d' :
        intra_DF.reset_index(inplace=True)
        del intra_DF['index']
        intra_DF['Time'], intra_DF['Date'] = intra_DF['Date'].apply(lambda x: x.time()), intra_DF['Date'].apply(lambda x: x.date())
        intra_DF.set_index('Date', inplace=True)

    DFw= calc_oz_series_pandas(symbol, num_weeks_back=numWeeksBack, averageTf='W')
    DFw.rename(columns={'Open': 'wOpen', 'MLo': 'wLo','MHi':'wHi'}, inplace=True)
    DFw= DFw.shift(1, freq='W').resample(pandasInterval).bfill()
    DFm = calc_oz_series_pandas(symbol, num_weeks_back=numWeeksBack, averageTf='M')
    DFm = DFm.shift(1, freq='M').resample(pandasInterval).bfill()
    DFm.rename(columns={'Open': 'mOpen', 'MLo': 'mLo', 'MHi': 'mHi'}, inplace=True)
    DFq = calc_oz_series_pandas(symbol, num_weeks_back=numWeeksBack, averageTf='Q')
    DFq.rename(columns={'Open': 'qOpen', 'MLo': 'qLo', 'MHi': 'qHi'}, inplace=True)
    DFq = DFq.shift(1, freq='Q').resample(pandasInterval).bfill()
    #print(DFq.index)
    #print(intra_DF.index)
    DF = pd.concat( [intra_DF,DFw['wOpen'], DFw['wLo'], DFw['wHi'],
                     DFm['mOpen'], DFm['mLo'], DFm['mHi'],
                     DFq['qOpen'], DFq['qLo'], DFq['qHi']],
                    axis=1, join_axes=[intra_DF.index])



    #OZW.rename(columns={'Open':'wOpen'},inplace=True)
    #intra_DF = pd.concat([intra_DF, OZW['MLo'], OZW['MHi'],OZW['wOpen']],axis=1,join_axes=[intra_DF.index])
    #print(intra_DF)
    #return intra_DF
    return DF


class MyTestCase(unittest.TestCase):
    def test_calc_multiple(self):
        endDate = datetime.datetime.now()
        startDate = endDate - datetime.timedelta(weeks=200)
        #DF=getDataFromStartDate(startDate, 'WTICO_USD', 'H1', 0)
        print('test')

        DF = generate_overlays_oz_pandas('1h', 'CORN_USD', numWeeksBack=20).dropna()
        print(DF)
    def test_HDF_Path(self):
        print(Constants.home)
        print(Constants.Database)

