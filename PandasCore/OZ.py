import pandas as pd
import datetime

#from Modules import Oanda
#from Modules import Constants
import matplotlib as plt
import sys
import os
import inspect
import unittest

#This is a comment
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

# use this if you want to include modules from a subfolder

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
    ValueZero = calc_OZ(symbol)
    return ValueZero

def calc_OZ(symbol):
    numdaysBack=2
    Date, Open, High, Close, Low, Volume = CandleHelper.get_DOHLCV(numdaysBack, symbol)
    aClose=CandleHelper.ensure_correct_ascending_timeseries(Date, Close  );
    MaH, MaL, wOpen, wDate = calc_oz_series(symbol)
    ValueZero = aClose[len(aClose)-1] - wOpen[len(wOpen)-1]
    if ValueZero < 0:
        ValueZero = ValueZero / MaL[len(MaL)-1]
    else:
        ValueZero = ValueZero / MaH[len(MaH)-1]
    return ValueZero


#def calc_oz_series(symbol,numWeeksBack=20):
#    PeriodMA = 10
#    wDate, wOpen, wHigh, wLow, wClose, wVolume = CandleHelper.get_weekly_DOHLCV(numWeeksBack, symbol)
#    RelL = wLow - wOpen
#    RelH = wHigh - wOpen
#    MaH = TA_Lib.movingaverage(wDate,RelH, PeriodMA)
#    MaL = TA_Lib.movingaverage(wDate,RelL, PeriodMA)

    return MaH, MaL, wOpen, wDate
    
    
def getWeeklyDF(numWeeksBack,symbol):
    if symbol in Oanda.OandaSymbols:
        weekly_DF = Oanda.get_weekly_DOHLCV_pandas(numWeeksBack, symbol)  
    else:
        weekly_DF = CandleHelper.get_weekly_DOHLCV_pandas(numWeeksBack, symbol)
    return weekly_DF

def getIntradDayDF(numWeeksBack,symbol, timeFrame):
    if symbol in Oanda.OandaSymbols:
        return Oanda.get_intraDay_DOHLCV_pandas(numWeeksBack, symbol)
    else:
        if timeFrame== '1d':
            return CandleHelper.get_daily_DOHLCV_pandas(symbol, numWeeksBack)

        return  CandleHelper.get_intraDay_DOHLCV_pandas(symbol,numWeeksBack)

def calc_oz_series_pandas(symbol, numWeeksBack=20, averageTf='W'):
    timeFrameMap={'W':(1*numWeeksBack),
                  '3M':(numWeeksBack*15),
                  'Q':(numWeeksBack*15),
                  'M':(numWeeksBack*4)}
    print(Constants.StockHDF)
    store = pd.HDFStore(Constants.StockHDF)
    symbolKey = symbol + '_'+ averageTf

    today = datetime.datetime.now()  # - datetime.timedelta(days=1)
    day_of_week = today.weekday()
    weekStart = today - datetime.timedelta(days=day_of_week + 1)

    if not (symbolKey in store.keys()):
        print('Symbol:'+symbol)
        weekly_DF = getWeeklyDF(timeFrameMap[averageTf], symbol)
        #print(weekly_DF)
        newDF=calc_OZ_pandas(weekly_DF,averageTf=averageTf)
        store[symbolKey] = newDF
        store.flush()
        #print('READ')


    lenStore = len(store[symbolKey]) - 1
    if not (store[symbolKey].index[lenStore].date() == weekStart.date()):
        weekly_DF = getWeeklyDF(timeFrameMap[averageTf], symbol)
        newDF=calc_OZ_pandas(weekly_DF,averageTf=averageTf)
        store[symbolKey] = newDF
        store.flush()

    return store[symbolKey]


def calc_OZ_pandas(DF, averageTf='W'):
    print('test')
    print(averageTf)
    newDF=Oanda.resample_DOHLCV_pandas(DF,averageTf)
    #print(newDF['High'][0])
    newDF['DHi'] = newDF['High'] - newDF['Open']
    #print(newDF['DHi'])
    newDF['DLo'] = newDF['Low'] - newDF['Open']
    newDF['MLo'] = newDF['Open'] + newDF['DLo'].rolling(window=10).mean().shift(1)
    newDF['MHi'] = newDF['Open'] + newDF['DHi'].rolling(window=10).mean().shift(1)
    return newDF


def generate_overlay_oz_pandas(interval, symbol,numWeeksBack=20, averageTf='W'):
    OZW=calc_oz_series_pandas(symbol, numWeeksBack=20, averageTf=averageTf)
    OZW=OZW.shift(1, freq=averageTf).resample(CandleHelper.intervalMapPandas[interval]).bfill()
    #print(OZW)
    intra_DF = getIntradDayDF(numWeeksBack, symbol, interval)
    if interval == '1d' :
        intra_DF.reset_index(inplace=True)
        del intra_DF['index']
        intra_DF['Time'], intra_DF['Date'] = intra_DF['Date'].apply(lambda x: x.time()), intra_DF['Date'].apply(lambda x: x.date())
        intra_DF.set_index('Date', inplace=True)

    OZW.rename(columns={'Open':'wOpen'},inplace=True)
    intra_DF = pd.concat([intra_DF, OZW['MLo'], OZW['MHi'],OZW['wOpen']],axis=1,join_axes=[intra_DF.index])
    #print(intra_DF)
    return intra_DF

def generate_overlays_oz_pandas(interval, symbol,numWeeksBack=20, averageTf='W'):
    pandasInterval=CandleHelper.intervalMapPandas[interval]
    OZW=calc_oz_series_pandas(symbol, numWeeksBack=numWeeksBack, averageTf=averageTf)
    OZW=OZW.shift(1, freq=averageTf).resample(pandasInterval).bfill()
    #print(OZW)
    intra_DF = getIntradDayDF(numWeeksBack, symbol, interval)
    if interval == '1d' :
        intra_DF.reset_index(inplace=True)
        del intra_DF['index']
        intra_DF['Time'], intra_DF['Date'] = intra_DF['Date'].apply(lambda x: x.time()), intra_DF['Date'].apply(lambda x: x.date())
        intra_DF.set_index('Date', inplace=True)

    DFw= calc_oz_series_pandas(symbol, numWeeksBack=numWeeksBack, averageTf='W')
    DFw.rename(columns={'Open': 'wOpen', 'MLo': 'wLo','MHi':'wHi'}, inplace=True)
    DFw= DFw.shift(1, freq='W').resample(pandasInterval).bfill()
    DFm = calc_oz_series_pandas(symbol, numWeeksBack=numWeeksBack, averageTf='M')
    DFm = DFm.shift(1, freq='M').resample(pandasInterval).bfill()
    DFm.rename(columns={'Open': 'mOpen', 'MLo': 'mLo', 'MHi': 'mHi'}, inplace=True)
    DFq = calc_oz_series_pandas(symbol, numWeeksBack=numWeeksBack, averageTf='Q')
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

