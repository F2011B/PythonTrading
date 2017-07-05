#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 20:42:33 2016
"""
import datetime
import pandas as pd
import oandapy.oandapy as oandapy
import Constants as Constants
import os
import sys
import inspect

import unittest
from enum import Enum

#import RestSettings

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
insertModule('Passwords')
import Passwords
import Constants
#import Constants.Constants as Constants


OANDA= oandapy.API(environment="live", access_token=Passwords.accessTokenOanda)

accounts=OANDA.get_accounts()
accounts['accounts'][0]['accountId']


M1='M1'
M2='M2'
M3='M3'
M4='M5'
M15='M15'
M30='M30'
H1='H1'
D='D'
W='W'

granularities=['M1','M2','M3','M4','M15','M30','H1','D','W']

OandaSymbols=['AU200_AUD','AUD_CAD','AUD_CHF','AUD_HKD','AUD_JPY','AUD_NZD','AUD_SGD','AUD_USD','BCO_USD',
'CAD_CHF','CAD_HKD','CAD_JPY','CAD_SGD','CH20_CHF','CHF_HKD','CHF_JPY','CHF_ZAR','CN50_USD','CORN_USD',
'DE10YB_EUR','DE30_EUR','EU50_EUR','EUR_AUD','EUR_CAD','EUR_CHF','EUR_CZK','EUR_DKK','EUR_GBP','EUR_HKD',
'EUR_HUF','EUR_JPY','EUR_NOK','EUR_NZD','EUR_PLN','EUR_SEK','EUR_SGD','EUR_TRY','EUR_USD','EUR_ZAR',
'FR40_EUR','GBP_AUD','GBP_CAD','GBP_CHF','GBP_HKD','GBP_JPY','GBP_NZD','GBP_PLN','GBP_SGD','GBP_USD',
'GBP_ZAR','HK33_HKD','HKD_JPY','IN50_USD','JP225_USD','NAS100_USD','NATGAS_USD','NL25_EUR','NZD_CAD',
'NZD_CHF','NZD_HKD','NZD_JPY','NZD_SGD','NZD_USD','SG30_SGD','SGD_CHF','SGD_HKD','SGD_JPY','SOYBN_USD',
'SPX500_USD','SUGAR_USD','TRY_JPY','TWIX_USD','UK100_GBP','UK10YB_GBP','US2000_USD','US30_USD','USB02Y_USD',
'USB05Y_USD','USB10Y_USD','USB30Y_USD','USD_CAD','USD_CHF','USD_CNH','USD_CZK','USD_DKK','USD_HKD','USD_HUF',
'USD_INR','USD_JPY','USD_MXN','USD_NOK','USD_PLN','USD_SAR','USD_SEK','USD_SGD','USD_THB','USD_TRY','USD_ZAR',
'WHEAT_USD','WTICO_USD','XAG_AUD','XAG_CAD','XAG_CHF','XAG_EUR','XAG_GBP','XAG_HKD','XAG_JPY','XAG_NZD',
'XAG_SGD','XAG_USD','XAU_AUD','XAU_CAD','XAU_CHF','XAU_EUR','XAU_GBP','XAU_HKD','XAU_JPY','XAU_NZD',
'XAU_SGD','XAU_USD','XAU_XAG','XCU_USD','XPD_USD','XPT_USD','ZAR_JPY' ]


updateFrequency=datetime.timedelta(minutes = 1)

def convertDateTimeStr(input):
    splitted=input.split('T',)
    Date=splitted[0].split('-')
    Time=splitted[1].split(':')
    Hour=Time[0]
    Minute=Time[1]
    Second=Time[2].split('.')[0]
    return datetime.datetime(year=int(Date[0]), 
                      month=int(Date[1]), 
                      day=int(Date[2]), 
                      hour=int(Hour), 
                      minute=int(Minute), 
                      second=int(Second))
    
    
def getDataFromStartDate(startDate,instrument='WTICO_USD',gran='M15',dayDiff=5):
    condition=True
    StartDateString=str(startDate.year)+'-'+ '{:02d}'.format(startDate.month)+'-'+'{:02d}'.format(startDate.day)
    Today=datetime.datetime.now()
    DFList=list()
    while condition:
        #print(StartDateString)
        try:
            DF=pd.DataFrame(OANDA.get_history(instrument=instrument,granularity=gran,start=StartDateString,count=5000)['candles'])
            if not 'time' in DF.keys():
                break
            DF['DateTime'] = DF['time'].map(convertDateTimeStr)

            lastDate = startDate
            startDate = DF['DateTime'][len(DF['DateTime']) - 1].to_datetime()
            Diff = Today - startDate
            #print(Diff)
            # print(startDate)
            newDay = "%02d" % (startDate.day)
            newHour = "%02d" % (startDate.hour,)
            newMin = "%02d" % (startDate.minute + 15,)
            newS = "%02d" % (startDate.second,)
            StartDateString = str(startDate.year) + '-' + str(
                startDate.month) + '-' + newDay + 'T' + newHour + ':' + newMin + ':' + newS

            DF.set_index('DateTime', inplace=True)
            DFList.append(DF)
            condition = not (Diff.days <= dayDiff)

            Result = DFList[0]
            for i in range(1, len(DFList)):
                Result = Result.append(DFList[i])
        except:
            print(StartDateString)
            print(DFList)
            condition=False
            return None
    return Result


        


def calc_start_date(Today, daysBack):
    if daysBack < 1:
        daysBack = 1
    startDay = Today.day - daysBack
    startMonth = Today.month
    startYear = Today.year
    if startDay < 0:
        startDay = startDay + 31
        startMonth = Today.month - 1
    if startMonth < 0:
        startMonth = 12 + startMonth
        startYear = startYear - 1
        
    return datetime.datetime(startYear, startMonth, startDay, 0,0,0)   
    
def ReadOrUpdataDB(symbol,startDate, endDate, gran) :
    StoreFilePath=Constants.DatabaseOanda
    store=pd.HDFStore(StoreFilePath)
    resample=False
    granularity=gran
    if (gran=='W') or (gran=='D'):
        resample=True
        granularity='H1'


    symbolKey = '/' + symbol + '_' + granularity
    #DFHDF=pd.read_hdf('/home/lc1bfrbl/Oanda.hdf',symbolKey  )

    if not (symbolKey in store.keys()):
        store.close()
        # print(startDate)
        # print(symbol)
        # print(granularity)
        Result = getDataFromStartDate(startDate,symbol,granularity)
        Result['Close'] = Result['closeAsk']
        Result['High'] = Result['highAsk']
        Result['Low'] = Result['lowAsk']
        Result['Open'] = Result['openAsk']
        Result['Volume'] = Result['volume']
        Result['LastUpdated']=datetime.datetime.now()

        Result.to_hdf(StoreFilePath,symbolKey)
        print('Will write Oanda.hdf : '+symbolKey)
        return Result

    NeedUpdate=True
    if ('LastUpdated' in store[symbolKey].keys()):
        TimeCheck=(store[symbolKey]['LastUpdated'].iloc[-1]+updateFrequency).time() <=endDate.time()
        print((store[symbolKey].index.max()+updateFrequency).time())
        print(endDate.time())
        print((store[symbolKey]['LastUpdated'].iloc[-1]+updateFrequency).time())
        DateCheck=store[symbolKey].index.max().date()<=endDate.date()
        NeedUpdate=DateCheck and TimeCheck

    print(NeedUpdate)
    if NeedUpdate:
        DiffDays=0
        ResultDF = getDataFromStartDate(startDate,symbol,granularity,DiffDays)
        if ResultDF is not None :
            store[symbolKey]=store[symbolKey].combine_first(ResultDF)
            #store.flush()

    Result=store[symbolKey]
    store.close()
    Result['Close']=Result['closeAsk']
    Result['High'] = Result['highAsk']
    Result['Low'] = Result['lowAsk']
    Result['Open'] = Result['openAsk']
    Result['Volume'] = Result['volume']
    Result['LastUpdated']=datetime.datetime.now()
    Result.to_hdf(StoreFilePath,symbolKey)
    print(Result.keys())
    print('Will write Oanda.hdf : ' + symbolKey)
    if resample:
        return resample_DOHLCV_pandas(Result, gran)

    return Result
        
            
    #os.path.isfile(path)
        
    
def get_ohlc_days_back(symbol,daysBack,granularity='1d'):    
    endDate=datetime.datetime.now()    
    startDate=endDate- datetime.timedelta(days=daysBack)
    ohlcv= ReadOrUpdataDB(symbol,startDate,endDate,granularity)     
    return ohlcv


def get_intraday_pandas_dback(symbol, daysBack, granularity ):
    endDate=datetime.datetime.now()
    startDate=endDate- datetime.timedelta(days=daysBack)
    ohlcv= ReadOrUpdataDB(symbol,startDate,endDate,granularity)
    return ohlcv

def get_weekly_DOHLCV_pandas(numWeeksBack,symbol):
    endDate=datetime.datetime.now()    
    startDate=endDate-datetime.timedelta(weeks=numWeeksBack)    
    ohlcv = ReadOrUpdataDB(symbol,startDate, endDate,'W')
    return ohlcv

def get_intraDay_DOHLCV_pandas(numWeeksBack,symbol,timeFrame='H1'):
    endDate=datetime.datetime.now()
    startDate=endDate-datetime.timedelta(weeks=numWeeksBack)
    ohlcv = ReadOrUpdataDB(symbol,startDate, endDate,timeFrame)
    ohlcv['High']=ohlcv['highAsk']
    ohlcv['Close']=ohlcv['closeAsk']
    ohlcv['Open']=ohlcv['openAsk']
    ohlcv['Low']=ohlcv['lowAsk']
    return ohlcv

def get_availableSymbols(SymbolFilter=None):
    DF=pd.read_hdf(Constants.InputFolder+'Symbols.hdf', 'OANDA')
    return DF.instrument.values


def resample_DOHLCV_pandas(DF,targetTF):
    ohlc_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        #'closeAsk': 'last',
        #'closeBid': 'last',
        #'Volume': 'sum'
    }
    ResampledData = DF[['Open', 'High','Low','Close']].resample(targetTF,closed='left',label='left', how=ohlc_dict )#.apply(ohlc_dict)
    return ResampledData
    
#endDate=datetime.datetime.now()    
#startDate = endDate- datetime.timedelta(days=1000)
#DF=ReadOrUpdataDB('WTICO_USD',startDate,endDate,'W')


class MyTestCase(unittest.TestCase):
    def test_calc_multiple(self):
        endDate = datetime.datetime.now()
        print(endDate)
        startDate = endDate - datetime.timedelta(weeks=200)
        print(startDate)
        #DF=getDataFromStartDate(startDate,instrument='CORN_USD',gran='H1')
        print('test')

        #DF= getDataFromStartDate(startDate, 'WTICO_USD', gran='H1')
        DF = ReadOrUpdataDB('WTICO_USD',startDate, endDate, 'H1')#('1h', 'WTICO_USD', numWeeksBack=200).dropna()
       # DF=resample_DOHLCV_pandas(DF, 'W')
        print(DF)
        #print(DF['LastUpdated'].iloc[-1])


    def test_get_weekly_DOHLCV_pandas(self):
        DF=get_weekly_DOHLCV_pandas(20,'WTICO_USD')
        print(DF)

    def test_Oanda(self):
        endDate = datetime.datetime.now()
        startDate = endDate - datetime.timedelta(weeks=200)
        print(startDate)
        StartDateString = str(startDate.year) + '-' + str(startDate.month) + '-' + str(startDate.day)
        print(StartDateString)
        StartDateString='2013-9-2'
        print(StartDateString)
        print(pd.DataFrame(OANDA.get_history(instrument='WTICO_USD',start=StartDateString, granularity='H1', count=5000)['candles']))
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    