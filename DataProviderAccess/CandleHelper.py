import IQData
import numpy as np
import pandas as panda
import datetime

intervalMap={"3h": 10800, "1h": 3600, "1m": 60, "5m": 300, "15m": 900, "30m": 1800}

intervalMapPandas={"3h": '180min', '1h': '60min', '1m': '1min', '5m': '5min', '30m': '30min', '1d': '1d', '15m': '15min'}


def get_ohlcv(periods, symbol):
    ohlcv = IQData.get_daily_history(symbol, periods)
    close, high, low, open_price, volume = convert_to_array_ohlcv(ohlcv)
    return open_price, high, low, close, volume


def convert_to_array_ohlcv_rev(ohlcv):
    high = ohlcv['High']
    low = ohlcv['Low']
    close = ohlcv['Close']
    volume = ohlcv['Volume']
    open_price = ohlcv['Open']
    return close, high, low, open_price, volume


def convert_to_array_ohlcv(ohlcv):
    high = ohlcv['High'][::-1]
    low = ohlcv['Low'][::-1]
    close = ohlcv['Close'][::-1]
    volume = ohlcv['Volume'][::-1]
    open_price = ohlcv['Open'][::-1]
    return close, high, low, open_price, volume


def get_dohlcv_desc(periods, symbol):
    ohlcv = IQData.get_ohlc_days_back(symbol, periods)
    date, open_price, high, low, close, volume = convert_to_array_DOHLCV(ohlcv)
    return date, open_price, high, low, close, volume


def get_dohlcv(Periods, symbol):
    ohlcv = IQData.get_daily_history(symbol, Periods)
    date, open_price, high, low, close, volume = convert_to_array_DOHLCV(ohlcv)
    return date, open_price, high, low, close, volume


def get_DOHLCV_rev(Periods,symbol):
    ohlcv = IQData.get_daily_history(symbol, Periods)
    date, open_price, high, low, close, volume = convert_to_array_DOHLCV_rev(ohlcv)
    return date, open_price, high, low, close, volume


def convert_to_array_DOHLCV_rev(ohlcv):
    Close, High, Low, Open, Volume = convert_to_array_ohlcv_rev(ohlcv)
    Date = ohlcv['Date']
    return  Date, Open, High, Low, Close, Volume


def convert_to_array_DOHLCV(ohlcv):
    Close, High, Low, Open, Volume = convert_to_array_ohlcv(ohlcv)
    Date = ohlcv['Date'][::-1]
    return Date, Open, High, Low, Close, Volume


def get_weekly_dohlcv(Periods, symbol):
    daily_periods=Periods*5+10
    Date, Open, High, Low, Close, Volume=get_dohlcv(daily_periods, symbol)
    wDate, wOpenArr, wHighArr, wLowArr, wCloseArr, wVolumeArr=convert_to_weekly(Close, Date, High, Low, Open, Volume)
    return wDate, wOpenArr, wHighArr, wLowArr, wCloseArr, wVolumeArr


def ohlcsum(df):
    df = df.sort()
    return {
       'Open': df['Open'][0],
       'High': df['High'].max(),
       'Low': df['Low'].min(),
       'Close': df['Close'][-1],
       'Volume': df['Volume'].sum()
      }


def get_weekly_DOHLCV_pandas(Periods,symbol):
    ohlc_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'OI': 'sum'
    }
    DF=IQData.get_intraday_pandas_dback(symbol, 3600, Periods*5)
    ResampledData = DF.resample('W',closed='left',label='left', how=ohlc_dict )#.apply(ohlc_dict)
    return ResampledData


def resample_DOHLCV_pandas(DF,targetTF):
    ohlc_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'OI': 'sum'
    }
    ResampledData = DF.resample(targetTF,closed='left',label='left', how=ohlc_dict )#.apply(ohlc_dict)
    return ResampledData

def get_intraDay_DOHLCV_pandas(symbol,numWeeks=20,timeFrame=3600):
    DF = IQData.get_intraday_pandas_wback(symbol, timeFrame, numWeeks)
    return DF

def get_daily_DOHLCV_pandas(symbol,numWeeks=20):
    return IQData.get_daily_history_pandas(symbol, numWeeks*5)


def convert_to_weekly(Close, Date, High, Low, Open, Volume):
    wOpen = list()
    wHigh = list()
    wClose = list()
    wLow = list()
    wVolume = list()
    wDate=list()
    for i in range(len(Date)):
        if i == 0:
            continue
        if Date[i].day - Date[i - 1].day >= 2:
            LC = len(wClose)
            if LC != 0:
                wClose[LC - 1] = Close[i - 1]
            wDate.append(Date[i])
            wOpen.append(Open[i])
            wLow.append(Low[i])
            wHigh.append(High[i])
            wClose.append(Close[i])
            wVolume.append(Volume[i])
        else:
            if len(wLow) == 0:
                continue
            else:
                Last = len(wLow) - 1
                if wLow[Last] > Low[i]: wLow[Last] = Low[i]
                if wHigh[Last] < High[i]: wHigh[Last] = High[i]
                wVolume[Last] = wVolume[Last] + Volume[i]
    wOpenArr = np.array(wOpen)
    wCloseArr = np.array(wClose)
    wLowArr = np.array(wLow)
    wHighArr = np.array(wHigh)
    wVolumeArr = np.array(wVolume)
    return wDate, wOpenArr, wHighArr, wLowArr, wCloseArr, wVolumeArr

def expand_weekly(wDate,wSeries,Date):
    if len(wDate) != len(wSeries) :
   #     Logger.log('wDate to short')
        return None


    #Logger.log('Before correct')

    nDate=ensure_correct_ascending_order(Date)
    nWDate=ensure_correct_ascending_order(wDate)
    nwSeries=ensure_correct_ascending_timeseries(wDate,wSeries)

    #nWDate.append(nWDate[len(nWDate)-1]+ datetime.timedelta(days=7))
    Series=list()
    NewDate=list()
#    Logger.log(str(len(Date)))
#    Logger.log('nWDate Length')
#    Logger.log(str(len(nWDate)))
#    Logger.log(str(len(nwSeries)))
#    Logger.log('nWDate Length Over')
    for counter in range(len(nWDate)-1):
        for i in range(len(nDate)):
            if (nDate[i] >= nWDate[counter]) and (nDate[i] < nWDate[counter+1]):
                Series.append(nwSeries[counter])
                NewDate.append(nDate[i])
    return Series,NewDate

def expand_weekly_pandas(wDate,wSeries,TargetTF):
    DF=panda.DataFrame({'Date':wDate,'Series':wSeries})
    DF = DF.set_index(['Date'])
    ResampledData=DF.resample(intervalMapPandas[TargetTF], fill_method='bfill')
    return ResampledData['Series'],ResampledData.index

def expand_weekly_new(wDate,wSeries,Date):
    if len(wDate) != len(wSeries) :
#        Logger.log('wDate to short')
        return None

    nDate=ensure_correct_ascending_order(Date)
    nWDate=ensure_correct_ascending_order(wDate)
    nwSeries=ensure_correct_ascending_timeseries(wDate,wSeries)
#    Logger.log('nWDate Length ')
#    Logger.log(str(len(nWDate)))
#    Logger.log('  ')
#    Logger.log(str(len(nwSeries)))
#    Logger.log('nWDate Length Over ')

    nWDate.append(nWDate[len(nWDate)-1]+ datetime.timedelta(days=7))
    Series=list()
    NewDate=list()
#    Logger.log(str(len(Date)))

    counter=0
    wCounter=0
    Stop=False
    while not Stop :
        while not Stop and (counter < len(nDate) ):
            if (nDate[counter].isocalendar()[1] == nWDate[wCounter].isocalendar()[1]) :
                Stop=True
            if not Stop :
                counter = counter + 1
        if not Stop :
            wCounter= wCounter +1



    lastWeek=nWDate[0].isocalendar()[1]

    wCount=0
    for i in range(counter,len(nDate) ):
        if nDate[i].isocalendar()[1] > lastWeek :
            lastWeek=nDate[i].isocalendar()[1]
            wCount=wCount+1
        Series.append(nwSeries[wCount])
        NewDate.append(nDate[i])

    return Series,NewDate

def ensure_correct_ascending_timeseries(inDate,Series):
    nSeries=list(Series)
    if inDate[0] > inDate[1]:
        nSeries=Series[::-1]
    return nSeries

def ensure_correct_ascending_order(input):
    nOutput = list(input)
    if input[0] > input[1]:
        nOutput = input[::-1]
    return nOutput




Black=0
Blue=16711680
Cyan=16776960
Green=65280
Magenta=16711935
Red=255
Yellow=65535
White=16777215
DarkBlue=8388608
DarkCyan=8421376
DarkGreen=32768
DarkMagenta=8388736
DarkRed=128
DarkBrown=32896
DarkGray=8421504
LightGray=12632256

def rgb_to_hex(rgb):
    return '#'+str(format(rgb, '04X'))








