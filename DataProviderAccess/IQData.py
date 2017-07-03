
import socket
from io import StringIO
import datetime
import numpy as np
import io
import math
import pandas as pd
import Constants as Constants

TCP_IP=''
#TCP_IP='127.0.0.1'
TCP_PORT_History=9101
BUFFER_SIZE = 4096




intervalMap={ "3h":10800,"1h": 3600, "1m":60,"5m":300,"15m":900,"30m":1800}

def timeConverter(Raw):
    encoding='utf-8'
    TimeDate=datetime.datetime.strptime(Raw.decode(encoding), "%Y-%m-%d  %H:%M:%S")
 #   print(TimeDate)
    #newFormat=TimeDate.year*10000+TimeDate.month*100+TimeDate.day+TimeDate.hour/100+TimeDate.minute/10000+TimeDate.second/1000000
#    print(newFormat)
    return TimeDate

def readIQCSV_pandas(fileName,Symbol):
    fileName.seek(0)
    data=readIQCSV(fileName,Symbol)
    #return pd.read_csv(fileName,header=None,parse_dates=['Date'],date_parser=timeConverter, names=['Date','High','Low','Open','Close','Volume','OI','Unamed'],)
    return pd.DataFrame(data)


def readIQCSV(fileName,Symbol):
    encoding='utf-8'
    TempData=None
    DataFormat =[('Date','object'),('High','d'),('Low','d'),('Open','d'),('Close','d'),('OI','d'),('Volume','d')]
    try :
        TempData=np.genfromtxt(io.BytesIO(fileName.getvalue().encode() ),dtype=DataFormat,invalid_raise=False, delimiter=',',converters={0:timeConverter})
    except:
        print('There was an error reading np.genfromtxt in: ' +Symbol)
    return TempData

def getDataFromIQFeed(Message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((TCP_IP, TCP_PORT_History))
    s.send(Message.encode('ascii'))

    data = ''
    outputbuffer = StringIO()
    condition = True    
    while condition:
        data = s.recv(BUFFER_SIZE).decode("ascii")
        if data.find('!ENDMSG!') != -1:
            data = data.replace('!ENDMSG!', '')
            condition = False
            #          print('End found')

        outputbuffer.write(data)
    s.close()
    return outputbuffer

def get_history_IQ(MESSAGE, symbol):
    ohlc = readIQCSV(getDataFromIQFeed(MESSAGE), symbol)
    return ohlc

def get_history_IQ_pandas(MESSAGE, symbol):
    data = readIQCSV_pandas(getDataFromIQFeed(MESSAGE), symbol)
    data= data.set_index(pd.DatetimeIndex(data['Date']))
    return data


def get_intraday_IQ(symbol,interval=3600):
    Today=datetime.datetime.now()
    endDate=Today.year*10000+Today.month*100+Today.day+1

    daysBack=math.ceil(60*interval/3600)
    startDate = calc_start_date(Today, daysBack)

    MESSAGE = "HIT," + symbol + "," +str(interval)+','+str(startDate)+','+ str(endDate) + "\r\n"
    ohlcv = get_history_IQ(MESSAGE, symbol)
    return ohlcv


def get_intraday_IQ_pandas(symbol,interval=3600):
    daysBack=math.ceil(60*interval/3600)
    return get_intraday_pandas_dback(symbol, interval, daysBack)

def formatDate(date):
    return date.year * 10000 + date.month * 100 + date.day

def get_intraday_pandas_dback(symbol, interval=3600, daysBack=10):
    Today = datetime.datetime.now()

    endDateStr = formatDate(Today+datetime.timedelta(days=1))
    startDate = Today - datetime.timedelta(days=daysBack)
    startDateStr = formatDate(startDate)
    return get_intraday_pandas(symbol, interval, startDateStr, endDateStr)

def get_intraday_pandas_wback(symbol, interval, weeksBack=20):
    Today = datetime.datetime.now()
    FormatDate= lambda x: x.year*10000+x.month*100+x.day
    endDate = Today.year * 10000 + Today.month * 100 + Today.day + 1
    endDate=FormatDate(Today)+1
    startDate = Today-datetime.timedelta(weeks=weeksBack)
    return get_intraday_pandas(symbol, interval, FormatDate(startDate), endDate)


def get_intraday_pandas(symbol, interval, startDate, endDate):
    MESSAGE = "HIT," + symbol + "," + str(interval) + ',' + str(startDate) + ',' + str(endDate) + "\r\n"
    ohlcv = get_history_IQ_pandas(MESSAGE, symbol)
    return ohlcv


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
    startDate = startYear * 10000 + startMonth * 100 + startDay
    return startDate


def get_ohlc(symbol):
    Today=datetime.datetime.now()
    endDate=Today.year*10000+Today.month*100+Today.day+1
    startDate=calc_start_date(Today,5)

    MESSAGE = "HDT," + symbol + ","+str(startDate)+','+ str(endDate) + "\r\n"
    ohlcv = get_history_IQ(MESSAGE, symbol)
    data = convert_to_json_element(ohlcv)
    return data

def get_ohlc_days_back(symbol,days):
    Today=datetime.datetime.now()
    endDate=Today.year*10000+Today.month*100+Today.day+1
    startDate=calc_start_date(Today,days)

    MESSAGE = "HDT," + symbol + ","+str(startDate)+','+ str(endDate) + "\r\n"
    ohlcv = get_history_IQ(MESSAGE, symbol)
    return ohlcv

def get_ohlc_days_back_pandas(symbol,days):
    Today=datetime.datetime.now()
    endDate = convert_date(Today)
    startDate=calc_start_date(Today,days)
    return get_daily_ohclv_start_end(symbol, startDate, endDate )


def convert_date(Today):
    endDate = Today.year * 10000 + Today.month * 100 + Today.day + 1
    return endDate


def get_daily_ohclv_start_end(symbol, startDate, endDate ):
    MESSAGE = "HDT," + symbol + "," + str(startDate) + ',' + str(endDate) + "\r\n"
    data = get_history_IQ_pandas(MESSAGE, symbol)
    print(data)
    #data = data.set_index(['Date'])
    return data


def convert_to_json_element(ohlcv):
    data = list()
    open = {'Open': float(ohlcv['Open'][0])}
    high = {'High': float(ohlcv['High'][0])}
    low = {'Low': float(ohlcv['Low'][0])}
    close = {'Close': float(ohlcv['Close'][0])}
    volume = {'Volume': float(ohlcv['Volume'][0])}
    data.append(open)
    data.append(high)
    data.append(low)
    data.append(close)
    data.append(volume)
    return data

def get_daily_history(symbol,num_days):
    MESSAGE = "HDX," + symbol + "," + str(num_days) + "\r\n"
    return get_history_IQ(MESSAGE, symbol)

def get_daily_history_pandas(symbol,num_days):
    MESSAGE = "HDX," + symbol + "," + str(num_days) + "\r\n"
    #return get_ohlc_days_back_pandas(symbol,num_days)
    return get_history_IQ_pandas(MESSAGE, symbol)

def get_availableExchanges():
    SymbolsDF = pd.read_hdf(Constants.InputFolder + 'Symbols.hdf', 'Symbols')
    return SymbolsDF.EXCHANGE.drop_duplicates().values

def get_availableSymbols(SymbolFilter=None):
    SymbolsDF = pd.read_hdf(Constants.InputFolder+'Symbols.hdf', 'Symbols')

    if SymbolFilter == None :
        DFNew = SymbolsDF.loc[lambda DF: DF.EXCHANGE == 'NYSE', :]
        return DFNew.loc[DFNew.SYMBOL.str.match('[A-Z]{1,4}$'), :].SYMBOL.values

    if not ('Exchange' in SymbolFilter.keys()):
        DFNew = SymbolsDF.loc[lambda DF: DF.EXCHANGE == 'NYSE', :]
        return DFNew.loc[DFNew.SYMBOL.str.match('[A-Z]{1,4}$'), :].SYMBOL.values

    DFNew = SymbolsDF.loc[lambda DF: DF.EXCHANGE == SymbolFilter['Exchange'], :]
    return DFNew.loc[DFNew.SYMBOL.str.match('[A-Z]{1,4}$'), :].SYMBOL.values



