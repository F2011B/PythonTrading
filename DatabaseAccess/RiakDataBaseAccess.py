import pandas as pd
import os
import sys
import inspect
import calendar
import datetime
from riak import RiakClient

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

def insertModule(ModuleFolder):
    cmd_subfolder = os.path.realpath(
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],'..', ModuleFolder)))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)

insertModule('PandasCore')
insertModule('Constants')
insertModule('DataProviderAccess')
import OZ
import Constants
import Oanda
import IQData
import logging

write_table = "MyStockData"
client = RiakClient(nodes=[{'host':Constants.RiakServer,'http_port':Constants.RiakPort}],transport_options={'ts_convert_timestamp': True})
LastWeekDay={}
Refreshed={}

def getToday():
    return datetime.datetime.today()

def ShouldRefreshAll(Symbol):
    if not (Symbol in LastWeekDay ) :
        LastWeekDay[Symbol]=-1

    isRefreshday = (datetime.datetime.today().weekday() == Constants.OZRefreshDay)
    ChangedWeekDay = (datetime.datetime.today().weekday() != LastWeekDay[Symbol])
    CorrectTimeForRefreshAll = datetime.datetime.today().hour == 16
    CorrectRefreshTime = isRefreshday and CorrectTimeForRefreshAll

    if not ChangedWeekDay :
        return False
    if not CorrectRefreshTime :
        return False

    LastWeekDay[Symbol] = datetime.datetime.today().weekday()

    if not (Symbol in Refreshed):
        Refreshed[Symbol]=True
        return True

    return not(Refreshed[Symbol])


def FillUpToNow(Symbol,LastDF):
    MyDF=LastDF.reset_index()
    DayDiff=getToday().day - MyDF['Date'][0].day
    if DayDiff > 4 :
        NewDF = OZ.generate_overlays_oz_pandas('1h', Symbol, numWeeksBack=200).dropna()
    else :
        NewDF=Oanda.get_intraday_pandas_dback(Symbol, Oanda.H1, max(DayDiff,1) )   #get_intraday_pandas_dback(Symbol, 3600, max(DayDiff,1) )
        NewDF['Symbol']=Symbol
        NewDF['qOpen']=MyDF['qOpen'][0]
        NewDF['qLo']=MyDF['qLo'][0]
        NewDF['qHi']=MyDF['qHi'][0]
        NewDF['mOpen']=MyDF['mOpen'][0]
        NewDF['mLo']=MyDF['mLo'][0]
        NewDF['mHi']=MyDF['mHi'][0]
        NewDF['wOpen']=MyDF['wOpen'][0]
        NewDF['wLo']=MyDF['wLo'][0]
        NewDF['wHi']=MyDF['wHi'][0]
    return NewDF

def refresh_SymbolFrame(Symbol):
    DF=None
    if ShouldRefreshAll(Symbol) :
        DF = OZ.generate_overlays_oz_pandas('1h', Symbol, numWeeksBack=200).dropna()
        return DF

    #LastDF = getLastDateFrame(Symbol)
    LastDF=None
    if not (LastDF is not None):
        DF = OZ.generate_overlays_oz_pandas('1h', Symbol, numWeeksBack=200).dropna()
        return DF

    #DF=FillUpToNow(Symbol,LastDF)
    return DF

def updateOZRiak(symbolList):
    DFDict={}
    newAllDF=pd.DataFrame()
    for symbol in symbolList:
        print(symbol)
        DF=refresh_SymbolFrame(symbol)
        DF.to_hdf(Constants.DatabaseOanda,symbol+'_OZ')
        Converted=convertToList(DF,symbol)
        table_object = client.table(write_table).new(Converted)
        result = table_object.store()
        #newAllDF=newAllDF.append(DF)
        print('After referesh_SymbolFrame: '+symbol)
    #newAllDF.reset_index(inplace=True)
    #newAllDF.rename(columns={'index':'DateTime'},inplace=True)
    #return newAllDF



def convertToList(DF,SymbolName="HD"):
    newList=list()
    for i in range(len(DF.index.values)):
        getVal= lambda x: DF[x][i]
        #print(DF.index.to_datetime()[i])
        newList.append( [SymbolName, DF.index.to_pydatetime()[i],
                         getVal('Open'),getVal('High'),getVal('Low'),getVal('Close'),int(getVal('Volume')),
              getVal('qOpen'),getVal('qLo'),getVal('qHi'),
              getVal('mOpen'),getVal('mLo'),getVal('mHi'),
              getVal('wOpen'),getVal('wLo'),getVal('wHi') ])
    return newList

def convertDynamicToList(DF, Symbol='CORN_USD', table_name="OandaTTT"):
    elemList, NewDict = createMappingFromTable(table_name)
    newDF = DF
    newDF['Symbol'] = Symbol
    newList = list()

    for i in range(len(DF.index.values)):
        getVal = lambda x: newDF[x][i]
        newListElem = [NewDict[x](getVal(x)) for x in elemList]
        newList.append(newListElem)

    return newList



def writeDFToTable(DF,symbol,table_name,Logger):
    Logger.info('Before dropping Nans in DF')
    newDF=DF.dropna()
    Logger.info('Before converting DF to List')
    Converted = convertDynamicToList(newDF, symbol,table_name)
    Logger.info('After converting DF to List')
    table_object = client.table(table_name).new(Converted)
    result = table_object.store()
    if not result :
        try:
            Logger.warning('''{} was tried to store in {}'''.format(symbol,table_name))
        except:
            Logger.info('''{} was stored in {}'''.format(symbol, table_name))



def getNewestDateForSymbol(table=write_table,Symbol='CORN_USD_H1'):
    '''
    :param table: Which riak ts table should be accessed
    :param Symbol: which symbol is used to retrieve the latest date
    :return: latest date as epoch integer
    '''
    DF=pd.DataFrame()
    table=client.table(write_table)
    for key in table.stream_keys():
        DF=DF.append(pd.DataFrame(key,columns=['Date','Symbol']))
    DF['Symbol']=DF['Symbol'].str.decode('ASCII')
    return DF[DF['Symbol']==Symbol].Date.sort_values().tail(1).values[0].astype(datetime.datetime)

# Function to convert Python date to Unix Epoch
def convert_to_epoch ( date_to_convert ):
    return calendar.timegm(date_to_convert.timetuple()) * 1000

# Function to convert TsObject to list of lists
def ts_obj_to_list_of_lists(ts_obj):
    list_to_return = []
    for row in ts_obj.rows:
        list = []
        for i in range(len(row)):
            list.append(row[i])
        list_to_return.append(list)
    return list_to_return


def createMappingFromTable(table_name="OandaTTT"):
    table = client.table(table_name)
    Desc = table.describe()
    DFRows = pd.DataFrame(Desc.rows)
    typeMap = {'boolean': bool, 'double': float, 'timestamp': pd.to_datetime, 'sint64': int, 'varchar': str}
    elemList = [x for x in DFRows[0].str.decode('ASCII')]
    mappingDict = {i[0].decode('ASCII'): typeMap[i[1].decode('ASCII')] for i in DFRows.values}
    return elemList, mappingDict





def convert_to_epoch(date_to_convert):
    return calendar.timegm(date_to_convert.timetuple()) * 1000


def query_table_from_to(symbol, table_name, start_date, end_date):
    query = """\
            SELECT *
            FROM {}
            WHERE Symbol = '{}' AND Date >= {} AND Date < {} order by Date desc
        """.format(table_name, symbol, convert_to_epoch(start_date),
                   convert_to_epoch(end_date))
    try:
        elemList, mappingDict = createMappingFromTable(table_name)
        data_set = client.ts_query(table_name, query)
        boring_list = ts_obj_to_list_of_lists(data_set)
        DF = pd.DataFrame(boring_list)
        DF.columns = elemList
        DF['Symbol'] = symbol
        return DF
    except:
        return None

def query_from_to(symbol, start_date, end_date):
    query = """\
        SELECT *
        FROM {}
        WHERE Symbol = '{}' AND Date >= {} AND Date < {} order by Date desc
    """.format(write_table,
               symbol,
               convert_to_epoch( start_date ),
               convert_to_epoch( end_date ) )


    data_set = client.ts_query(write_table, query)
    boring_list=ts_obj_to_list_of_lists(data_set)
    if len(boring_list) == 0 :
        return None

    DF = pd.DataFrame(boring_list)

    # Set the column names and the index to the Date field
    DF.columns = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume',
                  'qOpen','qLo','qHi','mOpen','mLo','mHi','wOpen','wLo','wHi']
    DF['Symbol']=symbol
    return DF

def getLastDateFrame(symbol):
    WrongValue=None
    today = datetime.datetime.now()  # - datetime.timedelta(days=1)
    Start = today - datetime.timedelta(days=5)
    DF=query_from_to(symbol, Start, today )
    if not(DF is not None) :
        return WrongValue
    if len(DF['Date']) ==0 :
        return WrongValue
    return DF.tail(1)



