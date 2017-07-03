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
        DF.to_hdf('/home/lc1bfrbl/Oanda.hdf',symbol+'_OZ')
        #Converted=convertToList(DF,symbol)
        #print(Converted[0:3])
        #table_object = client.table(write_table).new(Converted)
        #print(table_object.rows)
        #result = table_object.store()
        newAllDF=newAllDF.append(DF)
        print('After referesh_SymbolFrame: '+symbol)
    #newAllDF.reset_index(inplace=True)
    #newAllDF.rename(columns={'index':'DateTime'},inplace=True)
    return newAllDF

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

# Function to convert Python date to Unix Epoch
def convert_to_epoch ( date_to_convert ):
    return calendar.timegm(date_to_convert.timetuple()) * 1000

# Function to convert TsObject to list of lists
def ts_obj_to_list_of_lists (ts_obj):
    list_to_return = []
    for row in ts_obj.rows:
        list = []
        for i in range(len(row)):
            list.append(row[i])
        list_to_return.append(list)
    return list_to_return

def query_from_to(symbol, start_date, end_date):
    query = """\
        SELECT *
        FROM {}
        WHERE Symbol = '{}' AND Date >= {} AND Date < {} order by Date desc
    """.format(write_table,
               symbol,
               convert_to_epoch( start_date ),
               convert_to_epoch( end_date ) )

    print(query)
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



