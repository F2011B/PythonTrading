import pandas as pd
import os
import sys
import inspect
import calendar
import datetime
import OZ
from riak import RiakClient

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)


def insert_module(ModuleFolder):
    cmd_subfolder = os.path.realpath(
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0], '..', ModuleFolder)))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)


insert_module('PandasCore')
insert_module('Constants')
insert_module('DataProviderAccess')
import Constants
import Oanda

write_table = "MyStockData"
client = RiakClient(nodes=[{'host': Constants.RiakServer, 'http_port': Constants.RiakPort}],
                    transport_options={'ts_convert_timestamp': True})
last_week_day = {}
refreshed = {}


def getToday():
    return datetime.datetime.today()


def ShouldRefreshAll(symbol):
    if not (symbol in last_week_day):
        last_week_day[symbol] = -1

    is_refreshday = (datetime.datetime.today().weekday() == Constants.OZRefreshDay)
    changed_week_day = (datetime.datetime.today().weekday() != last_week_day[symbol])
    correct_time_for_refresh_all = datetime.datetime.today().hour == 16
    correct_refresh_time = is_refreshday and correct_time_for_refresh_all

    if not changed_week_day:
        return False
    if not correct_refresh_time:
        return False

    last_week_day[symbol] = datetime.datetime.today().weekday()

    if not (symbol in refreshed):
        refreshed[symbol] = True
        return True

    return not (refreshed[symbol])


def fill_up_to_now(symbol, last_df):
    my_df = last_df.reset_index()
    day_diff = getToday().day - my_df['Date'][0].day
    if day_diff > 4:
        new_df = OZ.generate_overlays_oz_pandas('1h', symbol, numWeeksBack=200).dropna()
    else:
        new_df = Oanda.get_intraday_pandas_dback(symbol, Oanda.H1, max(day_diff,
                                                                       1))  # get_intraday_pandas_dback(Symbol, 3600, max(DayDiff,1) )
        new_df['Symbol'] = symbol
        new_df['qOpen'] = my_df['qOpen'][0]
        new_df['qLo'] = my_df['qLo'][0]
        new_df['qHi'] = my_df['qHi'][0]
        new_df['mOpen'] = my_df['mOpen'][0]
        new_df['mLo'] = my_df['mLo'][0]
        new_df['mHi'] = my_df['mHi'][0]
        new_df['wOpen'] = my_df['wOpen'][0]
        new_df['wLo'] = my_df['wLo'][0]
        new_df['wHi'] = my_df['wHi'][0]
    return new_df


def refresh_symbol_frame(symbol):
    df = None
    if ShouldRefreshAll(symbol):
        df = OZ.generate_overlays_oz_pandas('1h', symbol, numWeeksBack=200).dropna()
        return df

    # LastDF = getLastDateFrame(Symbol)
    last_df = None
    if not (last_df is not None):
        df = OZ.generate_overlays_oz_pandas('1h', symbol, numWeeksBack=200).dropna()
        return df

    # DF=FillUpToNow(Symbol,LastDF)
    return df


def update_oz_riak(symbolList):
    for symbol in symbolList:
        print(symbol)
        df = refresh_symbol_frame(symbol)
        df.to_hdf(Constants.DatabaseOanda, symbol + '_OZ')
        converted = convertToList(df, symbol)
        table_object = client.table(write_table).new(converted)
        # newAllDF=newAllDF.append(DF)
        print('After referesh_SymbolFrame: ' + symbol)
    # newAllDF.reset_index(inplace=True)
    # newAllDF.rename(columns={'index':'DateTime'},inplace=True)
    # return newAllDF


def convertToList(df, symbol_name="HD"):
    new_list = list()
    for i in range(len(df.index.values)):
        getVal = lambda x: df[x][i]
        # print(DF.index.to_datetime()[i])
        new_list.append([symbol_name, df.index.to_pydatetime()[i],
                         getVal('Open'), getVal('High'), getVal('Low'), getVal('Close'), int(getVal('Volume')),
                         getVal('qOpen'), getVal('qLo'), getVal('qHi'),
                         getVal('mOpen'), getVal('mLo'), getVal('mHi'),
                         getVal('wOpen'), getVal('wLo'), getVal('wHi')])
    return new_list


def convertDynamicToList(df, symbol='CORN_USD', table_name="OandaTTT"):
    elem_list, new_dict = create_mapping_from_table(table_name)
    new_df = df
    new_df['Symbol'] = symbol
    new_list = list()

    for i in range(len(df.index.values)):
        get_val = lambda x: new_df[x][i]
        new_list_elem = [new_dict[x](get_val(x)) for x in elem_list]
        new_list.append(new_list_elem)

    return new_list


def writeDFToTable(df, symbol, table_name, logger):
    logger.info('Before dropping Nans in DF')
    newDF = df.dropna()
    logger.info('Before converting DF to List')
    converted = convertDynamicToList(newDF, symbol, table_name)
    logger.info('After converting DF to List')
    table_object = client.table(table_name).new(converted)
    result = table_object.store()
    if not result:
        try:
            logger.warning('''{} was tried to store in {}'''.format(symbol, table_name))
        except:
            logger.info('''{} was stored in {}'''.format(symbol, table_name))


def getNewestDateForSymbol(table=write_table, symbol='CORN_USD_H1'):
    '''
    :param table: Which riak ts table should be accessed
    :param symbol: which symbol is used to retrieve the latest date
    :return: latest date as epoch integer
    '''
    df = pd.DataFrame()
    table = client.table(table)
    for key in table.stream_keys():
        df = df.append(pd.DataFrame(key, columns=['Date', 'Symbol']))
    df['Symbol'] = df['Symbol'].str.decode('ASCII')
    return df[df['Symbol'] == symbol].Date.sort_values().tail(1).values[0].astype(datetime.datetime)


def getAvailSymbols(table=write_table):
    '''
    :param table: Which riak ts table should be accessed
    :return: all available symbols as list
    '''
    df = pd.DataFrame()
    table = client.table(table)
    for key in table.stream_keys():
        df = df.append(pd.DataFrame(key, columns=['Date', 'Symbol']))
    symbols = df['Symbol'].drop_duplicates()
    return symbols.values


# Function to convert Python date to Unix Epoch
def convert_to_epoch(date_to_convert):
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


def create_mapping_from_table(table_name="OandaTTT"):
    table = client.table(table_name)
    desc = table.describe()
    df_rows = pd.DataFrame(desc.rows)
    type_map = {'boolean': bool, 'double': float, 'timestamp': pd.to_datetime, 'sint64': int, 'varchar': str}
    elem_list = [x for x in d_f_rows[0].str.decode('ASCII')]
    mapping_dict = {i[0].decode('ASCII'): type_map[i[1].decode('ASCII')] for i in d_f_rows.values}
    return elem_list, mapping_dict


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
        elemList, mappingDict = create_mapping_from_table(table_name)
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
               convert_to_epoch(start_date),
               convert_to_epoch(end_date))

    data_set = client.ts_query(write_table, query)
    boring_list = ts_obj_to_list_of_lists(data_set)
    if len(boring_list) == 0:
        return None

    df = pd.DataFrame(boring_list)

    # Set the column names and the index to the Date field
    df.columns = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume',
                  'qOpen', 'qLo', 'qHi', 'mOpen', 'mLo', 'mHi', 'wOpen', 'wLo', 'wHi']
    df['Symbol'] = symbol
    return df


def get_last_date_frame(symbol):
    wrong_value = None
    today = datetime.datetime.now()  # - datetime.timedelta(days=1)
    start = today - datetime.timedelta(days=5)
    df = query_from_to(symbol, start, today)
    if not (df is not None):
        return wrong_value
    if len(df['Date']) == 0:
        return wrong_value
    return df.tail(1)
