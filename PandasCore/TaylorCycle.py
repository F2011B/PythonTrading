import datetime
import pandas as pd


def count3DayGroups(DF):
    DF['DayGroup'] = 0
    grouped = DF.groupby('DayNum')
    DFList = list()
    for i in range(3):
        Temp = grouped.get_group(i).reset_index()
        DFList.append(Temp)
        DFList[i]['DayGroup'] = 1
        DFList[i]['DayGroup'] = DFList[i]['DayGroup'].cumsum()
    # print(DFList[0]['DayGroup'])


    newDF = DFList[0][['DateTime', 'DayGroup']]
    for i in range(2):
        newDF = newDF.append(DFList[i + 1][['DateTime', 'DayGroup']], ignore_index=True)
    del (DF['DayGroup'])
    return pd.concat([DF, newDF.set_index('DateTime').sort_index()], axis=1, join_axes=[DF.index])


def UsingDayGroupAsProxyDate(DF):
    result = DF.reset_index().groupby('DayGroup').agg(
    {'DateTime':'first','Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'})
    return result.set_index('DateTime').sort_index()

def AddAverages(DF):

    DF['DHi'] = DF['High'] - DF['Open']
    # print(newDF['DHi'])
    DF['DLo'] = DF['Low'] - DF['Open']
    DF['MLo'] = DF['Open'] + DF['DLo'].rolling(window=10).mean().shift(1)
    DF['MHi'] = DF['Open'] + DF['DHi'].rolling(window=10).mean().shift(1)
    return DF


def TaylorCycle(DF):
    ohlc_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        # 'closeAsk': 'last',
        # 'closeBid': 'last',
        # 'Volume': 'sum'
    }
    ResampledData = DF[['Open', 'High', 'Low', 'Close']].resample('1D', closed='left', label='left', how=ohlc_dict)

    newDF = ResampledData.dropna()
    newDF['Day'] = 1
    newDF['DayNum'] = newDF.Day.cumsum().mod(3)

    newDF = count3DayGroups(newDF)
    newDF=UsingDayGroupAsProxyDate(newDF)
    newDF = newDF.shift(1, freq='3D').resample('60min').bfill()
    newDF = pd.concat([DF, newDF['MO'], newDF['MLo'], newDF['MHi']], axis=1, join_axes=[DF.index])

    return newDF