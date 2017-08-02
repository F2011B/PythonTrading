import datetime
import pandas as pd
import logging

Logger=None

def setLogger(logger):
    Logger=logger


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

    DF['MO'] = DF['Open']
    DF['DHi'] = DF['High'] - DF['Open']
    # print(newDF['DHi'])
    DF['DLo'] = DF['Low'] - DF['Open']
    DF['MLo'] = DF['Open'] + DF['DLo'].rolling(window=10).mean().shift(1)
    DF['MHi'] = DF['Open'] + DF['DHi'].rolling(window=10).mean().shift(1)
    return DF

def resampleToHourlyFrame(DF,TargetDF,TaylorDF):
    DF.reset_index()
    testDF = DF.reset_index()
    testDF = testDF.append(testDF.tail(1))
    testDF = testDF.reset_index()
    testDF.set_value(len(testDF) - 1, 'DateTime',
                     testDF.iloc[len(testDF) - 1].DateTime + testDF.DateTime.diff().iloc[-3])
    testDF[['MO', 'MLo', 'MHi']] = testDF[['MO', 'MLo', 'MHi']].shift(1)
    testDF.set_index('DateTime', inplace=True)
    resampled = testDF.resample('60min').bfill()
    ResultDF=pd.concat([TargetDF, resampled[['MO', 'MLo', 'MHi']]], axis=1, join_axes=[TargetDF.index])
    TaylorDF['Date']=pd.to_datetime(TaylorDF.index.date)
    ResultDF['Date']=pd.to_datetime(ResultDF.index.date)
    TaylorDF = TaylorDF.reset_index().set_index('Date')
    ResultDF = ResultDF.reset_index().set_index('Date')
    ResultDF['TaylorDay']=TaylorDF['DayNum']
    ResultDF = ResultDF.reset_index().set_index('DateTime')
    return ResultDF

def CalcTaylorCycle(DF):
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

    TaylorDF = ResampledData.dropna()
    TaylorDF['Day'] = 1
    TaylorDF['DayNum'] = TaylorDF.Day.cumsum().mod(3)

    newDF = count3DayGroups(TaylorDF)
    newDF=UsingDayGroupAsProxyDate(newDF)
    newDF=AddAverages(newDF)

    return resampleToHourlyFrame(newDF,DF,TaylorDF)




def main():
    DF = pd.read_hdf('/home/lc1bfrbl/Database/Oanda.hdf', 'WTICO_USD_H1')
    TTT=CalcTaylorCycle(DF)
    Index = (TTT.index.year == 2017) & (TTT.index.month == 6)
    TTT[Index].MO.plot()
    TTT[Index].MLo.plot()
    TTT[Index].MHi.plot()
    TTT[Index].High.plot()
    TTT[Index].Low.plot()


if __name__ == "__main__":
    main()