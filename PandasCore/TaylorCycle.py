import datetime
import pandas as pd
import logging

Logger = None


def setLogger(logger):
    Logger = logger


def count3_day_groups(df):
    df['DayGroup'] = 0
    grouped = df.groupby('DayNum')
    df_list = list()
    for i in range(3):
        temp = grouped.get_group(i).reset_index()
        df_list.append(temp)
        df_list[i]['DayGroup'] = 1
        df_list[i]['DayGroup'] = df_list[i]['DayGroup'].cumsum()
    # print(df_list[0]['DayGroup'])

    new_df = df_list[0][['DateTime', 'DayGroup']]
    for i in range(2):
        new_df = new_df.append(df_list[i + 1][['DateTime', 'DayGroup']], ignore_index=True)
    del (df['DayGroup'])
    return pd.concat([df, new_df.set_index('DateTime').sort_index()], axis=1, join_axes=[df.index])


def UsingDayGroupAsProxyDate(df):
    result = df.reset_index().groupby('DayGroup').agg(
        {'DateTime': 'first', 'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'})
    return result.set_index('DateTime').sort_index()


def add_averages(df):
    df['MO'] = df['Open']
    df['DHi'] = df['High'] - df['Open']
    # print(newDF['DHi'])
    df['DLo'] = df['Low'] - df['Open']
    df['MLo'] = df['Open'] + df['DLo'].rolling(window=10).mean().shift(1)
    df['MHi'] = df['Open'] + df['DHi'].rolling(window=10).mean().shift(1)
    return df


def resample_to_hourly_frame(df, target_df, taylor_df):
    df.reset_index()
    test_df = df.reset_index()
    test_df = test_df.append(test_df.tail(1))
    test_df = test_df.reset_index()
    test_df.set_value(len(test_df) - 1, 'DateTime',
                     test_df.iloc[len(test_df) - 1].DateTime + test_df.DateTime.diff().iloc[-3])
    test_df[['MO', 'MLo', 'MHi']] = test_df[['MO', 'MLo', 'MHi']].shift(1)
    test_df.set_index('DateTime', inplace=True)
    resampled = test_df.resample('60min').bfill()
    result_df = pd.concat([target_df, resampled[['MO', 'MLo', 'MHi']]], axis=1, join_axes=[target_df.index])
    taylor_df['Date'] = pd.to_datetime(taylor_df.index.date)
    result_df['Date'] = pd.to_datetime(result_df.index.date)
    taylor_df = taylor_df.reset_index().set_index('Date')
    result_df = result_df.reset_index().set_index('Date')
    result_df['TaylorDay'] = taylor_df['DayNum']
    result_df = result_df.reset_index().set_index('DateTime')
    return result_df


def calc_taylor_cycle(DF):
    ohlc_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        # 'closeAsk': 'last',
        # 'closeBid': 'last',
        # 'Volume': 'sum'
    }
    resampled_data = DF[['Open', 'High', 'Low', 'Close']].resample('1D', closed='left', label='left', how=ohlc_dict)

    taylor_df = resampled_data.dropna()
    taylor_df['Day'] = 1
    taylor_df['DayNum'] = taylor_df.Day.cumsum().mod(3)

    new_df = count3_day_groups(taylor_df)
    new_df = UsingDayGroupAsProxyDate(new_df)
    new_df = add_averages(new_df)

    return resample_to_hourly_frame(new_df, DF, taylor_df)


def main():
    df = pd.read_hdf('/home/lc1bfrbl/Database/Oanda.hdf', 'WTICO_USD_H1')
    ttt = calc_taylor_cycle(df)
    index = (ttt.index.year == 2017) & (ttt.index.month == 6)
    ttt[index].MO.plot()
    ttt[index].MLo.plot()
    ttt[index].MHi.plot()
    ttt[index].High.plot()
    ttt[index].Low.plot()


if __name__ == "__main__":
    main()
