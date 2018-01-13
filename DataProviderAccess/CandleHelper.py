import IQData
import numpy as np
import pandas as panda
import datetime

intervalMap = {"3h": 10800, "1h": 3600, "1m": 60, "5m": 300, "15m": 900, "30m": 1800}

intervalMapPandas = {"3h": '180min', '1h': '60min', '1m': '1min',
                     '5m': '5min', '30m': '30min', '1d': '1d', '15m': '15min'}


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
    date, open_price, high, low, close, volume = convert_to_array_dohlcv(ohlcv)
    return date, open_price, high, low, close, volume


def get_dohlcv(periods, symbol):
    ohlcv = IQData.get_daily_history(symbol, periods)
    date, open_price, high, low, close, volume = convert_to_array_dohlcv(ohlcv)
    return date, open_price, high, low, close, volume


def get_dohlcv_rev(periods, symbol):
    ohlcv = IQData.get_daily_history(symbol, periods)
    date, open_price, high, low, close, volume = convert_to_array_dohlcv_rev(ohlcv)
    return date, open_price, high, low, close, volume


def convert_to_array_dohlcv_rev(ohlcv):
    close, high, low, open_price, volume = convert_to_array_ohlcv_rev(ohlcv)
    date = ohlcv['Date']
    return date, open_price, high, low, close, volume


def convert_to_array_dohlcv(ohlcv):
    close, high, low, open_price, volume = convert_to_array_ohlcv(ohlcv)
    date = ohlcv['Date'][::-1]
    return date, open_price, high, low, close, volume


def get_weekly_dohlcv(periods, symbol):
    daily_periods = periods * 5 + 10
    date, open_price, high, low, close, volume = get_dohlcv(daily_periods, symbol)
    w_date, w_open_arr, w_high_arr, w_low_arr, w_close_arr, w_volume_arr = \
        convert_to_weekly(close, date, high, low, open_price, volume)
    return w_date, w_open_arr, w_high_arr, w_low_arr, w_close_arr, w_volume_arr


def ohlcsum(df):
    df = df.sort()
    return {
       'Open': df['Open'][0],
       'High': df['High'].max(),
       'Low': df['Low'].min(),
       'Close': df['Close'][-1],
       'Volume': df['Volume'].sum()
      }


def get_weekly_dohlcv_pandas(periods, symbol):
    ohlc_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'OI': 'sum'
    }
    df = IQData.get_intraday_pandas_dback(symbol, 3600, periods * 5)
    resampled_data = df.resample('W', closed='left', label='left', how=ohlc_dict)
    return resampled_data


def resample_dohlcv_pandas(df, target_tf):
    ohlc_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'OI': 'sum'
    }
    resampled_data = df.resample(target_tf, closed='left', label='left', how=ohlc_dict)
    return resampled_data


def get_intra_day_dohlcv_pandas(symbol, num_weeks=20, time_frame=3600):
    df = IQData.get_intraday_pandas_wback(symbol, time_frame, num_weeks)
    return df


def get_daily_dohlcv_pandas(symbol, num_weeks=20):
    return IQData.get_daily_history_pandas(symbol, num_weeks * 5)


def convert_to_weekly(close, date, high, low, open_price, volume):
    w_open = list()
    w_high = list()
    w_close = list()
    w_low = list()
    w_volume = list()
    w_date = list()
    for i in range(len(date)):
        if i == 0:
            continue
        if date[i].day - date[i - 1].day >= 2:
            lc = len(w_close)
            if lc != 0:
                w_close[lc - 1] = close[i - 1]
            w_date.append(date[i])
            w_open.append(open_price[i])
            w_low.append(low[i])
            w_high.append(high[i])
            w_close.append(close[i])
            w_volume.append(volume[i])
        else:
            if len(w_low) == 0:
                continue
            else:
                last = len(w_low) - 1
                if w_low[last] > low[i]:
                    w_low[last] = low[i]
                if w_high[last] < high[i]:
                    w_high[last] = high[i]
                w_volume[last] = w_volume[last] + volume[i]
    w_open_arr = np.array(w_open)
    w_close_arr = np.array(w_close)
    w_low_arr = np.array(w_low)
    w_high_arr = np.array(w_high)
    w_volume_arr = np.array(w_volume)
    return w_date, w_open_arr, w_high_arr, w_low_arr, w_close_arr, w_volume_arr


def expand_weekly(w_date, w_series, date):
    if len(w_date) != len(w_series):
        return None

    n_date = ensure_correct_ascending_order(date)
    n_w_date = ensure_correct_ascending_order(w_date)
    nw_series = ensure_correct_ascending_timeseries(w_date, w_series)

    series = list()
    new_date = list()

    for counter in range(len(n_w_date)-1):
        for i in range(len(n_date)):
            if (n_date[i] >= n_w_date[counter]) and (n_date[i] < n_w_date[counter+1]):
                series.append(nw_series[counter])
                new_date.append(n_date[i])
    return series, new_date


def expand_weekly_pandas(w_date, w_series, target_tf):
    df = panda.DataFrame({'Date': w_date, 'Series': w_series})
    df = df.set_index(['Date'])
    resampled_data = df.resample(intervalMapPandas[target_tf], fill_method='bfill')
    return resampled_data['Series'], resampled_data.index


def expand_weekly_new(w_date, w_series, date):
    if len(w_date) != len(w_series):

        return None

    n_date = ensure_correct_ascending_order(date)
    nw_date = ensure_correct_ascending_order(w_date)
    nw_series = ensure_correct_ascending_timeseries(w_date, w_series)

    nw_date.append(nw_date[len(nw_date)-1] + datetime.timedelta(days=7))
    series = list()
    new_date = list()

    counter = 0
    w_counter = 0
    stop = False
    while not stop:
        while not stop and (counter < len(n_date)):
            if n_date[counter].isocalendar()[1] == nw_date[w_counter].isocalendar()[1]:
                stop = True
            if not stop:
                counter = counter + 1
        if not stop:
            w_counter = w_counter + 1

    last_week = nw_date[0].isocalendar()[1]

    w_count = 0
    for i in range(counter, len(n_date)):
        if n_date[i].isocalendar()[1] > last_week:
            last_week = n_date[i].isocalendar()[1]
            w_count = w_count+1
        series.append(nw_series[w_count])
        new_date.append(n_date[i])

    return series, new_date


def ensure_correct_ascending_timeseries(in_date, series):
    n_series = list(series)
    if in_date[0] > in_date[1]:
        n_series = series[::-1]
    return n_series


def ensure_correct_ascending_order(input_data):
    n_output = list(input_data)
    if input_data[0] > input_data[1]:
        n_output = input_data[::-1]
    return n_output


Black = 0
Blue = 16711680
Cyan = 16776960
Green = 65280
Magenta = 16711935
Red = 255
Yellow = 65535
White = 16777215
DarkBlue = 8388608
DarkCyan = 8421376
DarkGreen = 32768
DarkMagenta = 8388736
DarkRed = 128
DarkBrown = 32896
DarkGray = 8421504
LightGray = 12632256


def rgb_to_hex(rgb):
    return '#'+str(format(rgb, '04X'))
