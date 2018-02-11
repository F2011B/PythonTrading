# copied from
# https://www.quantopian.com/posts/technical-analysis-indicators-without-talib-code

import pandas as pd


# Rate of change
def roc_pandas(df, period=3):
    roc = (df['close'] - df['close'].shift(period)) / df['close'].shift(period)
    return roc


def tr_pandas(df, periods=14):
    dataframe = pd.DataFrame()
    dataframe['TR1'] = abs(dataframe['High'] - dataframe['Low'])
    dataframe['TR2'] = abs(dataframe['High'] - dataframe['Close'].shift())
    dataframe['TR3'] = abs(dataframe['Low'] - dataframe['Close'].shift())
    dataframe[['TR1', 'TR2', 'TR3']].max(axis=1)
    return dataframe


# Moving Average
def ma(df, n):
    ma = pd.Series(pd.rolling_mean(df['Close'], n), name='MA_' + str(n))
    df = df.join(ma)
    return df


# Exponential Moving Average
def ema(df, n):
    ema = pd.Series(pd.ewma(df['Close'], span=n, min_periods=n - 1), name='EMA_' + str(n))
    df = df.join(ema)
    return df


# Momentum
def mom(df, n):
    m = pd.Series(df['Close'].diff(n), name='Momentum_' + str(n))
    df = df.join(m)
    return df


# Rate of Change
def roc(df, n):
    m = df['Close'].diff(n - 1)
    n = df['Close'].shift(n - 1)
    roc = pd.Series(m / n, name='ROC_' + str(n))
    df = df.join(roc)
    return df


# Average True Range
def atr(df, n):
    i = 0
    tr_l = [0]
    while i < df.index[-1]:
        TR = max(df.get_value(i + 1, 'High'), df.get_value(i, 'Close')) - min(df.get_value(i + 1, 'Low'),
                                                                              df.get_value(i, 'Close'))
        tr_l.append(TR)
        i = i + 1
    tr_s = pd.Series(tr_l)
    atr = pd.Series(pd.ewma(tr_s, span=n, min_periods=n), name='ATR_' + str(n))
    df = df.join(atr)
    return df


# Bollinger Bands
def bbands(df, n):
    ma = pd.Series(pd.rolling_mean(df['Close'], n))
    msd = pd.Series(pd.rolling_std(df['Close'], n))
    b1 = 4 * msd / ma
    B1 = pd.Series(b1, name='BollingerB_' + str(n))
    df = df.join(B1)
    b2 = (df['Close'] - ma + 2 * msd) / (4 * msd)
    B2 = pd.Series(b2, name='Bollinger%b_' + str(n))
    df = df.join(B2)
    return df


# Pivot Points, Supports and Resistances
def ppsr(df):
    pp = pd.Series((df['High'] + df['Low'] + df['Close']) / 3)
    r1 = pd.Series(2 * pp - df['Low'])
    s1 = pd.Series(2 * pp - df['High'])
    r2 = pd.Series(pp + df['High'] - df['Low'])
    s2 = pd.Series(pp - df['High'] + df['Low'])
    r3 = pd.Series(df['High'] + 2 * (pp - df['Low']))
    s3 = pd.Series(df['Low'] - 2 * (df['High'] - pp))
    psr = {'PP': pp, 'R1': r1, 'S1': s1, 'R2': r2, 'S2': s2, 'R3': r3, 'S3': s3}
    psr_df = pd.DataFrame(psr)
    df = df.join(psr_df)
    return df


# Stochastic oscillator %K
def stok(df):
    sok = pd.Series((df['Close'] - df['Low']) / (df['High'] - df['Low']), name='SO%k')
    df = df.join(sok)
    return df


# Stochastic oscillator %D
def sto(df, n):
    sok = pd.Series((df['Close'] - df['Low']) / (df['High'] - df['Low']), name='SO%k')
    sod = pd.Series(pd.ewma(sok, span=n, min_periods=n - 1), name='SO%d_' + str(n))
    df = df.join(sod)
    return df


# Trix
def trix(df, n):
    ex1 = pd.ewma(df['Close'], span=n, min_periods=n - 1)
    ex2 = pd.ewma(ex1, span=n, min_periods=n - 1)
    ex3 = pd.ewma(ex2, span=n, min_periods=n - 1)
    i = 0
    roc_l = [0]
    while i + 1 <= df.index[-1]:
        roc = (ex3[i + 1] - ex3[i]) / ex3[i]
        roc_l.append(roc)
        i = i + 1
    trix = pd.Series(roc_l, name='Trix_' + str(n))
    df = df.join(trix)
    return df


# Average Directional Movement Index
def adx(df, n, n_adx):
    i = 0
    up_i = []
    do_i = []
    while i + 1 <= df.index[-1]:
        up_move = df.get_value(i + 1, 'High') - df.get_value(i, 'High')
        do_move = df.get_value(i, 'Low') - df.get_value(i + 1, 'Low')
        if up_move > do_move and up_move > 0:
            up_d = up_move
        else:
            up_d = 0
        up_i.append(up_d)
        if do_move > up_move and do_move > 0:
            do_d = do_move
        else:
            do_d = 0
        do_i.append(do_d)
        i = i + 1
    i = 0
    tr_l = [0]
    while i < df.index[-1]:
        tr = max(df.get_value(i + 1, 'High'), df.get_value(i, 'Close')) - min(df.get_value(i + 1, 'Low'),
                                                                              df.get_value(i, 'Close'))
        tr_l.append(tr)
        i = i + 1
    tr_s = pd.Series(tr_l)
    atr = pd.Series(pd.ewma(tr_s, span=n, min_periods=n))
    up_i = pd.Series(up_i)
    do_i = pd.Series(do_i)
    pos_di = pd.Series(pd.ewma(up_i, span=n, min_periods=n - 1) / atr)
    neg_di = pd.Series(pd.ewma(do_i, span=n, min_periods=n - 1) / atr)
    adx_df = pd.Series(pd.ewma(abs(pos_di - neg_di) / (pos_di + neg_di), span=n_adx, min_periods=n_adx - 1),
                    name='ADX_' + str(n) + '_' + str(n_adx))
    df = df.join(adx_df)
    return df


# MACD, MACD Signal and MACD difference
def macd(df, n_fast, n_slow):
    ema_fast = pd.Series(pd.ewma(df['Close'], span=n_fast, min_periods=n_slow - 1))
    ema_slow = pd.Series(pd.ewma(df['Close'], span=n_slow, min_periods=n_slow - 1))
    macd = pd.Series(ema_fast - ema_slow, name='MACD_' + str(n_fast) + '_' + str(n_slow))
    macd_sign = pd.Series(pd.ewma(macd, span=9, min_periods=8), name='MACDsign_' + str(n_fast) + '_' + str(n_slow))
    macd_diff = pd.Series(macd - macd_sign, name='MACDdiff_' + str(n_fast) + '_' + str(n_slow))
    df = df.join(macd)
    df = df.join(macd_sign)
    df = df.join(macd_diff)
    return df


# Mass Index
def mass_i(df):
    range = df['High'] - df['Low']
    ex1 = pd.ewma(range, span=9, min_periods=8)
    ex2 = pd.ewma(ex1, span=9, min_periods=8)
    mass = ex1 / ex2
    mass_i = pd.Series(pd.rolling_sum(mass, 25), name='Mass Index')
    df = df.join(mass_i)
    return df


# Vortex Indicator: http://www.vortexindicator.com/VFX_VORTEX.PDF
def vortex(df, n):
    i = 0
    tr = [0]
    while i < df.index[-1]:
        Range = max(df.get_value(i + 1, 'High'), df.get_value(i, 'Close')) - min(df.get_value(i + 1, 'Low'),
                                                                                 df.get_value(i, 'Close'))
        tr.append(Range)
        i = i + 1
    i = 0
    vm = [0]
    while i < df.index[-1]:
        Range = abs(df.get_value(i + 1, 'High') - df.get_value(i, 'Low')) - abs(
            df.get_value(i + 1, 'Low') - df.get_value(i, 'High'))
        vm.append(Range)
        i = i + 1
    vi = pd.Series(pd.rolling_sum(pd.Series(vm), n) / pd.rolling_sum(pd.Series(tr), n), name='Vortex_' + str(n))
    df = df.join(vi)
    return df


# KST Oscillator
def kst(df, r1, r2, r3, r4, n1, n2, n3, n4):
    m = df['Close'].diff(r1 - 1)
    n = df['Close'].shift(r1 - 1)
    roc1 = m / n
    m = df['Close'].diff(r2 - 1)
    n = df['Close'].shift(r2 - 1)
    roc2 = m / n
    m = df['Close'].diff(r3 - 1)
    n = df['Close'].shift(r3 - 1)
    roc3 = m / n
    m = df['Close'].diff(r4 - 1)
    n = df['Close'].shift(r4 - 1)
    roc4 = m / n
    kst = pd.Series(
        pd.rolling_sum(roc1, n1) + pd.rolling_sum(roc2, n2) * 2 + pd.rolling_sum(roc3, n3) * 3 + pd.rolling_sum(roc4,
                                                                                                                  n4) * 4,
        name='KST_' + str(r1) + '_' + str(r2) + '_' + str(r3) + '_' + str(r4) + '_' + str(n1) + '_' + str(
            n2) + '_' + str(n3) + '_' + str(n4))
    df = df.join(kst)
    return df


# Relative Strength Index
def rsi(df, n):
    i = 0
    up_i = [0]
    do_i = [0]
    while i + 1 <= df.index[-1]:
        up_move = df.get_value(i + 1, 'High') - df.get_value(i, 'High')
        do_move = df.get_value(i, 'Low') - df.get_value(i + 1, 'Low')
        if up_move > do_move and up_move > 0:
            up_d = up_move
        else:
            up_d = 0
        up_i.append(up_d)
        if do_move > up_move and do_move > 0:
            do_d = do_move
        else:
            do_d = 0
        do_i.append(do_d)
        i = i + 1
    up_i = pd.Series(up_i)
    do_i = pd.Series(do_i)
    pos_d_i = pd.Series(pd.ewma(up_i, span=n, min_periods=n - 1))
    neg_d_i = pd.Series(pd.ewma(do_i, span=n, min_periods=n - 1))
    r_s_i = pd.Series(pos_d_i / (pos_d_i + neg_d_i), name='RSI_' + str(n))
    df = df.join(r_s_i)
    return df


# True Strength Index
def tsi(df, r, s):
    m = pd.Series(df['Close'].diff(1))
    a_m = abs(m)
    ema1 = pd.Series(pd.ewma(m, span=r, min_periods=r - 1))
    a_ema1 = pd.Series(pd.ewma(a_m, span=r, min_periods=r - 1))
    ema2 = pd.Series(pd.ewma(ema1, span=s, min_periods=s - 1))
    a_ema2 = pd.Series(pd.ewma(a_ema1, span=s, min_periods=s - 1))
    tsi = pd.Series(ema2 / a_ema2, name='TSI_' + str(r) + '_' + str(s))
    df = df.join(tsi)
    return df


# Accumulation/Distribution
def accdist(df, n):
    ad = (2 * df['Close'] - df['High'] - df['Low']) / (df['High'] - df['Low']) * df['Volume']
    m = ad.diff(n - 1)
    n = ad.shift(n - 1)
    roc = m / n
    ad = pd.Series(roc, name='Acc/Dist_ROC_' + str(n))
    df = df.join(ad)
    return df


# Chaikin Oscillator
def chaikin(df):
    ad = (2 * df['Close'] - df['High'] - df['Low']) / (df['High'] - df['Low']) * df['Volume']
    chaikin = pd.Series(pd.ewma(ad, span=3, min_periods=2) - pd.ewma(ad, span=10, min_periods=9), name='Chaikin')
    df = df.join(chaikin)
    return df


# Money Flow Index and Ratio
def mfi(df, n):
    p_p = (df['High'] + df['Low'] + df['Close']) / 3
    i = 0
    pos_mf = [0]
    while i < df.index[-1]:
        if p_p[i + 1] > p_p[i]:
            pos_mf.append(p_p[i + 1] * df.get_value(i + 1, 'Volume'))
        else:
            pos_mf.append(0)
        i = i + 1
    pos_mf = pd.Series(pos_mf)
    tot_mf = p_p * df['Volume']
    mfr = pd.Series(pos_mf / tot_mf)
    mfi = pd.Series(pd.rolling_mean(mfr, n), name='MFI_' + str(n))
    df = df.join(mfi)
    return df


# On-balance Volume
def obv(df, n):
    i = 0
    obv = [0]
    while i < df.index[-1]:
        if df.get_value(i + 1, 'Close') - df.get_value(i, 'Close') > 0:
            obv.append(df.get_value(i + 1, 'Volume'))
        if df.get_value(i + 1, 'Close') - df.get_value(i, 'Close') == 0:
            obv.append(0)
        if df.get_value(i + 1, 'Close') - df.get_value(i, 'Close') < 0:
            obv.append(-df.get_value(i + 1, 'Volume'))
        i = i + 1
    obv = pd.Series(obv)
    obv_ma = pd.Series(pd.rolling_mean(obv, n), name='OBV_' + str(n))
    df = df.join(obv_ma)
    return df


# Force Index
def force(df, n):
    f = pd.Series(df['Close'].diff(n) * df['Volume'].diff(n), name='Force_' + str(n))
    df = df.join(f)
    return df


# Ease of Movement
def eom(df, n):
    eo_m = (df['High'].diff(1) + df['Low'].diff(1)) * (df['High'] - df['Low']) / (2 * df['Volume'])
    eom_ma = pd.Series(pd.rolling_mean(eo_m, n), name='EoM_' + str(n))
    df = df.join(eom_ma)
    return df


# Commodity Channel Index
def cci(df, n):
    pp = (df['High'] + df['Low'] + df['Close']) / 3
    cci = pd.Series((pp - pd.rolling_mean(pp, n)) / pd.rolling_std(pp, n), name='CCI_' + str(n))
    df = df.join(cci)
    return df


# Coppock Curve
def copp(df, n):
    m = df['Close'].diff(int(n * 11 / 10) - 1)
    n = df['Close'].shift(int(n * 11 / 10) - 1)
    roc1 = m / n
    m = df['Close'].diff(int(n * 14 / 10) - 1)
    n = df['Close'].shift(int(n * 14 / 10) - 1)
    roc2 = m / n
    copp = pd.Series(pd.ewma(roc1 + roc2, span=n, min_periods=n), name='Copp_' + str(n))
    df = df.join(copp)
    return df


# Keltner Channel
def kelch(df, n):
    kel_ch_m = pd.Series(pd.rolling_mean((df['High'] + df['Low'] + df['Close']) / 3, n), name='KelChM_' + str(n))
    kel_ch_u = pd.Series(pd.rolling_mean((4 * df['High'] - 2 * df['Low'] + df['Close']) / 3, n), name='KelChU_' + str(n))
    kel_ch_d = pd.Series(pd.rolling_mean((-2 * df['High'] + 4 * df['Low'] + df['Close']) / 3, n), name='KelChD_' + str(n))
    df = df.join(kel_ch_m)
    df = df.join(kel_ch_u)
    df = df.join(kel_ch_d)
    return df


# Ultimate Oscillator
def ultosc(df):
    i = 0
    tr_l = [0]
    bp_l = [0]
    while i < df.index[-1]:
        tr = max(df.get_value(i + 1, 'High'), df.get_value(i, 'Close')) - min(df.get_value(i + 1, 'Low'),
                                                                              df.get_value(i, 'Close'))
        tr_l.append(tr)
        bp = df.get_value(i + 1, 'Close') - min(df.get_value(i + 1, 'Low'), df.get_value(i, 'Close'))
        bp_l.append(bp)
        i = i + 1
    ult_o = pd.Series((4 * pd.rolling_sum(pd.Series(bp_l), 7) / pd.rolling_sum(pd.Series(tr_l), 7)) + (
            2 * pd.rolling_sum(pd.Series(bp_l), 14) / pd.rolling_sum(pd.Series(tr_l), 14)) + (
                             pd.rolling_sum(pd.Series(bp_l), 28) / pd.rolling_sum(pd.Series(tr_l), 28)),
                     name='Ultimate_Osc')
    df = df.join(ult_o)
    return df


# Donchian Channel
def donch(df, n):
    i = 0
    dcl = []
    while i < n - 1:
        dcl.append(0)
        i = i + 1
    i = 0
    while i + n - 1 < df.index[-1]:
        DC = max(df['High'].ix[i:i + n - 1]) - min(df['Low'].ix[i:i + n - 1])
        dcl.append(DC)
        i = i + 1
    don_ch = pd.Series(dcl, name='Donchian_' + str(n))
    don_ch = don_ch.shift(n - 1)
    df = df.join(don_ch)
    return df


# Standard Deviation
def stddev(df, n):
    df = df.join(pd.Series(pd.rolling_std(df['Close'], n), name='STD_' + str(n)))
    return df


def livermoore(df, ATRRange=14):
    new_df = atr(df, ATRRange)
    Key = [0, 1]
    upsw = 0
    new_df['LiverMoore_AtrThree'] = new_df['ATR_14'] * 3
    hh = new_df['High'].values[0]
    ll = new_df['Low'].values[0]
    new_df['LiverMoore_Key'] = 0

    for index, row in new_df.iterrows():
        if upsw == 1:
            hh = max(row['High'], hh)
            Trigger = hh - row['LiverMoore_AtrThree']
            if row['Close'] < Trigger:
                upsw = 0
                ll = row['Low']
        else:
            ll = min(row['Low'], ll)
            Trigger = ll + row['LiverMoore_AtrThree']
            if row['Close'] > Trigger:
                upsw = 1
                hh = row['High']
        new_df.loc[index, 'LiverMoore_Key'] = upsw

    del new_df['LiverMoore_AtrThree']
    return new_df
