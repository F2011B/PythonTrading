#!/usr/bin/env python

import pandas as pd
import asyncio
import unittest
import datetime
import time
import sys
import os
import inspect

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

# use this if you want to include modules from a subfolder
cmd_subfolder = os.path.realpath(
    os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0], "Modules")))
print(cmd_subfolder)
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import OZ

q = asyncio.Queue()
add_event = asyncio.Event()
kill_event = asyncio.Event()
delete_event = asyncio.Event()
send_event = asyncio.Event()
refresh_event = asyncio.Event()
refreshed_event = asyncio.Event()


class SimpleEchoProtocol(asyncio.Protocol):
    const_timeout = 5.0

    def connection_made(self, transport):
        print("Connection received!")
        self.transport = transport

    def timeout(self):
        print('connection timeout, closing.')
        self.transport.close()

    def connection_made(self, transport):
        print('connection made')
        self.transport = transport

    def data_received(self, data):

        print('data received: ', data.decode())
        message = data.decode()
        print(message)
        add_list = list()
        for element in message.split('_End'):
            if 'Add' in element:
                add_list.append(element.split('_')[1])
                print(add_list)

            if len(add_list) > 0:
                add_event.data = add_list
                add_event.set()

            if 'Send' in element:
                print('Send was sent')
                send_event.set()
                fut = asyncio.async(q.get())
                fut.add_done_callback(self.write_reply)

    def write_reply(self, fut):
        reply = fut.result().to_json()
        print('Send: {!r}'.format(reply))
        self.transport.write(reply.encode())
        self.transport.write('!ENDMSG!'.encode())

    def eof_received(self):
        pass

    def connection_lost(self, exc):
        print('connection lost:', exc)


@asyncio.coroutine
def calc_refresh(loop):
    while True:
        if datetime.datetime.today().weekday() == 5:
            refresh_event.set()


def refresh_symbol_frame(symbol):
    dfw = OZ.calc_oz_series_pandas(symbol, numWeeksBack=20, averageTf='W')
    dfw.rename(columns={'Open': 'wOpen', 'MLo': 'wLo', 'MHi': 'wHi'}, inplace=True)
    dfm = OZ.calc_oz_series_pandas(symbol, numWeeksBack=20, averageTf='M')
    dfm = dfm.shift(1, freq='M').resample('W').bfill()
    dfm.rename(columns={'Open': 'mOpen', 'MLo': 'mLo', 'MHi': 'mHi'}, inplace=True)
    dfq = OZ.calc_oz_series_pandas(symbol, numWeeksBack=20, averageTf='Q')
    dfq.rename(columns={'Open': 'qOpen', 'MLo': 'qLo', 'MHi': 'qHi'}, inplace=True)
    dfq = dfq.shift(1, freq='Q').resample('W').bfill()
    df = pd.concat([dfw['wOpen'], dfw['wLo'], dfw['wHi'],
                    dfm['mOpen'], dfm['mLo'], dfm['mHi'],
                    dfq['qOpen'], dfq['qLo'], dfq['qHi']],
                   axis=1, join_axes=[dfw.index])
    df.reset_index
    return df


def calc_multiple_symbol_frames(symbol_list):
    new_all_df = pd.DataFrame()
    for symbol in symbol_list:
        print(symbol)
        DF = refresh_symbol_frame(symbol)
        DF['SymbolName'] = symbol
        new_all_df = new_all_df.append(DF)
    new_all_df.reset_index(inplace=True)
    new_all_df.rename(columns={'index': 'DateTime'}, inplace=True)
    print('processed')
    print(symbol_list)
    return new_all_df


@asyncio.coroutine
def refresh(loop):
    symbolList = []  # ['X','HD','LMT','BA']
    df_frames = pd.DataFrame()
    while True:
        df_frames = check_for_refresh_event(df_frames, symbolList)

        df_frames = check_for_add_event(df_frames, symbolList)

        yield from check_for_send_event(df_frames, symbolList)

        yield None


def check_for_send_event(df_frames, symbol_list):
    if send_event.is_set():
        send_event.clear()
        if len(df_frames) == 0:
            df_frames = calc_multiple_symbol_frames(symbol_list)

        yield from q.put(df_frames)


def check_for_add_event(df_frames, symbol_list):
    if add_event.is_set():
        add_event.clear()
        addSymb = add_event.data
        print('Add')
        print(addSymb)
        update = False
        for element in addSymb:
            if not (element in symbol_list):
                symbol_list.append(element)
                update = True

        if update:
            df_frames = calc_multiple_symbol_frames(symbol_list)
    return df_frames


def check_for_refresh_event(df_frames, symbol_list):
    if refresh_event.is_set():
        refresh_event.clear()
        print('Refresh was sent')
        df_frames = calc_multiple_symbol_frames(symbol_list)
    return df_frames


class my_test_case(unittest.TestCase):
    def test_calc_multiple(self):
        symbolList = ['X', 'HD', 'LMT', 'BA']
        df_frames = calc_multiple_symbol_frames(symbolList)

        self.assertTrue('DateTime' in df_frames.keys())
        self.assertTrue('X' in df_frames['SymbolName'].values)
        self.assertTrue('HD' in df_frames['SymbolName'].values)
        self.assertTrue('LMT' in df_frames['SymbolName'].values)
        self.assertTrue('BA' in df_frames['SymbolName'].values)
        self.assertTrue(len(df_frames) > 0)

    def test_simple_frame(self):
        symbol_df = refresh_symbol_frame('HD')
        self.assertTrue('qHi' in symbol_df.keys())
        self.assertTrue('wHi' in symbol_df.keys())
        self.assertTrue('mHi' in symbol_df.keys())
        self.assertTrue('qOpen' in symbol_df.keys())
        self.assertTrue('wOpen' in symbol_df.keys())
        self.assertTrue('mOpen' in symbol_df.keys())
        self.assertTrue('qLo' in symbol_df.keys())
        self.assertTrue('wLo' in symbol_df.keys())
        self.assertTrue('mLo' in symbol_df.keys())
        self.assertTrue(len(symbol_df) > 0)

    def test_generate_overlay_oz_pandas(self):
        symbol_list = ['X', 'HD', 'LMT', 'BA']
        df_frames = calc_multiple_symbol_frames(symbol_list)
        print(df_frames.reset_index().to_json())
        # print(DFFrames.to_json())
        self.assertTrue(False)


def main():
    loop = asyncio.get_event_loop()
    loop.create_task(refresh(loop))
    # loop.create_task(calc_refresh(loop))
    server = loop.create_server(SimpleEchoProtocol, 'localhost', 2222)
    loop.run_until_complete(server)
    loop.run_forever()


if __name__ == "__main__":
    main()
