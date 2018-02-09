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
    yield None


def refresh_SymbolFrame(Symbol):
    DFw = OZ.calc_oz_series_pandas(Symbol, numWeeksBack=20, averageTf='W')
    DFw.rename(columns={'Open': 'wOpen', 'MLo': 'wLo', 'MHi': 'wHi'}, inplace=True)
    DFm = OZ.calc_oz_series_pandas(Symbol, numWeeksBack=20, averageTf='M')
    DFm = DFm.shift(1, freq='M').resample('W').bfill()
    DFm.rename(columns={'Open': 'mOpen', 'MLo': 'mLo', 'MHi': 'mHi'}, inplace=True)
    DFq = OZ.calc_oz_series_pandas(Symbol, numWeeksBack=20, averageTf='Q')
    DFq.rename(columns={'Open': 'qOpen', 'MLo': 'qLo', 'MHi': 'qHi'}, inplace=True)
    DFq = DFq.shift(1, freq='Q').resample('W').bfill()
    DF = pd.concat([DFw['wOpen'], DFw['wLo'], DFw['wHi'],
                    DFm['mOpen'], DFm['mLo'], DFm['mHi'],
                    DFq['qOpen'], DFq['qLo'], DFq['qHi']],
                   axis=1, join_axes=[DFw.index])
    DF.reset_index
    return DF


def calc_multipleSymbolFrames(symbolList):
    DFDict = {}
    newAllDF = pd.DataFrame()
    for symbol in symbolList:
        print(symbol)
        DF = refresh_SymbolFrame(symbol)
        DF['SymbolName'] = symbol
        newAllDF = newAllDF.append(DF)
    newAllDF.reset_index(inplace=True)
    newAllDF.rename(columns={'index': 'DateTime'}, inplace=True)
    print('processed')
    print(symbolList)
    return newAllDF


@asyncio.coroutine
def refresh(loop):
    symbolList = []  # ['X','HD','LMT','BA']
    DFFrames = pd.DataFrame()
    while True:
        DFFrames = check_for_refresh_event(DFFrames, symbolList)

        DFFrames = check_for_add_event(DFFrames, symbolList)

        yield from check_for_send_event(DFFrames, symbolList)

        yield None


def check_for_send_event(DFFrames, symbolList):
    if send_event.is_set():
        send_event.clear()
        if len(DFFrames) == 0:
            DFFrames = calc_multipleSymbolFrames(symbolList)

        yield from q.put(DFFrames)



def check_for_add_event(DFFrames, symbolList):
    if add_event.is_set():
        add_event.clear()
        addSymb = add_event.data
        print('Add')
        print(addSymb)
        update = False
        for element in addSymb:
            if not (element in symbolList):
                symbolList.append(element)
                update = True

        if update:
            DFFrames = calc_multipleSymbolFrames(symbolList)
    return DFFrames


def check_for_refresh_event(DFFrames, symbolList):
    if refresh_event.is_set():
        refresh_event.clear()
        print('Refresh was sent')
        DFFrames = calc_multipleSymbolFrames(symbolList)
    return DFFrames


class MyTestCase(unittest.TestCase):
    def test_calc_multiple(self):
        symbolList = ['X', 'HD', 'LMT', 'BA']
        DFFrames = calc_multipleSymbolFrames(symbolList)

        self.assertTrue('DateTime' in DFFrames.keys())
        self.assertTrue('X' in DFFrames['SymbolName'].values)
        self.assertTrue('HD' in DFFrames['SymbolName'].values)
        self.assertTrue('LMT' in DFFrames['SymbolName'].values)
        self.assertTrue('BA' in DFFrames['SymbolName'].values)
        self.assertTrue(len(DFFrames) > 0)

    def test_simple_frame(self):
        SymbolDF = refresh_SymbolFrame('HD')
        self.assertTrue('qHi' in SymbolDF.keys())
        self.assertTrue('wHi' in SymbolDF.keys())
        self.assertTrue('mHi' in SymbolDF.keys())
        self.assertTrue('qOpen' in SymbolDF.keys())
        self.assertTrue('wOpen' in SymbolDF.keys())
        self.assertTrue('mOpen' in SymbolDF.keys())
        self.assertTrue('qLo' in SymbolDF.keys())
        self.assertTrue('wLo' in SymbolDF.keys())
        self.assertTrue('mLo' in SymbolDF.keys())
        self.assertTrue(len(SymbolDF) > 0)

    def test_generate_overlay_oz_pandas(self):
        symbolList = ['X', 'HD', 'LMT', 'BA']
        DFFrames = calc_multipleSymbolFrames(symbolList)
        print(DFFrames.reset_index().to_json())
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
