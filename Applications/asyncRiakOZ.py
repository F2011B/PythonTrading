#!/usr/bin/env python

import pandas as pd
import asyncio
import unittest
import datetime
import time
import sys
import os
import inspect
import time

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)
print(cmd_folder)


def insertModule(ModuleFolder):
    cmd_subfolder = os.path.realpath(
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0], '..', ModuleFolder)))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)


insertModule('PandasCore')
insertModule('Constants')
insertModule('DataProviderAccess')

import OZ

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)


def insertModule(ModuleFolder):
    cmd_subfolder = os.path.realpath(
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0], '..', ModuleFolder)))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)


insertModule('Constants')
insertModule('DatabaseAccess')
import Constants
import RiakDataBaseAccess

symbolList = Constants.SymbolsToWatch

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
    refreshed = False
    lastMinute = -1
    while True:
        Minute = datetime.datetime.today().minute
        isSaturday = (Minute != lastMinute)  # Constants.OZRefreshDay)
        if isSaturday and not refreshed:
            refresh_event.set()
            lastMinute = Minute
            refreshed = False
        if not isSaturday:
            refreshed = False
        time.sleep(2)
        yield None


@asyncio.coroutine
def refresh(loop):
    DFFrames = pd.DataFrame()
    while True:
        DFFrames = check_for_refresh_event(DFFrames)

        DFFrames = check_for_add_event(DFFrames)

        DFFrames = yield from check_for_send_event(DFFrames)
        time.sleep(0.1)
        yield None


def check_for_send_event(DFFrames):
    if send_event.is_set():
        send_event.clear()
        if len(DFFrames) == 0:
            DFFrames = RiakDataBaseAccess.updateOZRiak(symbolList)
        yield from q.put(DFFrames)



def check_for_add_event(DFFrames):
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
            DFFrames = RiakDataBaseAccess.updateOZRiak(symbolList)
    return DFFrames


def check_for_refresh_event(DFFrames):
    if refresh_event.is_set():
        refresh_event.clear()
        print('Refresh was sent')
        DFFrames = RiakDataBaseAccess.updateOZRiak(symbolList)
    return DFFrames


class MyTestCase(unittest.TestCase):
    def test_calc_multiple(self):
        symbolList = ['WTICO_USD']  # , 'HD', 'LMT', 'BA']
        # DFFrames = RiakDataBaseAccess.updateOZRiak(symbolList)
        DF = OZ.generate_overlays_oz_pandas('1h', 'WTICO_USD', numWeeksBack=200).dropna()
        print(DF)
        # print(DFFrames.keys())
        # self.assertTrue('DateTime' in  DFFrames.keys() )
        # self.assertTrue('X' in DFFrames['SymbolName'].values)
        # self.assertTrue('HD' in DFFrames['SymbolName'].values)
        # self.assertTrue('LMT' in DFFrames['SymbolName'].values)
        # self.assertTrue('BA' in DFFrames['SymbolName'].values)
        # self.assertTrue(len(DFFrames)>0)

    def test_simple_frame(self):
        SymbolDF = refresh_SymbolFrame('WTICO_USD')
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
        # DFFrames = updateRiak(symbolList)
        print(DFFrames.reset_index().to_json())
        # print(DFFrames.to_json())
        self.assertTrue(False)

    def refresh_DataBase(self):
        DFFrames = RiakDataBaseAccess.updateOZRiak(['WTICO_USD'])
        print(DFFrames)


def main():
    loop = asyncio.get_event_loop()
    loop.create_task(refresh(loop))
    loop.create_task(calc_refresh(loop))
    server = loop.create_server(SimpleEchoProtocol, 'localhost', 2222)
    loop.run_until_complete(server)
    loop.run_forever()


if __name__ == "__main__":
    main()
