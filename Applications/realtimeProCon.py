#!/usr/bin/env python

import socket
import pandas as pd
import sys

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO
import asyncio

BUFFER_SIZE = 4096
COMM_PORT = 10000

data_queue = asyncio.Queue()
add_event = asyncio.Event()
addedSymbol_event = asyncio.Event()

kill_event = asyncio.Event()
delete_event = asyncio.Event()
send_event = asyncio.Event()
processed_data_queue = asyncio.Queue()
send_data_queue = asyncio.Queue()
updateOZ_event = asyncio.Event()
updateOZ_queue = asyncio.Queue()

symbol_list = ['AMT', 'AWK', 'BA', 'LMT', 'KORS', 'HES', 'HD', 'WMB', 'UA', 'X']


class CommServerProtocol:

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.addr = addr
        message = data.decode()
        if 'Add' in message:
            add_event.data = message.split('_')[1]
            add_event.set()
        if 'Send' in message:
            send_event.set()
            print('After send event')
            fut = asyncio.async(send_data_queue.get())
            fut.add_done_callback(self.write_reply)

    def write_reply(self, fut):
        reply = fut.result().to_json()
        print('In send')
        self.transport.sendto(reply.encode(), ('127.0.0.1', 1111))


class TCPProtocol(asyncio.Protocol):
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
        message = data.decode()
        if 'Add' in message:
            add_event.data = message.split('_')[1]
            add_event.set()
        if 'Send' in message:
            send_event.set()
            print('Send was sent')
            fut = asyncio.async(send_data_queue.get())
            fut.add_done_callback(self.write_reply)

    def write_reply(self, fut):
        reply = fut.result().to_json()
        print('Send: {!r}'.format(reply))
        self.transport.write(reply.encode())

    def eof_received(self):
        pass

    def connection_lost(self, exc):
        print('connection lost:', exc)


class TCPOZDataFrameProtocol(asyncio.Protocol):
    def __init__(self, message, loop):
        self.message = message
        self.loop = loop

    def connection_made(self, transport):
        print("Connection received!")
        self.transport.write(self.message.encode())
        self.transport = transport

    def timeout(self):
        print('connection timeout, closing.')
        self.transport.close()

    def connection_made(self, transport):
        print('connection made')
        self.transport = transport

    def data_received(self, data):
        updateOZ_event.data = pd.read_json(data.decode())
        updateOZ_event.set()

    def write_reply(self, fut):
        self.transport.write(self.message.encode())

    def eof_received(self):
        pass

    def connection_lost(self, exc):
        print('connection lost:', exc)


@asyncio.coroutine
def handle_OZServer(loop):
    reader, writer = yield from asyncio.open_connection('127.0.0.1', 2222, loop=loop)
    symbolList = list()
    while True:
        if updateOZ_event.is_set():
            print('In Server send')
            updateOZ_event.clear()
            for element in updateOZ_event.data:
                writer.write(('Add_' + element + '_End').encode())
            writer.write('Send'.encode())

            outputbuffer = StringIO()
            condition = True
            while condition:
                data = yield from reader.read(1024)
                message = data.decode()
                if message.find('!ENDMSG!') != -1:
                    message = message.replace('!ENDMSG!', '')
                    condition = False
                    print('End found')

                outputbuffer.write(message)

            outputbuffer.seek(0)
            DF = pd.read_json(outputbuffer)
            # print(DF)
            yield from updateOZ_queue.put(DF)
        yield None

    writer.close()
    reader.close()


@asyncio.coroutine
def tcp_iqfeed_client(loop):
    TCP_IP = '127.0.0.1'  # '85.214.93.234'
    TCP_PORT_History = 5009
    SET_MATLAB = "S,SET CLIENT NAME,MATLAB_CLIENT,\r\n"
    SET_UPDATE_FIELDS = "S,SELECT UPDATE FIELDS,Last Trade Time,Symbol,Last\r\n"  # ,Bid,Ask,Bid Size,Ask Size\r\n"
    CONNECT = "S,CONNECT\r\n"

    SECOND_SYMB = "wX\r\n"

    reader, writer = yield from asyncio.open_connection(TCP_IP, TCP_PORT_History, loop=loop)
    writer.write(SET_UPDATE_FIELDS.encode('ascii'))

    for element in symbol_list:
        MESSAGE = "w" + element + "\r\n"
        writer.write(MESSAGE.encode('ascii'))
    while True:
        if add_event.is_set():
            data = add_event.data
            symbol_list.append(data)
            MESSAGE = "w" + data + "\r\n"
            writer.write(MESSAGE.encode('ascii'))
            add_event.clear()
            addedSymbol_event.set()
        if delete_event.set():
            data = delete_event.data
            symbol_list.remove(data)
        data = yield from reader.read(1024)
        yield from data_queue.put(data.decode())
        # print(data.decode())
        if kill_event.is_set():
            writer.close()

    print('Close the socket')
    writer.close()


def calcRelOZ(DF):
    DF['dMHi'] = DF['mHi'] - DF['Quote']
    DF['dQHi'] = DF['qHi'] - DF['Quote']
    DF['dWHi'] = DF['wHi'] - DF['Quote']
    DF['dMLo'] = DF['Quote'] - DF['mLo']
    DF['dQLo'] = DF['Quote'] - DF['qLo']
    DF['dWLo'] = DF['Quote'] - DF['wLo']


@asyncio.coroutine
def consume_data(loop):
    StoredDF = pd.DataFrame()
    AllDataDF = pd.DataFrame()
    OZDF = pd.DataFrame()
    OZDFreduced = pd.DataFrame()
    # OZDFreduced['Symbol']=['APC']
    # OZDFreduced['mHi'] = [36]
    # OZDFreduced['qHi'] = [80]
    # OZDFreduced['wHi'] = [50]
    # OZDFreduced.set_index('Symbol',inplace=True)

    print('Before')
    OZDFreduced = yield from updateOZDFreduced(OZDFreduced)
    print(OZDFreduced)
    print('After')
    while True:
        if addedSymbol_event.is_set():
            print('Before update')
            OZDFreduced = yield from updateOZDFreduced(OZDFreduced)
            addedSymbol_event.clear()
            print('After update')

        data = yield from data_queue.get()
        # print('Data')
        print(data)
        #        print(data)
        if data[0] == 'Q':
            inString = StringIO(data)
            DF = pd.read_csv(inString, lineterminator='\n', names=['Type', 'Symbol', 'Date', 'Quote', 'Space'])
            StoredDF = pd.concat([StoredDF, DF], axis=0)
            StoredDF = StoredDF.drop_duplicates(subset=['Symbol'], keep='last')
            StoredDF = StoredDF[StoredDF.Type == 'Q']

            tempDF = StoredDF.set_index('Symbol')
            OZDFreduced.reset_index(inplace=True)
            OZDFreduced.set_index('Symbol', inplace=True)
            # print(tempDF)
            print(OZDFreduced)

            AllDataDF = pd.concat([tempDF, OZDFreduced.drop('Date', axis=1)], axis=1)
            calcRelOZ(AllDataDF)

        if send_event.is_set():
            print('In send event')
            AllDataDF.index.name = 'Symbol'
            print(AllDataDF.reset_index())  # .loc[:,('Symbol','Date')])
            send_event.clear()

            yield from send_data_queue.put(AllDataDF)


def updateOZDFreduced(OZDFreduced):
    updateOZ_event.data = symbol_list
    updateOZ_event.set()
    OZDF = yield from updateOZ_queue.get()
    OZDFreduced = OZDF[OZDF['DateTime'] == OZDF['DateTime'].max()]
    OZDFreduced.rename(columns={'SymbolName': 'Symbol', 'DateTime': 'Date'}, inplace=True)
    return OZDFreduced


@asyncio.coroutine
def save_data(loop):
    while True:
        data = yield from processed_data_queue.get()
        print('Store Data')


def main():
    loop = asyncio.get_event_loop()
    loop.create_task(tcp_iqfeed_client(loop))
    loop.create_task(consume_data(loop))
    loop.create_task(handle_OZServer(loop))

    listen = loop.create_datagram_endpoint(CommServerProtocol, local_addr=('127.0.0.1', 9999))
    transport, protocol = loop.run_until_complete(listen)
    loop.run_forever()


if __name__ == "__main__":
    main()
