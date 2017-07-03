#!/usr/bin/env python

import pandas as pd
import asyncio
import sys
import os
import inspect




cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

def insertModule(ModuleFolder):
    cmd_subfolder = os.path.realpath(
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],'..', ModuleFolder)))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)

insertModule('Constants')
insertModule('DatabaseAccess')
import Constants


q = asyncio.Queue()
add_event = asyncio.Event()
kill_event =asyncio.Event()
delete_event = asyncio.Event()
send_event =asyncio.Event()
refresh_event = asyncio.Event()
refreshed_event=asyncio.Event()




# Protocol should enable the service
# 1. To Add a Symbol
# 2. To Search for a available symbol
# 3. To track added symbols and store them in a file maybe hdf5


class SimpleEchoProtocol(asyncio.Protocol):
    TIMEOUT = 5.0
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
        for element in message.split('_End') :
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
def saveData(loop):
    symbolList=['X','HD','LMT','BA']
    DFFrames=pd.DataFrame()
    while True:
        if refresh_event.is_set():
            refresh_event.clear()
            print('Refresh was sent')

        if add_event.is_set():
            add_event.clear()
            addSymb= add_event.data
            print('Add')
            print(addSymb)
            update=False
            for element in addSymb:
                if not (element in symbolList):
                    symbolList.append(element)
                    update=True

        if send_event.is_set():
            send_event.clear()
            if len(DFFrames) == 0:
                yield from q.put(DFFrames)
            else:
                yield from q.put(DFFrames)
        yield None

def main():

    loop = asyncio.get_event_loop()
    loop.create_task(saveData(loop))
    server = loop.create_server(SimpleEchoProtocol, 'localhost', 3333)
    loop.run_until_complete(server)
    loop.run_forever()

if __name__ == "__main__":
    main()






