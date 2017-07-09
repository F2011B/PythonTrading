#!/usr/bin/env python

import threading
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
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],'..', ModuleFolder)))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)

insertModule('PandasCore')
insertModule('Constants')
insertModule('DataProviderAccess')
import Oanda
import TaylorCycle

import OZ


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
symbolList = Constants.SymbolsToWatch


def printit():
    threading.Timer(600.0, printit).start()

    for element in symbolList:
        DF =  Oanda.get_intraday_pandas_dback(element,3000,'H1')
        TTTDF=TaylorCycle.CalcTaylorCycle(DF)
        TTTDF.to_hdf(Constants.DatabaseTaylor,element)
        time.sleep(100)


printit()