#!/usr/bin/env python

import threading
import sys
import os
import inspect
import pandas as pd
import time
import logging
from logging.handlers import RotatingFileHandler


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



log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')

logFile = Constants.LoggingUpdateHDF

my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024,
                                 backupCount=2, encoding=None, delay=0)
my_handler.setFormatter(log_formatter)
my_handler.setLevel(logging.INFO)

app_log = logging.getLogger('root')
app_log.setLevel(logging.INFO)

app_log.addHandler(my_handler)



def printit():
    threading.Timer(600.0, printit).start()

    for element in symbolList:
        app_log.info(element)
        DF =  Oanda.get_intraday_pandas_dback(element,3000,'H1')
        TTTDF=TaylorCycle.CalcTaylorCycle(DF)
        TTTDF.to_hdf(Constants.DatabaseTaylor,element)
        app_log.info('TaylorHDF written')
        time.sleep(5)


printit()