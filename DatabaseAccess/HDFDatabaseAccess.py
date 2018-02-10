import pandas as pd
import os
import sys
import inspect

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)


# use this if you want to include modules from a subfolder

def insert_module(ModuleFolder):
    cmd_subfolder = os.path.realpath(
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0], '..', ModuleFolder)))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)


insert_module('Constants')
import Constants


def query_from_to(symbol, Start, today):
    store = pd.HDFStore(Constants.StockHDF)

    if not (symbol in store.keys()):
        return None

    store[]
