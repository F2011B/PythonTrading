#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 21:04:08 2017

@author: lc1bfrbl
"""

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


def insert_module(ModuleFolder):
    cmd_subfolder = os.path.realpath(
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0], '..', ModuleFolder)))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)


insert_module('PandasCore')
insert_module('Constants')
insert_module('DataProviderAccess')

import OZ

DF = OZ.generate_overlays_oz_pandas('1h', 'WTICO_USD', numWeeksBack=200).dropna()
