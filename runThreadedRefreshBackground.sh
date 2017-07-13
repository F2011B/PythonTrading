#!/bin/bash
cd ./Applications
exec -a MyThreadedRefresh python3 ThreadedRefresh.py &
disown -h
