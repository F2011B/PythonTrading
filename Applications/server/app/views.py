from app import app
import flask
import sys
import os
import inspect
import datetime

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

# use this if you want to include modules from a subfolder

def insertModule(ModuleFolder):
    cmd_subfolder = os.path.realpath(
        os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],'..','..','..', ModuleFolder)))
    print(cmd_subfolder)
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)

insertModule('DatabaseAccess')
insertModule(  )

@app.route('/intraDay')
def get_intraday_chart():
    symbol = flask.request.args.get('symbol')
    tf = flask.request.args.get('timeFrame')
    averageTf= flask.request.args.get('ATF')
    if averageTf == None:
        averageTf='W'

    column_order=['Date','Close','Volume','Open','Low','High','wLo','wHi','wOpen']
    today = datetime.datetime.now()  # - datetime.timedelta(days=1)
    Start = today - datetime.timedelta(days=40)
    DF=RDB.query_from_to(symbol,Start,today)

    resp = flask.make_response(DF[column_order].to_csv(index_label='Date'))
    resp.headers["Content-Disposition"] = "attachment; filename=table.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp #OZW.to_csv(index_label='Date')










