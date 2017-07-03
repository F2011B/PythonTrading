import unittest
import os
import sys
import inspect

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

# use this if you want to include modules from a subfolder
cmd_subfolder = os.path.realpath(
    os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0], "subfolder")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)



import IQData


class MyTestCase(unittest.TestCase):
    def test_something(self):
        Data=IQData.get_daily_history('HD', 10)
        print(len(Data))
        self.assertEqual(len(Data),10 )

    def test_get_ohlc_days_back_pandas(self):
        Data=IQData.get_ohlc_days_back_pandas('HD',10)
        self.assertEqual(len(Data), 10)

    def test_get_daily_history_pandas(self):

        Data =IQData.get_daily_history_pandas('HD',10)
        print(Data)
        self.assertEqual(len(Data), 10)

    def test_get_intraday_pandas_dback(self):
        #Data=IQData.get_intraday_pandas_dback('HD')
        #print(Data)
        #self.assertEqual(len(Data), 77)
        Data = IQData.get_intraday_pandas_dback('AMT',3600,500)
        print(Data)
        self.assertEqual(len(Data), 500)

    def test_get_availableExchanges(self):
        Data = IQData.get_availableExchanges()
        self.assertGreater(len(Data),10 )






if __name__ == '__main__':
    unittest.main()
