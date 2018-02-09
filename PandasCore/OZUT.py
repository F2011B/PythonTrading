import unittest
from Modules import OZ
from Modules import IQData
from Modules import CandleHelper


class MyTestCase(unittest.TestCase):
    def test_open_zero(self):
        data = IQData.get_intraday_IQ('HD', IQData.intervalMap['1h'])
        date = data['Date']
        wEDate, wEOpen, newHigh, newLow = OZ.generate_overlay_oz(date, 'HD')  # ascending order
        self.assertFalse(newLow[0] == newHigh[0])
        self.assertLessEqual(newLow[0], 0)
        self.assertGreaterEqual(newHigh[0], 0)
        self.assertTrue(date[0] > date[1])
        if date[0] > date[1]:
            self.assertTrue(wEDate[0] > wEDate[1])

    def test_oz(self):
        print(CandleHelper.rgb_to_hex(CandleHelper.Blue))
        print(CandleHelper.rgb_to_hex(CandleHelper.Magenta))
        print(CandleHelper.rgb_to_hex(CandleHelper.Green))
        print(CandleHelper.rgb_to_hex(CandleHelper.DarkGreen))
        print(CandleHelper.rgb_to_hex(CandleHelper.Red))
        self.assertTrue(False)

    def test_generate_overlay_oz_pandas(self):
        print(OZ.generate_overlay_oz_pandas('1d', 'HD', averageTf='Q').keys())

        self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
