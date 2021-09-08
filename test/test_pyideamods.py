import unittest
import os
from pyieamods import pyieamods


class TestpyModsIEA(unittest.TestCase):

    def test_get_proddat(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))
        test_zip = os.path.join(dirname, 'sdbstxt.zip')
        res = pyieamods.read_mods(test_zip, specific_files=['PRODDAT.TXT'])
        self.assertIn('PRODDAT.TXT', res)
        proddat = res['PRODDAT.TXT']
        self.assertIn('IEA.PROD.CRUDEOIL.AUSTRALI.OMRDEMB', proddat.columns)

    def test_get_crudedat(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))
        test_zip = os.path.join(dirname, 'sdbstxt.zip')
        res = pyieamods.read_mods(test_zip, specific_files=['CRUDEDAT.TXT'])
        self.assertIn('CRUDEDAT.TXT', res)
        crudedat = res['CRUDEDAT.TXT']
        self.assertIn('IEA.CRUDE.AUSTRALI.CRNGFEED.CSNATTERT', crudedat.columns)

    def test_get_stockdat(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))
        test_zip = os.path.join(dirname, 'sdbstxt.zip')
        res = pyieamods.read_mods(test_zip, specific_files=['STOCKDAT.TXT'])
        self.assertIn('STOCKDAT.TXT', res)
        stocksdat = res['STOCKDAT.TXT']
        self.assertIn('IEA.STOCKS.INDUSTRY.CRNGFEED.AUSTRIA', stocksdat.columns)

    def test_get_summarydat(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))
        test_zip = os.path.join(dirname, 'sdbstxt.zip')
        res = pyieamods.read_mods(test_zip, specific_files=['SUMMARY.TXT'])
        self.assertIn('SUMMARY.TXT', res)
        summary = res['SUMMARY.TXT']
        self.assertIn('IEA.SUMMARY.AMEDEM.FINAL', summary.columns)

    def test_get_supplydat(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))
        test_zip = os.path.join(dirname, 'sdbstxt.zip')
        res = pyieamods.read_mods(test_zip, specific_files=['SUPPLY.TXT'])
        self.assertIn('SUPPLY.TXT', res)
        supply = res['SUPPLY.TXT']
        self.assertIn('IEA.SUPPLY.COND.AUSTRALIA', supply.columns)

    def test_get_nonoecddat(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))
        test_zip = os.path.join(dirname, 'sdbstxt.zip')
        res = pyieamods.read_mods(test_zip, specific_files=['NOECDDE.TXT'])
        self.assertIn('NOECDDE.TXT', res)
        noedcdde = res['NOECDDE.TXT']
        self.assertIn('IEA.NOECDDE.BELARUS', noedcdde.columns)

    def test_get_oecddat(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))
        test_zip = os.path.join(dirname, 'sdbstxt.zip')
        res = pyieamods.read_mods(test_zip, specific_files=['OECDDE.TXT'])
        self.assertIn('OECDDE.TXT', res)
        oecdde = res['OECDDE.TXT']
        self.assertIn('IEA.OECDDE.JETANDKERO.AUSTRALI', oecdde.columns)
        self.assertEqual(oecdde['IEA.OECDDE.LPGETHANE.AUSTRALI']['2015-01'].iloc[0], 77.4722)

    def test_get_all(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))
        test_zip = os.path.join(dirname, 'sdbstxt.zip')
        res = pyieamods.read_mods(test_zip)
        self.assertTrue(isinstance(res, dict))

    def test_get_series_proddat(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))
        test_zip = os.path.join(dirname, 'sdbstxt.zip')
        df = pyieamods.get_series('IEA.PROD.CRUDEOIL.AUSTRALI.OMRDEMB', test_zip)
        self.assertEqual(df.columns, ['IEA.PROD.CRUDEOIL.AUSTRALI.OMRDEMB'])


if __name__ == '__main__':
    unittest.main()


