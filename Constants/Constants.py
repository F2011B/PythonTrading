import os
from os.path import expanduser
home = expanduser("~")

WeekDays={'Monday':0,'Tuesday':1,'Wednesday':2,'Thursday':3,'Friday':4,'Saturday':5,'Sunday':6}

InputFolder=os.path.join(home,'/Development/Servers/restserver/Input/')

Database=os.path.join(home,'Database')
DatabaseOanda=os.path.join(Database,'Oanda.hdf')
DatabaseTaylor=os.path.join(Database,'Taylor.hdf')
DatabaseTaylorCP=os.path.join(Database,'TaylorCP.hdf')
SymbolsHDF=os.path.join(Database,'symbols.hdf')
StockHDF=os.path.join(Database,'Stock.hdf')


LoggingUpdateHDF=os.path.join(home,'log/ThreadedRefresh.log')
LoggingFlask=os.path.join(home,'log/Flask.log')


OZRefreshDay=WeekDays['Monday']
RiakServer='85.214.153.175'
RiakPort= 8098
RiakOZTable='MyStockData'

#SymbolsToWatch=['LMT','HES','WMB','X', 'HD', 'LMT', 'BA','NVDA','LOW']
SymbolsToWatch=['EUR_USD','CORN_USD','DE30_EUR','DE10YB_EUR','EU50_EUR','US30_USD','USB30Y_USD','XAU_USD','WHEAT_USD'
    ,'WTICO_USD','NATGAS_USD','XAG_USD','XCU_USD','XPD_USD','XPT_USD',]
# SymbolsToWatch=[
# 'AU200_AUD',
# 'AUD_CAD',
# 'AUD_CHF',
# 'AUD_HKD',
# 'AUD_JPY',
# 'AUD_NZD',
# 'AUD_SGD',
# 'AUD_USD',
# 'BCO_USD',
# 'CAD_CHF',
# 'CAD_HKD',
# 'CAD_JPY',
# 'CAD_SGD',
# 'CH20_CHF',
# 'CHF_HKD',
# 'CHF_JPY',
# 'CHF_ZAR',
# 'CN50_USD',
# 'CORN_USD',
# 'DE10YB_EUR',
# 'DE30_EUR',
# 'EU50_EUR',
# 'EUR_AUD',
# 'EUR_CAD',
# 'EUR_CHF',
# 'EUR_CZK',
# 'EUR_DKK',
# 'EUR_GBP',
# 'EUR_HKD',
# 'EUR_HUF',
# 'EUR_JPY',
# 'EUR_NOK',
# 'EUR_NZD',
# 'EUR_PLN',
# 'EUR_SEK',
# 'EUR_SGD',
# 'EUR_TRY',
# 'EUR_USD',
# 'EUR_ZAR',
# 'FR40_EUR',
# 'GBP_AUD',
# 'GBP_CAD',
# 'GBP_CHF',
# 'GBP_HKD',
# 'GBP_JPY',
# 'GBP_NZD',
# 'GBP_PLN',
# 'GBP_SGD',
# 'GBP_USD',
# 'GBP_ZAR',
# 'HK33_HKD',
# 'HKD_JPY',
# 'IN50_USD',
# 'JP225_USD',
# 'NAS100_USD',
# 'NATGAS_USD',
# 'NL25_EUR',
# 'NZD_CAD',
# 'NZD_CHF',
# 'NZD_HKD',
# 'NZD_JPY',
# 'NZD_SGD',
# 'NZD_USD',
# 'SG30_SGD',
# 'SGD_CHF',
# 'SGD_HKD',
# 'SGD_JPY',
# 'SOYBN_USD',
# 'SPX500_USD',
# 'SUGAR_USD',
# 'TRY_JPY',
# 'TWIX_USD',
# 'UK100_GBP',
# 'UK10YB_GBP',
# 'US2000_USD',
# 'US30_USD',
# 'USB02Y_USD',
# 'USB05Y_USD',
# 'USB10Y_USD',
# 'USB30Y_USD',
# 'USD_CAD',
# 'USD_CHF',
# 'USD_CNH',
# 'USD_CZK',
# 'USD_DKK',
# 'USD_HKD',
# 'USD_HUF',
# 'USD_INR',
# 'USD_JPY',
# 'USD_MXN',
# 'USD_NOK',
# 'USD_PLN',
# 'USD_SAR',
# 'USD_SEK',
# 'USD_SGD',
# 'USD_THB',
# 'USD_TRY',
# 'USD_ZAR',
# 'WHEAT_USD',
# 'WTICO_USD',
# 'XAG_AUD',
# 'XAG_CAD',
# 'XAG_CHF',
# 'XAG_EUR',
# 'XAG_GBP',
# 'XAG_HKD',
# 'XAG_JPY',
# 'XAG_NZD',
# 'XAG_SGD',
# 'XAG_USD',
# 'XAU_AUD',
# 'XAU_CAD',
# 'XAU_CHF',
# 'XAU_EUR',
# 'XAU_GBP',
# 'XAU_HKD',
# 'XAU_JPY',
# 'XAU_NZD',
# 'XAU_SGD',
# 'XAU_USD',
# 'XAU_XAG',
# 'XCU_USD',
# 'XPD_USD',
# 'XPT_USD',
# 'ZAR_JPY' ]
