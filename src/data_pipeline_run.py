# テスト用
# import os
# os.chdir(os.getcwd() + "/src")

# インポート
from getData import getRawData
from rawDataProcessing import rawDataProcessing
from makeBarProcessing import makeBarProcessing
from datetime import date, datetime


from_dt = date(2018, 9, 5)
to_dt = date(2022, 5, 30)
MARKET = "BTC"
DL_STITE = "GMO"

# get_raw_data = getRawData(market=MARKET, dl_site=DL_STITE)
# res = get_raw_data.get_data(from_dt=from_dt, to_dt=to_dt)

raw_data_processing = rawDataProcessing(market=MARKET, dl_site=DL_STITE)
res = raw_data_processing.process_raw_format(from_dt=from_dt, to_dt=to_dt)

make_bar_processing = makeBarProcessing("BTC", "GMO")
make_bar_processing.make_bar(from_dt=from_dt, to_dt=to_dt, bar_setting={
    "type": "doll", "threshold": 300000000})
