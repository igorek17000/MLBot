# テスト用
# import os
# os.chdir(os.getcwd() + "/src")

# インポート
from lib.getData import getRawData
from lib.rawDataProcessing import rawDataProcessing
from lib.makeBarProcessing import makeBarProcessing
from lib.downloadFireStore import downloadFireStore
from datetime import date, datetime


# GMO
from_dt = date(2022, 5, 13)
to_dt = date(2022, 6, 30)
MARKET = "BTC"
DL_STITE = "GMO"

get_raw_data = getRawData(market=MARKET, dl_site=DL_STITE)
res = get_raw_data.get_data(from_dt=from_dt, to_dt=to_dt)

raw_data_processing = rawDataProcessing(market=MARKET, dl_site=DL_STITE)
res = raw_data_processing.process_raw_format(from_dt=from_dt, to_dt=to_dt)

make_bar_processing = makeBarProcessing("BTC", "GMO")
make_bar_processing.make_bar(from_dt=from_dt, to_dt=to_dt, bar_setting={
    "type": "doll", "threshold": 300000000})

# BitFlyer
from_dt = date(2022, 5, 11)
MARKET = "BTC"
DL_STITE = "bitFlyer"
FROM_BAR_NAME = 'processing_doll_bar_300000000'

dfs = downloadFireStore(
    market=MARKET,
    dl_site=DL_STITE,
    from_bar_name=FROM_BAR_NAME
)
dfs.download_bar(from_dt=from_dt)
