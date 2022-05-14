# テスト用
# import os
# os.chdir(os.getcwd() + "/src")

# インポート
from raw.getData import getRawData
from datetime import date, datetime

get_raw_data = getRawData("BTC", "GMO")

from_dt = date(2022, 5, 11)
to_dt = date(2022, 5, 30)

res = get_raw_data.get_data(from_dt=from_dt, to_dt=to_dt)
