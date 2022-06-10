from time import sleep
import os
from firebase_admin import firestore
from firebase_admin import credentials
import firebase_admin
from warnings import warn
from datetime import datetime, date
from typing import List

os.chdir("/mnt/g/workspace/MLBot/server/develop")


def make_doll_bar(target_rows: List[dict], threshold: int, price_col_name: str, volume_col_name: str, timestamp_col_name: str, timestmap_format: str, idx_col_name: str) -> bool:
    """ドルバーを作成する関数。

    Args:
        target_rows (List[dict]):処理対象のレコード
        threshold(int): 閾値

    Returns:
        List[dict]: 処理後のレコード
    """

    print("■ ohlcv形式でバーを作成")
    sum_sum_price = 0
    sum_volume = 0
    open_price = 0
    min_price = 0
    max_price = 0
    start_id = None
    today = datetime.strptime(target_rows[0][timestamp_col_name], timestmap_format).date()

    print("■■ barの作成")
    bar_list = []
    for row in target_rows:

        price = row[price_col_name]
        volume = row[volume_col_name]
        timestamp = datetime.strptime(target_rows[0][timestamp_col_name], timestmap_format)
        idx = row[idx_col_name]

        if start_id == None:
            start_id = row[idx_col_name]

        end_id = row[idx_col_name]

        sum_price = volume * price
        sum_sum_price += sum_price

        min_price = min(min_price, price)
        max_price = max(max_price, price)

        # 閾値を超えていたら、bar_listに追加する。
        # 最終的に、閾値を超えた分の取引のみがデータとして取得できる。中途半端に余ったものは次回に持ち越し。
        if sum_sum_price >= threshold:

            left_price = sum_sum_price - threshold
            left_volumne = (volume * left_price) / sum_price
            sum_volume += (volume - left_volumne)

            bar_list.append({
                "open": open_price,
                "low": min_price,
                "high": max_price,
                "close": price,
                "volume": sum_volume,
                "timestamp": timestamp,
                "date": timestamp.date(),
                "start_id": start_id,
                "end_id": end_id
            })
            open_price = price
            min_price = open_price
            max_price = open_price
            sum_sum_price = left_price
            sum_volume = left_volumne
            start_id = None
        else:
            sum_volume += volume

    return bar_list


# def getBitFlyer():
if not firebase_admin._apps:
    cred = credentials.Certificate('./serviceAccount.json')
    firebase_admin.initialize_app(cred)

db = firestore.client()
doc_ref = (
    db
    .collection('Exchanger')
    .document('bitFlyer')
)

# doc_ref.update({"processing_oldest_id": 2342354662, "processing_latest_id": 2342354662})


res = doc_ref.get().to_dict()
INTERVAL = 1000
THRESHOLD = 300000000

start_id = int(res["processing_latest_id"])
target_rows = [d.to_dict() for d in doc_ref.collection('raw_data').where("id", ">=", start_id).order_by('id').limit(INTERVAL).get()]


bar_list = make_doll_bar(
    target_rows,
    threshold=THRESHOLD,
    price_col_name="price",
    volume_col_name="size",
    timestamp_col_name="exec_date",
    timestmap_format='%Y-%m-%dT%H:%M:%S.%f',
    idx_col_name="id"
)

# TODO 書き込む

bar_list[-1]["end_id"]

# while True:
#     pyaload = {"product_code": "BTC_JPY", "count": 500, "after": start_id}

#     res_dict_list = requests.get(endpoint + "executions", params=pyaload).json()
#     # dir(res)

#     if len(res_dict_list) == 0:
#         break

#     new_start_dict = res_dict_list[0]
#     latest_id = new_start_dict["id"]
#     latest_dt = new_start_dict["exec_date"][:10].replace('-', '')

#     print(latest_id, latest_dt)

#     for res_dict in res_dict_list:
#         row = doc_ref.collection('raw_data').document(str(res_dict["id"]))
#         row.set(res_dict)

#     doc_ref.update({"latest_ymd": latest_dt, "latest_id": latest_id})

#     start_id = latest_id
#     sleep(0.8)
