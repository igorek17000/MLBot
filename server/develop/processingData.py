import os
from firebase_admin import firestore
from firebase_admin import credentials
import firebase_admin
from datetime import datetime
from typing import List

# os.chdir("./server/develop")


def makeBar(event, context):
    make_bitflyer_doll_bar()


def make_doll_bar(
        target_rows: List[dict], threshold: int,
        price_col_name: str, volume_col_name: str,
        timestamp_col_name: str, timestmap_format: str,
        idx_col_name: str, latest_pricessed_row: dict = None) -> bool:
    """ドルバーを作成する関数。

    Args:
        target_rows (List[dict]):処理対象のレコード
        threshold(int): 閾値

    Returns:
        List[dict]: 処理後のレコード
    """

    print("■ ohlcv形式でバーを作成")
    if latest_pricessed_row is None:
        sum_sum_price = 0
        sum_volume = 0
    else:
        sum_sum_price = latest_pricessed_row["left_price"]
        sum_volume = latest_pricessed_row["left_volumne"]
    open_price = target_rows[0][price_col_name]
    min_price = open_price
    max_price = open_price
    start_id = target_rows[0][idx_col_name]

    print("■■ barの作成")
    bar_list = []
    for row in target_rows:

        price = row[price_col_name]
        volume = row[volume_col_name]
        timestamp = datetime.strptime(row[timestamp_col_name], timestmap_format)
        idx = row[idx_col_name]

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

            if left_price != 0:
                next_id = end_id
            else:
                next_id = end_id + 1

            bar_list.append({
                "open": open_price,
                "low": min_price,
                "high": max_price,
                "close": price,
                "volume": sum_volume,
                "timestamp": timestamp,
                "date": str(timestamp.date()),
                "start_id": start_id,
                "end_id": end_id,

                "left_price": left_price,
                "left_volumne": left_volumne,
                "next_id": next_id
            })
            open_price = price
            min_price = open_price
            max_price = open_price
            sum_sum_price = left_price
            sum_volume = left_volumne
            start_id = next_id
        else:
            sum_volume += volume

    return bar_list


def make_bitflyer_doll_bar():

    INTERVAL = 10000
    THRESHOLD = 300000000
    COLLECTION_NAME = f'processing_doll_bar_{THRESHOLD}'

    if not firebase_admin._apps:
        cred = credentials.Certificate('./serviceAccount.json')
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    doc_ref = (
        db
        .collection('Exchanger')
        .document('bitFlyer')
    )

    res = doc_ref.get().to_dict()

    start_id = int(res["processing_latest_id"])
    target_rows = [d.to_dict() for d in doc_ref.collection('raw_data').where("id", ">=", start_id).order_by('id').limit(INTERVAL).get()]

    # 初期化時は下記コメントアウト
    latest_pricessed_row = [d.to_dict() for d in doc_ref.collection(COLLECTION_NAME).order_by(
        'end_id', direction=firestore.Query.DESCENDING).limit(1).get()][0]

    def check_format(strtime):
        if "." not in strtime:
            return strtime + ".0"
        else:
            return strtime

    for row in target_rows:
        row["exec_date"] = check_format(row["exec_date"])

    bar_list = make_doll_bar(
        target_rows,
        threshold=THRESHOLD,
        price_col_name="price",
        volume_col_name="size",
        timestamp_col_name="exec_date",
        timestmap_format='%Y-%m-%dT%H:%M:%S.%f',
        idx_col_name="id",

        # 初期化時は下記コメントアウト
        latest_pricessed_row=latest_pricessed_row
    )

    if len(bar_list) == 0:
        print("更新なし")
        print(target_rows[-1])
        return

    for res_dict in bar_list:
        row = doc_ref.collection(COLLECTION_NAME).document(str(res_dict["end_id"]))
        row.set(res_dict)

    doc_ref.update({
        "processing_latest_id": bar_list[-1]["next_id"],
        "processing_timestamp": bar_list[-1]["timestamp"]
    })
