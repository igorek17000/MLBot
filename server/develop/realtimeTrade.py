import os
from firebase_admin import firestore
from firebase_admin import credentials
import firebase_admin
from datetime import datetime, date
from typing import List
from pandas import DataFrame
# import pandas as pd
import numpy as np

# os.chdir("./server/develop")


def trade(event, context):
    judgeGoaldenCrossBitFlyer()


def _calc_ma_now(td_idx: int, n: int, tg_df: DataFrame, price_col: str) -> float:
    """移動平均を計算する関数

    Args:
        td_idx (int): 基準のidx
        n (int): 移動平均のN
        tg_df (DataFrame): 対象抽出されたohlcvデータ
        price_col (str): 価格のカラム名

    Returns:
        float: _description_
    """
    return np.mean(tg_df.iloc[td_idx - n + 1:td_idx+1][price_col])


def judgeGoaldenCrossBitFlyer():

    DL_STITE = "bitFlyer"
    FROM_BAR_NAME = 'processing_doll_bar_300000000'
    LONG_MA_N = 18
    SHORT_MA_N = 3
    SELL_TILT_SPAN = 8
    SELL_TILT_THRESHOLD = 22.69003088824373
    PRICE_COL = "close"
    RISK = 0.05*0.01
    RULE_NAME = "GoaldenCross_300000000"

    read_limit = max(LONG_MA_N, SHORT_MA_N + SELL_TILT_SPAN)

    print(dict(
        DL_STITE=DL_STITE,
        FROM_BAR_NAME=FROM_BAR_NAME,
        LONG_MA_N=LONG_MA_N,
        SHORT_MA_N=SHORT_MA_N,
        SELL_TILT_SPAN=SELL_TILT_SPAN,
        SELL_TILT_THRESHOLD=SELL_TILT_THRESHOLD,
        PRICE_COL=PRICE_COL,
        RISK=RISK,
        RULE_NAME=RULE_NAME,
        read_limit=read_limit,
    ))

    if not firebase_admin._apps:
        cred = credentials.Certificate('./serviceAccount.json')
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    doc_ref = (
        db
        .collection('Exchanger')
        .document(DL_STITE)
    )

    target_rows = [
        d.to_dict()
        for d in doc_ref.collection(FROM_BAR_NAME).order_by('end_id', direction=firestore.Query.DESCENDING).limit(read_limit).get()
    ]

    bar_list = [
        {
            "end_id": row["end_id"],
            "open":  row["open"],
            "low": row["low"],
            "high": row["high"],
            "close": row["close"],
            "volume": row["volume"],
            "timestamp": row["timestamp"],
            "date": datetime.strptime(row["date"], '%Y-%m-%d').date()
        }
        for row in target_rows
    ]

    # 既に判定済みであればスキップする。
    rule_doc = doc_ref.collection('trade_rule').document(RULE_NAME).get().to_dict()
    if rule_doc["judge_latest_id"] == bar_list[-1]["end_id"]:
        return True

    tg_df = DataFrame(bar_list)
    now_idx = LONG_MA_N

    # now_idx 時点の移動平均
    td_long_ma = _calc_ma_now(now_idx, LONG_MA_N, tg_df, PRICE_COL)
    td_short_ma = _calc_ma_now(now_idx, SHORT_MA_N, tg_df, PRICE_COL)

    # now_idx - 1 時点の移動平均
    yd_long_ma = _calc_ma_now(now_idx-1, LONG_MA_N, tg_df, PRICE_COL)
    yd_short_ma = _calc_ma_now(now_idx-1, SHORT_MA_N, tg_df, PRICE_COL)

    price = bar_list[-1][PRICE_COL]  # NOTE 実際のリアルタイムの価格に変更

    trade_flg = False

    # N-1 時点では long > short & N 時点では long <= short となり、上に抜いたら買うタイミング
    if (yd_long_ma > yd_short_ma) & (td_long_ma <= td_short_ma):

        # 買えないことがあるので、5000円残すようにする。
        balance = 1000000  # TODO リリース時に要変更
        buy_volume = ((balance - 5000) / price) * 1000
        buy_volume = np.floor(buy_volume) / 1000

        buy_doc = (
            doc_ref
            .collection('trade_rule')
            .document(RULE_NAME)
            .collection("buy_list")
            .document(str(bar_list[-1]['end_id']))
        )

        buy_dict = {
            "status": "Buy",
            "end_id": bar_list[-1]['end_id'],
            "buy_volume": buy_volume,
            "buy_price": price,
            "buy_timestamp": bar_list[-1]['timestamp'],
            "buy_amount": buy_volume * (1-RISK) * price
        }

        print(buy_dict)
        buy_doc.set(buy_dict)
        trade_flg = True

    # shortの傾きが一定値以下になったら売り
    else:
        buy_doc_list = [
            d.to_dict()
            for d in doc_ref.collection('trade_rule').document(RULE_NAME).collection("buy_list").where("status", "==", "Buy").get()
        ]

        if len(buy_doc_list) == 0:
            return True

        base_short_ma = _calc_ma_now(now_idx - SELL_TILT_SPAN, SHORT_MA_N, tg_df, PRICE_COL)
        sell_tilt = (td_short_ma - base_short_ma) / SELL_TILT_SPAN

        if sell_tilt <= SELL_TILT_THRESHOLD:

            for buy_doc in buy_doc_list:
                sell_doc = (
                    doc_ref
                    .collection('trade_rule')
                    .document(RULE_NAME)
                    .collection("buy_list")
                    .document(str(buy_doc['end_id']))
                )
                sell_doc_dict = sell_doc.get().to_dict()
                risk_volume = sell_doc_dict["buy_volume"] * (1-RISK)

                amount = risk_volume * price
                sell_return = amount - sell_doc_dict["buy_amount"]

                sell_dict = {
                    "status": "Sell",
                    "sell_amount": amount,
                    "sell_return": sell_return,
                    "sell_volume": risk_volume,
                    "sell_price": price,
                    "sell_timestamp": datetime.now()
                }
                print(sell_dict)
                sell_doc.update(sell_dict)
            trade_flg = True

        if trade_flg:
            doc_ref.collection('trade_rule').document(RULE_NAME).update({
                "trade_latest_id": bar_list[-1]["end_id"],
                "trade_timestamp": datetime.now()
            })
    doc_ref.collection('trade_rule').document(RULE_NAME).update({
        "judge_latest_id": bar_list[-1]["end_id"],
        "trade_judge_timestamp": datetime.now()
    })
