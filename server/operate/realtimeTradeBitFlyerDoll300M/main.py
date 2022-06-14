import os
from firebase_admin import firestore
from firebase_admin import credentials
import firebase_admin
from datetime import datetime, date
from typing import List
from pandas import DataFrame
# import pandas as pd
import numpy as np
import time
import hmac
import hashlib
import requests
import json
# os.chdir("./server/develop")


def trade(event, context):
    judgeGoaldenCrossBitFlyer()


class BitFlyerAPI:
    def __init__(self) -> None:
        self.API_KEY = os.environ["BIT_FLYER_API_KEY"]
        self.API_SECRET = os.environ["BIT_FLYER_API_SECRET"]
        self.BASE_URL = 'https://api.bitflyer.com'

    def header(self, method: str, endpoint: str, body: str = '') -> dict:
        timestamp = str(time.time())
        message = timestamp + method + endpoint + body
        signature = hmac.new(self.API_SECRET.encode('utf-8'), message.encode('utf-8'),
                             digestmod=hashlib.sha256).hexdigest()
        headers = {
            'Content-Type': 'application/json',
            'ACCESS-KEY': self.API_KEY,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-SIGN': signature
        }
        return headers

    def getBalance(self):

        endpoint = '/v1/me/getbalance'

        headers = self.header('GET', endpoint=endpoint, body='')
        response = requests.get(self.BASE_URL + endpoint, headers=headers)
        return response.json()

    def get_ticker(self):
        endpoint = '/v1/ticker'
        product_code = 'btc_jpy'  # ビットコインの場合

        response = requests.get(self.BASE_URL + endpoint, params={"product_code": product_code})
        return response.json()

    def _create_endpoint(self, endpoint, params):
        return endpoint + '?' + '&'.join([k + '=' + v for k, v in params.items()])

    def get_risk(self):
        endpoint = '/v1/me/gettradingcommission'
        product_code = 'btc_jpy'  # ビットコインの場合
        params = {"product_code": product_code}
        endpoint_header = self._create_endpoint(endpoint, params)

        headers = self.header('GET', endpoint=endpoint_header, body='')

        response = requests.get(self.BASE_URL + endpoint_header, headers=headers)
        return response.json()

    def order(self, side='BUY', size=0.001):
        endpoint = "/v1/me/sendchildorder"

        body = {
            "product_code": 'btc_jpy',
            "child_order_type": 'MARKET',
            "side": side,
            "size": size,
            "minute_to_expire": 10,
            "time_in_force": 'GTC'
        }

        body = json.dumps(body)
        headers = self.header('POST', endpoint=endpoint, body=body)

        response = requests.post(self.BASE_URL + endpoint, data=body, headers=headers)
        return response.json()


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


def _buy(doc_ref, bar_list, price, rule_name, risk, balance, api: BitFlyerAPI):
    # 買えないことがあるので、5000円残すようにする。

    if balance <= 5000:
        return False

    buy_volume = ((balance - 5000) / price) * 1000
    buy_volume = np.floor(buy_volume) / 1000
    # buy_volume = 0.001

    api_res = api.order(side="BUY", size=buy_volume)
    print(api_res)

    buy_doc = (
        doc_ref
        .collection('trade_rule')
        .document(rule_name)
        .collection("buy_list")
        .document(str(bar_list[-1]['end_id']))
    )

    buy_dict = {
        "status": "Buy",
        "end_id": bar_list[-1]['end_id'],
        "buy_volume": buy_volume * (1-risk),
        "buy_price": price,
        "buy_timestamp": bar_list[-1]['timestamp'],
        "buy_amount": buy_volume * (1-risk) * price,
        "child_order_acceptance_id": api_res["child_order_acceptance_id"]
    }

    print(buy_dict)
    buy_doc.set(buy_dict)
    return True


def _sell(doc_ref, buy_doc_list, price, rule_name, risk, api: BitFlyerAPI):
    for buy_doc in buy_doc_list:
        sell_doc = (
            doc_ref
            .collection('trade_rule')
            .document(rule_name)
            .collection("buy_list")
            .document(str(buy_doc['end_id']))
        )
        sell_doc_dict = sell_doc.get().to_dict()
        risk_volume = sell_doc_dict["buy_volume"] * (1-risk)
        risk_volume = np.floor(risk_volume * 100000000) / 100000000
        # risk_volume = 0.00199700 * (1-risk)
        if risk_volume < 0.001:
            print('残高不足', risk_volume)
            return False

        api_res = api.order(side="SELL", size=risk_volume)
        print(api_res)

        amount = risk_volume * price * (1-risk)
        sell_return = amount - sell_doc_dict["buy_amount"]

        sell_dict = {
            "status": "Sell",
            "sell_amount": amount,
            "sell_return": sell_return,
            "sell_volume": risk_volume,
            "sell_price": price,
            "sell_timestamp": datetime.now(),
            "child_order_acceptance_id": api_res["child_order_acceptance_id"]
        }
        print(sell_dict)
        sell_doc.update(sell_dict)
    return True


def judgeGoaldenCrossBitFlyer():

    DL_STITE = "bitFlyer"
    FROM_BAR_NAME = 'processing_doll_bar_300000000'
    LONG_MA_N = 18
    SHORT_MA_N = 3
    SELL_TILT_SPAN = 8
    SELL_TILT_THRESHOLD = 22.69003088824373
    PRICE_COL = "close"
    # RISK = 0.05*0.01
    RULE_NAME = "GoaldenCross_300000000"

    read_limit = max(LONG_MA_N + 1, SHORT_MA_N + SELL_TILT_SPAN)

    print(dict(
        DL_STITE=DL_STITE,
        FROM_BAR_NAME=FROM_BAR_NAME,
        LONG_MA_N=LONG_MA_N,
        SHORT_MA_N=SHORT_MA_N,
        SELL_TILT_SPAN=SELL_TILT_SPAN,
        SELL_TILT_THRESHOLD=SELL_TILT_THRESHOLD,
        PRICE_COL=PRICE_COL,
        # RISK=RISK,
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

    bar_list = sorted([
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
    ], key=lambda x: x["end_id"])

    # 既に判定済みであればスキップする。
    rule_doc = doc_ref.collection('trade_rule').document(RULE_NAME).get().to_dict()
    if rule_doc["judge_latest_id"] == bar_list[-1]["end_id"]:
        return True

    doc_ref.collection('trade_rule').document(RULE_NAME).update({
        "judge_latest_id": bar_list[-1]["end_id"],
        "trade_judge_timestamp": datetime.now()
    })

    tg_df = DataFrame(bar_list)
    now_idx = LONG_MA_N

    # now_idx 時点の移動平均
    td_long_ma = _calc_ma_now(now_idx, LONG_MA_N, tg_df, PRICE_COL)
    td_short_ma = _calc_ma_now(now_idx, SHORT_MA_N, tg_df, PRICE_COL)

    # now_idx - 1 時点の移動平均
    yd_long_ma = _calc_ma_now(now_idx-1, LONG_MA_N, tg_df, PRICE_COL)
    yd_short_ma = _calc_ma_now(now_idx-1, SHORT_MA_N, tg_df, PRICE_COL)

    btfAPI = BitFlyerAPI()

    trade_flg = False
    # N-1 時点では long > short & N 時点では long <= short となり、上に抜いたら買うタイミング
    if (yd_long_ma > yd_short_ma) & (td_long_ma <= td_short_ma):

        balance_res = btfAPI.getBalance() * 0.1  # TODO 本番稼働時は変更
        price = btfAPI.get_ticker()["ltp"]
        risk = btfAPI.get_risk()["commission_rate"]

        balance = [d for d in balance_res if d["currency_code"] == "JPY"][0]["amount"]
        trade_flg = _buy(
            doc_ref=doc_ref,
            bar_list=bar_list,
            price=price,
            rule_name=RULE_NAME,
            risk=risk,
            balance=balance,
            api=btfAPI
        )

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
        price = btfAPI.get_ticker()["ltp"]
        risk = btfAPI.get_risk()["commission_rate"]

        if sell_tilt <= SELL_TILT_THRESHOLD:
            trade_flg = _sell(
                doc_ref=doc_ref,
                buy_doc_list=buy_doc_list,
                price=price,
                rule_name=RULE_NAME,
                risk=risk,
                api=btfAPI
            )

    if trade_flg:
        doc_ref.collection('trade_rule').document(RULE_NAME).update({
            "trade_latest_id": bar_list[-1]["end_id"],
            "trade_timestamp": datetime.now()
        })
