# import os
# os.chdir("src")

from typing import Tuple
from mlflow.entities import Experiment, Run
from datetime import date, datetime
from typing import List
from pandas import DataFrame
from numpy import float64
from lib import util


import mlflow
import numpy as np
import pandas as pd

# # 設定


class Order():
    def __init__(self, type: str, idx: int, volume: float, price: float) -> None:
        self.type = type
        self.idx = idx
        self.volume = volume
        self.price = price
        self.amount = volume * price
        pass


class Context:
    def __init__(self, initial_balance) -> None:
        self.balance = initial_balance

        self.buy_status = []
        self.total_buy_count = 0
        self.total_sell_count = 0
        self.total_buy_amount = 0
        self.total_sell_amount = 0
        self.total_return_amount = 0
        pass


class BackTest:
    """_summary_
    """

    def __init__(self, rule_name: str, version: str, experiment_name: str = "MLBot") -> None:
        """_summary_

        Args:
            experiment_name (str): mlflowの実験名。MLBotがデフォルト
            rule_name (str): 検証するルールの名称
            version (str): ルールのバージョン
        """
        self.experiment_name = experiment_name
        self.rule_name = rule_name
        self.version = version
        self.context = Context()

    def start_mlflow(self) -> Tuple[Experiment, Run]:
        """MLFlowを開始する

        Returns:
            Tuple[Experiment, Run]
        """
        mlflow.set_experiment(self.experiment_name)
        experiment = mlflow.get_experiment_by_name(self.experiment_name)

        mlflow.start_run(experiment_id=experiment.experiment_id)
        run = mlflow.active_run()

        mlflow.set_tags(
            {
                "rule_name": self.rule_name,
                "version": self.version,
            }
        )

        return experiment, run

    def finish(self):
        mlflow.end_run()

    def run_backtest(self):
        pass

        # mlflow.log_figure(fig, "figure.png")
        # mlflow.log_param("k", list(range(2, 50)))
        # mlflow.log_metric("Silhouette Score", score, step=i)


class BackTestSetting():
    def __init__(self) -> None:
        self.dir_path = "../data/processing/bar/GMO/BTC/doll/threshold=300000000/bar/"
        self.file_name = "process_bar.pkl"
        self.read_from_dt = date(2021, 1, 1)
        self.read_to_dt = date(2022, 5, 13)

        self.start_dt = date(2021, 5, 1)
        self.price_col = "close"

        self._long_ma_n = 25
        self._short_ma_n = 5

        self._buysell_timing = 1

    def get_start_idx(self, ohlcv_data):
        tgdt_df = ohlcv_data[ohlcv_data["date"] == self.start_dt]
        return np.min(tgdt_df.index)

    def _calc_ma_now(self, td_idx: int, n: int, tg_df: DataFrame, price_col: str) -> float:
        return np.mean(tg_df.iloc[td_idx - n + 1:td_idx+1][price_col])

    def judge_buysell_timing(self, now_idx: int, ohlcv_data: DataFrame, context: Context) -> dict:
        # 実際の購買が走るのは次のバーのタイミング
        tg_df = ohlcv_data.iloc[:now_idx+1]

        # price_col = "close"

        # now_idx 時点の移動平均
        td_long_ma = self._calc_ma_now(now_idx, self._long_ma_n, tg_df, self.price_col)
        td_short_ma = self._calc_ma_now(now_idx, self._short_ma_n, tg_df, self.price_col)

        # now_idx - 1 時点の移動平均
        yd_long_ma = self._calc_ma_now(now_idx-1, self._long_ma_n, tg_df, self.price_col)
        yd_short_ma = self._calc_ma_now(now_idx-1, self._short_ma_n, tg_df, self.price_col)

        evidence = dict(
            td_long_ma=td_long_ma, td_short_ma=td_short_ma,
            yd_long_ma=yd_long_ma, yd_short_ma=yd_short_ma,
        )

        buy_flg = False
        sell_flg = False
        buy_idx = None
        sell_idx = None
        buy_volume = 0

        # N-1 時点では long > short & N 時点では long <= short となり、上に抜いたら買うタイミング
        if (yd_long_ma > yd_short_ma) & (td_long_ma <= td_short_ma):
            buy_flg = True
            buy_idx = now_idx + self._buysell_timing

            # 買えないことがあるので、5000円残すようにする。
            buy_volume = ((context.balance - 5000) / ohlcv_data.iloc[now_idx][self.price_col]) * 1000
            buy_volume = np.floor(buy_volume) / 1000

        # N-1 時点では long < short & N 時点では long >= short となり、下に抜いたら買うタイミング
        elif (yd_long_ma < yd_short_ma) & (td_long_ma >= td_short_ma):
            sell_flg = True
            sell_idx = now_idx + self._buysell_timing

        return dict(
            idx=now_idx, buy_flg=buy_flg, sell_flg=sell_flg,
            buy_idx=buy_idx, sell_idx=sell_idx, buy_volume=buy_volume, evidence=evidence)


def buy(context: Context, now_idx: int, volume: float64, price: float64):

    amount = volume * price

    buy_success_flg = False
    if (context.balance - amount) >= 0:
        context.buy_status.append(
            Order(type="Buy", idx=now_idx, volume=volume, price=price)
        )
        context.balance -= amount
        context.total_buy_count += 1
        context.total_buy_amount += amount
        buy_success_flg = True

    return dict(
        idx=now_idx,
        buy_success_flg=buy_success_flg,
        amount=amount,
        balance=context.balance,
        total_buy_count=context.total_buy_count,
        total_buy_amount=context.total_buy_amount,
    )


def sell(context: Context, now_idx: int, price: float64):

    sell_success_flg = False
    buy_idx = None
    amount = 0
    sell_return = 0
    if len(context.buy_status) > 0:
        latest_buy = context.buy_status.pop(-1)
        buy_idx = latest_buy.idx
        amount = latest_buy.volume * price
        sell_return = amount - latest_buy.amount

        context.balance += amount
        context.total_sell_count += 1
        context.total_sell_amount += amount
        context.total_return_amount += sell_return
        sell_success_flg = True

    return dict(
        idx=now_idx,
        buy_idx=buy_idx,
        sell_success_flg=sell_success_flg,
        amount=amount,
        sell_return=sell_return,
        balance=context.balance,
        total_sell_count=context.total_sell_count,
        total_sell_amount=context.total_sell_amount,
        total_return_amount=context.total_return_amount,
    )


context = Context(initial_balance=100000)
bt_stng = BackTestSetting()

# ここからバックテスト開始
ohlcv_data, success_flg = util.read_pickle_data_to_df(
    dir_path=bt_stng.dir_path, file_name=bt_stng.file_name, from_dt=bt_stng.read_from_dt, to_dt=bt_stng.read_to_dt)
ohlcv_data = ohlcv_data.reset_index().drop("index", axis=1)

start_idx = bt_stng.get_start_idx(ohlcv_data)

res_list = []
buy_res_list = []
sell_res_list = []
next_buy_idx = None
next_sell_idx = None
latest_buy_judge_res = None
for idx in range(start_idx, len(ohlcv_data)):
    if idx == next_buy_idx:
        buy_res = buy(
            context=context, now_idx=idx,
            volume=latest_buy_judge_res["buy_volume"], price=ohlcv_data.loc[idx][bt_stng.price_col]
        )
        next_buy_idx = None
        latest_buy_judge_res = None
        buy_res_list.append(buy_res)
    elif idx == next_sell_idx:
        sell_res = sell(
            context=context, now_idx=idx,
            price=ohlcv_data.loc[idx][bt_stng.price_col]
        )
        next_sell_idx = None
        sell_res_list.append(sell_res)

    res = bt_stng.judge_buysell_timing(now_idx=idx, ohlcv_data=ohlcv_data, context=context)
    res_list.append(res)

    if res["buy_flg"]:
        next_buy_idx = res["buy_idx"]
        latest_buy_judge_res = res
    elif res["sell_flg"]:
        next_sell_idx = res["sell_idx"]

# 結果出力
pd.DataFrame(buy_res_list).to_csv('buy_res_list.csv')
pd.DataFrame(sell_res_list).to_csv('sell_res_list.csv')
context.buy_status
