from datetime import date
from pandas import DataFrame
import numpy as np
import pandas as pd

from lib.IBackTestSetting import IBackTestSetting
from lib.backTest import BackTest
from lib.context import Context


class GoaldenCrossBackTestSetting(IBackTestSetting):
    def __init__(
        self, rule_name: str, version: str,
        dir_path: str, file_name: str,
        read_from_dt: date, read_to_dt: date, start_dt: date,
        initial_balance: int, price_col: str,
        experiment_name: str = "MLBot",
        _long_ma_n: int = 25, _short_ma_n: int = 5, _buysell_timing: int = 1
    ) -> None:
        """_summary_

        Args:
            ～インターフェース同様なので略～
            _long_ma_n (int, optional): 長期移動平均のN. Defaults to 25.
            _short_ma_n (int, optional): 短期移動平均のN. Defaults to 5.
            _buysell_timing (int, optional): 売買タイミングと判定してから、実際に売買するまでのディレイ. Defaults to 1.
        """

        self.experiment_name = experiment_name
        self.rule_name = rule_name
        self.version = version

        self.dir_path = dir_path
        self.file_name = file_name
        self.read_from_dt = read_from_dt
        self.read_to_dt = read_to_dt

        self.initial_balance = initial_balance
        self.start_dt = start_dt
        self.price_col = price_col

        self._long_ma_n = _long_ma_n
        self._short_ma_n = _short_ma_n

        self._buysell_timing = _buysell_timing

    def get_start_idx(self, ohlcv_data):
        """開始日付の中で最も小さいインデックスを開始インデックスとする

            ～インターフェース同様なので略～
        """
        tgdt_df = ohlcv_data[ohlcv_data["date"] == self.start_dt]
        return np.min(tgdt_df.index)

    def _calc_ma_now(self, td_idx: int, n: int, tg_df: DataFrame, price_col: str) -> float:
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

    def judge_buysell_timing(self, now_idx: int, ohlcv_data: DataFrame, context: Context) -> dict:
        """ゴールデンクロスが生じたら買う。
            下降トレンドに転換したら売る　→　要修正

            ～インターフェース同様なので略～
        """

        # 実際の購買が走るのは次のバーのタイミング
        tg_df = ohlcv_data.iloc[:now_idx+1]

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
        sell_order_list = []

        # N-1 時点では long > short & N 時点では long <= short となり、上に抜いたら買うタイミング
        if (yd_long_ma > yd_short_ma) & (td_long_ma <= td_short_ma):
            buy_flg = True
            buy_idx = now_idx + self._buysell_timing

            # 買えないことがあるので、5000円残すようにする。
            buy_volume = ((context.balance - 5000) / ohlcv_data.iloc[now_idx][self.price_col]) * 1000
            buy_volume = np.floor(buy_volume) / 1000

        # N-1 時点では long < short & N 時点では long >= short となり、下に抜いたら買うタイミング
        elif (len(context.buy_status) > 0) & (yd_long_ma < yd_short_ma) & (td_long_ma >= td_short_ma):
            sell_flg = True
            sell_idx = now_idx + self._buysell_timing
            sell_order_list = [context.buy_status[-1]]

        return dict(
            idx=now_idx, buy_flg=buy_flg, sell_flg=sell_flg,
            buy_idx=buy_idx, sell_idx=sell_idx,
            buy_volume=buy_volume, sell_order_list=sell_order_list,
            evidence=evidence
        )


# context = Context(initial_balance=100000)
bt_stng = GoaldenCrossBackTestSetting(

    rule_name="test",
    version="test",

    dir_path="../data/processing/bar/GMO/BTC/doll/threshold=300000000/bar/",
    file_name="process_bar.pkl",
    read_from_dt=date(2021, 1, 1),
    read_to_dt=date(2022, 5, 13),

    initial_balance=100000,
    start_dt=date(2021, 5, 1),
    price_col="close",
)


bktest = BackTest(back_test_setting=bt_stng)
res = bktest.run_backtest()

# 結果出力
buy_res_list = res["buy_res_list"]
sell_res_list = res["sell_res_list"]
pd.DataFrame(buy_res_list).to_csv('buy_res_list.csv')
pd.DataFrame(sell_res_list).to_csv('sell_res_list.csv')
print(bktest.context.buy_status)
