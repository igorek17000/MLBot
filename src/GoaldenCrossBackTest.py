from itertools import product
from multiprocessing import Pool
from lib.context import Context
from lib.backTest import BackTest
from lib.IBackTestSetting import IBackTestSetting
import pandas as pd
import numpy as np
import shutil
import os
from pandas import DataFrame
from datetime import date

# import os
# os.chdir('src')


class GoaldenCrossBackTestSetting(IBackTestSetting):
    def __init__(
        self, rule_name: str, version: str,
        dir_path: str, file_name: str,
        read_from_dt: date, read_to_dt: date, start_dt: date,
        initial_balance: int, price_col: str,
        experiment_name: str = "MLBot",
        _long_ma_n: int = 25, _short_ma_n: int = 5,
        _sell_tilt_span: int = 1, _sell_tilt_threshold: float = 0.5,
        _buysell_timing: int = 1
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

        self._sell_tilt_span = _sell_tilt_span
        self._sell_tilt_threshold = _sell_tilt_threshold

        self._buysell_timing = _buysell_timing

    def get_mlflow_params(self) -> dict:
        """mlflowに記録するタグを設定

        Returns:
            dict : タグを記載したディクショナリ
        """
        return dict(
            _long_ma_n=self._long_ma_n,
            _short_ma_n=self._short_ma_n,
            _sell_tilt_span=self._sell_tilt_span,
            _sell_tilt_threshold=self._sell_tilt_threshold,
            _buysell_timing=self._buysell_timing
        )

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
           買っている状態で、ショート移動平均の傾きが0に近づいたら売る

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
        elif len(context.buy_status) > 0:

            base_short_ma = self._calc_ma_now(now_idx - self._sell_tilt_span, self._short_ma_n, tg_df, self.price_col)
            sell_tilt = (td_short_ma - base_short_ma) / self._sell_tilt_span

            if sell_tilt <= self._sell_tilt_threshold:
                sell_flg = True
                sell_idx = now_idx + self._buysell_timing
                sell_order_list = [context.buy_status[-1]]

        return dict(
            idx=now_idx, buy_flg=buy_flg, sell_flg=sell_flg,
            buy_idx=buy_idx, sell_idx=sell_idx,
            buy_volume=buy_volume, sell_order_list=sell_order_list,
            evidence=evidence  # TODO これ使うと移動平均値とかを記録できる
        )

    def get_judge_evidence_data(self, res: dict) -> DataFrame:
        """移動平均の値をエビデンスとして出力

        ～～以下、インターフェース同様～～
        """
        judge_res_list = res["res_list"]

        evidence_list = [dict(idx=r["idx"], **r["evidence"]) for r in judge_res_list]
        return pd.DataFrame(evidence_list).set_index('idx')

    def get_judge_evidence_plot_stng(self) -> dict:
        """移動平均のグラフを出力

        ～～以下、インターフェース同様～～
        """
        total_fig_row_num = 4
        evidence_setting = [
            dict(
                fig_row_num=1,
                type='line',
                cols=['td_long_ma', 'td_short_ma']
            )
        ]

        return total_fig_row_num, evidence_setting


def GoaldenCrossBackTest(std_dict):
    print(std_dict)
    bt_stng = GoaldenCrossBackTestSetting(

        rule_name="GoaldenCross",
        version="0.1",
        experiment_name="GoaldenCrossTest2",

        dir_path="../data/processing/bar/GMO/BTC/doll/threshold=300000000/bar/",
        file_name="process_bar.pkl",
        read_from_dt=date(2021, 1, 1),
        read_to_dt=date(2022, 5, 13),

        initial_balance=100000,
        start_dt=date(2021, 5, 1),
        price_col="close",

        **std_dict
    )

    bktest = BackTest(back_test_setting=bt_stng)
    res = bktest.run_backtest()

    return True


if __name__ == "__main__":
    # _long_ma_n_list = [25, 30, 50, 80, 100]
    # _short_ma_n_list = [3, 5, 8, 10, 15]
    # _sell_tilt_span_list = [1, 2, 4]
    # _sell_tilt_threshold_list = [0.5, 0.1, 1, 10, 100]
    # _buysell_timing_list = [1]

    _long_ma_n_list = [50]
    _short_ma_n_list = [10]
    _sell_tilt_span_list = [2]
    _sell_tilt_threshold_list = [1]
    _buysell_timing_list = [1]

    std_dict_list = [
        dict(
            _long_ma_n=lman, _short_ma_n=sman,
            _sell_tilt_span=sts, _sell_tilt_threshold=stt,
            _buysell_timing=bt
        )
        for lman, sman, sts, stt, bt in product(
            _long_ma_n_list, _short_ma_n_list,
            _sell_tilt_span_list, _sell_tilt_threshold_list,
            _buysell_timing_list
        )
    ]
    print(len(std_dict_list))

    tmp_output_path = '../results/output/'
    if not os.path.exists(tmp_output_path):
        os.mkdir(tmp_output_path)

    # GoaldenCrossBackTest(std_dict_list[0])

    with Pool(20) as p:
        result = p.map(GoaldenCrossBackTest, std_dict_list)
        print(result)

    # outputフォルダを削除
    shutil.rmtree(tmp_output_path)
