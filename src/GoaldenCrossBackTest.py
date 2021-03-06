from itertools import product
from multiprocessing import Pool
from lib.context import Context
from lib.backTest import BackTest
from lib.IBackTestSetting import IBackTestSetting
import pandas as pd
import numpy as np
import shutil
import os
import optuna
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
        risk: float,
        experiment_name: str = "MLBot", mlflow_db_name: str = "mlflow_db",
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
        self.mlflow_db_name = mlflow_db_name
        self.rule_name = rule_name
        self.version = version

        self.dir_path = dir_path
        self.file_name = file_name
        self.read_from_dt = read_from_dt
        self.read_to_dt = read_to_dt

        self.initial_balance = initial_balance
        self.start_dt = start_dt
        self.price_col = price_col
        self.risk = risk

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

        # shortの傾きが一定値以下になったら売り
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
        # version="BitFlyer",
        experiment_name="GoaldenCrossBTF_kai_pmax",

        # dir_path="../data/processing/bar/GMO/BTC/doll/threshold=300000000/bar/",
        dir_path="../data/processing/bar/bitFlyer/BTC/doll/threshold=300000000/bar/",
        file_name="process_bar.pkl",
        read_from_dt=date(2022, 5, 11),
        # read_from_dt=date(2018, 10, 1),
        read_to_dt=date(2022, 6, 17),

        initial_balance=100000,
        start_dt=date(2022, 5, 20),
        # start_dt=date(2019, 1, 1),
        price_col="close",

        risk=0.0015,

        ** std_dict
    )

    bktest = BackTest(back_test_setting=bt_stng)
    res = bktest.run_backtest()

    return 1 / res["final_metric"]["sell_rtn_p_value"]


def optuna_trial(trial):
    std_dict = dict(
        _long_ma_n=trial.suggest_int("_long_ma_n", 5, 50),
        _short_ma_n=trial.suggest_int("_short_ma_n", 1, 50),
        _sell_tilt_span=trial.suggest_int("_sell_tilt_span", 1, 30),
        _sell_tilt_threshold=trial.suggest_int("_sell_tilt_threshold", 0, 150, step=10),
        _buysell_timing=1
    )

    if std_dict["_long_ma_n"] <= std_dict["_short_ma_n"]:
        return (std_dict["_short_ma_n"] - std_dict["_long_ma_n"]) * (-100)

    return GoaldenCrossBackTest(std_dict)


def oputuna_run(idx):
    print(idx)
    study_name = "GoaldenCrossBTF_kai4_pmax"
    oputuna_db_name = "oputuna_db"
    postgre_user = os.environ.get('MLFLOW_POSTGRE_USER')
    postgre_pass = os.environ.get('MLFLOW_POSTGRE_PASS')
    study = optuna.load_study(
        study_name=study_name, storage=f"postgresql://{postgre_user}:{postgre_pass}@localhost/{oputuna_db_name}"
    )
    # study = optuna.create_study(direction='maximize')
    study.optimize(optuna_trial, n_trials=3)
    print(study.best_params)

    return study.best_params


if __name__ == "__main__":

    tmp_output_path = '../results/output/'
    if not os.path.exists(tmp_output_path):
        os.mkdir(tmp_output_path)

    with Pool(20) as p:
        result = p.map(oputuna_run, range(1000))

    # std_dict = dict(
    #     _long_ma_n=18,
    #     _short_ma_n=3,
    #     _sell_tilt_span=8,
    #     _sell_tilt_threshold=22.69003088824373,
    #     _buysell_timing=1
    # )
    # res = GoaldenCrossBackTest(std_dict)

    # outputフォルダを削除
    shutil.rmtree(tmp_output_path)
