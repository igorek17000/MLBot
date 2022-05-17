# import os
# os.chdir("src")

from typing import Tuple
from mlflow.entities import Experiment, Run
from numpy import float64
from lib import util
from lib.context import Context
from lib.IBackTestSetting import IBackTestSetting


import mlflow

# # 設定


class Order():
    def __init__(self, type: str, idx: int, volume: float, price: float) -> None:
        self.type = type
        self.idx = idx
        self.volume = volume
        self.price = price
        self.amount = volume * price
        pass


class BackTest:
    """_summary_
    """

    def __init__(self, back_test_setting: IBackTestSetting) -> None:
        """_summary_

        Args:
            experiment_name (str): mlflowの実験名。MLBotがデフォルト
            rule_name (str): 検証するルールの名称
            version (str): ルールのバージョン
        """
        self.initial_balance = back_test_setting.initial_balance
        self.experiment_name = back_test_setting.experiment_name
        self.rule_name = back_test_setting.rule_name
        self.version = back_test_setting.version
        self.context = Context(initial_balance=back_test_setting.initial_balance)
        self.back_test_setting = back_test_setting

    def _start_mlflow(self) -> Tuple[Experiment, Run]:
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

    def _finish_mlflow(self):
        mlflow.end_run()

    def _buy(self, context: Context, now_idx: int, volume: float64, price: float64):

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

    def _sell(self, context: Context, now_idx: int, price: float64):

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

    def run_backtest(self):

        # 設定
        bt_stng = self.back_test_setting

        # データ読み込み
        ohlcv_data, success_flg = util.read_pickle_data_to_df(
            dir_path=bt_stng.dir_path, file_name=bt_stng.file_name,
            from_dt=bt_stng.read_from_dt, to_dt=bt_stng.read_to_dt
        )
        ohlcv_data = ohlcv_data.reset_index().drop("index", axis=1)
        start_idx = bt_stng.get_start_idx(ohlcv_data)

        res_list = []
        buy_res_list = []
        sell_res_list = []
        next_buy_idx = None
        next_sell_idx = None
        latest_buy_judge_res = None
        for idx in range(start_idx, len(ohlcv_data)):
            # 購買する場合
            if idx == next_buy_idx:
                buy_res = self._buy(
                    context=self.context, now_idx=idx,
                    volume=latest_buy_judge_res["buy_volume"], price=ohlcv_data.loc[idx][bt_stng.price_col]
                )
                next_buy_idx = None
                latest_buy_judge_res = None
                buy_res_list.append(buy_res)

            # 売却する場合
            elif idx == next_sell_idx:
                sell_res = self._sell(
                    context=self.context, now_idx=idx,
                    price=ohlcv_data.loc[idx][bt_stng.price_col]
                )
                next_sell_idx = None
                sell_res_list.append(sell_res)

            # 次の売買タイミングの取得
            res = bt_stng.judge_buysell_timing(now_idx=idx, ohlcv_data=ohlcv_data, context=self.context)
            res_list.append(res)

            # 次の売買タイミングを指示
            if res["buy_flg"]:
                next_buy_idx = res["buy_idx"]
                latest_buy_judge_res = res
            elif res["sell_flg"]:
                next_sell_idx = res["sell_idx"]

        # mlflow.log_figure(fig, "figure.png")
        # mlflow.log_param("k", list(range(2, 50)))
        # mlflow.log_metric("Silhouette Score", score, step=i)

        return dict(
            success_flg=success_flg,
            res_list=res_list,
            buy_res_list=buy_res_list,
            sell_res_list=sell_res_list
        )
