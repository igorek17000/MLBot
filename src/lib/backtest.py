# import os
# os.chdir("src")

from typing import Tuple, List
from mlflow.entities import Experiment, Run
from numpy import float64
from lib import util
from lib.context import Context
from lib.IBackTestSetting import IBackTestSetting

import mlflow

# # 設定


class Order():
    def __init__(self, type: str, idx: int, volume: float, price: float) -> None:
        """オーダークラス

        Args:
            type (str): 売買タイプ（買い："buy"、売り："sell"）
            idx (int): オーダーが発生するidx
            volume (float): オーダーしたvolume
            price (float): オーダー時の価格
        """
        self.type = type
        self.idx = idx
        self.volume = volume
        self.price = price

        # 売買金額
        self.amount = volume * price
        pass


class BackTest:
    def __init__(self, back_test_setting: IBackTestSetting) -> None:
        """バックテストを実行するクラス

        Args:
            back_test_setting (IBackTestSetting): バックテストの設定インスタンス。IBackTestSettingを継承しているクラスであること。
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
        mlflow.set_tracking_uri('../results/mlruns/')
        mlflow.set_experiment(self.experiment_name)
        experiment = mlflow.get_experiment_by_name(self.experiment_name)

        mlflow.start_run(experiment_id=experiment.experiment_id)
        run = mlflow.active_run()

        bt_stng = self.back_test_setting
        mlflow.set_tags(
            dict(
                rule_name=self.rule_name,
                version=self.version,
                read_from_dt=bt_stng.read_from_dt,
                read_to_dt=bt_stng.read_to_dt,
                dir_path=bt_stng.dir_path,
                file_name=bt_stng.file_name,
                price_col=bt_stng.price_col,
            )
        )
        mlflow.log_params(
            dict(
                initial_balance=bt_stng.initial_balance,
                start_dt=bt_stng.start_dt,
            )
        )

        return experiment, run

    def _finish_mlflow(self):
        """MLFlowを終了する。
        """
        mlflow.end_run()

    def _buy(self, context: Context, now_idx: int, volume: float64, price: float64) -> dict:
        """買う関数

        Args:
            context (Context): コンテキスト
            now_idx (int): 現時点のidx
            volume (float64): 買うボリューム
            price (float64): 現時点の価格

        Returns:
            dict: 購買結果
            dict(
                idx : 現時点のインデックス
                buy_success_flg :買いが成功したフラグ
                amount :購買金額
                balance :現時点の残高
                total_buy_count :現時点での合計購買回数
                total_buy_amount :現時点での合計購買金額
            )
        """
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

    def _sell(self, context: Context, now_idx: int, sell_order_list: List[Order], price: float64) -> List[dict]:
        """売る関数

        Args:
            context (Context): コンテキスト
            now_idx (int): 現時点のインデックス
            sell_order_list (List[Order]): 売りたいオーダーのリスト
            price (float64): 現時点の価格

        Returns:
            List[dict]: 売った結果のリスト
            dict(
                idx : 現時点のインデックス
                buy_idx : 購入時のオーダーのインデックス
                sell_success_flg :売り成功フラグ
                amount :売却金額
                sell_return :対象の購入オーダーに対するリターン
                balance :残高
                total_sell_count :現時点の合計売却回数
                total_sell_amount :現時点の合計売却金額
                total_return_amount :現時点のトータルリターン
            )
        """
        sell_success_flg = False
        buy_idx = None
        amount = 0
        sell_return = 0

        selled_order_list = []
        for sell_order in sell_order_list:

            latest_buy = sell_order
            buy_idx = latest_buy.idx
            amount = latest_buy.volume * price
            sell_return = amount - latest_buy.amount

            context.balance += amount
            context.total_sell_count += 1
            context.total_sell_amount += amount
            context.total_return_amount += sell_return
            sell_success_flg = True

            selled_order_list.append(
                dict(
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
            )
            context.buy_status.remove(sell_order)

        return selled_order_list

    def run_backtest(self) -> dict:
        """バックテストを実行する関数

        Returns:
            dict: 実行結果
            dict(
                success_flg :データ読み込み成功フラグ
                res_list :バーごとの判定結果のリスト
                buy_res_list :購買結果のリスト
                sell_res_list :売却結果のリスト
            )
        """
        # 設定
        bt_stng = self.back_test_setting
        experiment, run = self._start_mlflow()
        mlflow.log_params(bt_stng.get_mlflow_params())

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
        latest_sell_judge_res = None
        for idx in range(start_idx, len(ohlcv_data)):
            # this_return = 0

            # 購買する場合
            if idx == next_buy_idx:
                buy_res = self._buy(
                    context=self.context, now_idx=idx,
                    volume=latest_buy_judge_res["buy_volume"],
                    price=ohlcv_data.loc[idx][bt_stng.price_col]
                )
                next_buy_idx = None
                latest_buy_judge_res = None
                buy_res_list.append(buy_res)

            # 売却する場合
            elif idx == next_sell_idx:
                sell_res = self._sell(
                    context=self.context, now_idx=idx,
                    sell_order_list=latest_sell_judge_res["sell_order_list"],
                    price=ohlcv_data.loc[idx][bt_stng.price_col]
                )
                next_sell_idx = None
                latest_sell_judge_res = None
                sell_res_list += sell_res

                # this_return = sum([s["sell_return"] for s in sell_res])

            # 次の売買タイミングの取得
            res = bt_stng.judge_buysell_timing(now_idx=idx, ohlcv_data=ohlcv_data, context=self.context)
            res_list.append(res)

            # 次の売買タイミングを指示
            if res["buy_flg"]:
                next_buy_idx = res["buy_idx"]
                latest_buy_judge_res = res
            elif res["sell_flg"]:
                next_sell_idx = res["sell_idx"]
                latest_sell_judge_res = res

            mlflow.log_metrics(dict(
                total_return_amount=self.context.total_return_amount,
                # this_return=this_return,
                # balance=self.context.balance,
                # total_buy_count=self.context.total_buy_count,
                # total_sell_count=self.context.total_sell_count,
                # total_buy_amount=self.context.total_buy_amount,
                # total_sell_amount=self.context.total_sell_amount,
                now_price=ohlcv_data.loc[idx][bt_stng.price_col],
            ), step=idx)

        # mlflow.log_figure(fig, "figure.png")
        # mlflow.log_param("k", list(range(2, 50)))
        # mlflow.log_metric("Silhouette Score", score, step=i)

        mlflow.log_metrics(dict(
            final_total_return=self.context.total_return_amount,
            final_total_return_rate=self.context.total_return_amount / bt_stng.initial_balance,
            period=(bt_stng.read_to_dt - bt_stng.start_dt).days,
            total_win_count=len([1 for s in sell_res_list if s["sell_return"] >= 0]),
            total_lose_count=len([1 for s in sell_res_list if s["sell_return"] < 0]),
        ))

        self._finish_mlflow()

        return dict(
            success_flg=success_flg,
            res_list=res_list,
            buy_res_list=buy_res_list,
            sell_res_list=sell_res_list
        )
