# import os
# os.chdir("src")

from typing import Tuple, List
from mlflow.entities import Experiment, Run
from numpy import float64
from pandas import DataFrame
from scipy import stats
from lib import util, plot
from lib.context import Context
from lib.IBackTestSetting import IBackTestSetting
import pandas as pd
import numpy as np
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
            buy_price=price,
            buy_volume=volume,
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
                    sell_price=price,
                    sell_volume=latest_buy.volume,
                    sell_return=sell_return,
                    balance=context.balance,
                    total_sell_count=context.total_sell_count,
                    total_sell_amount=context.total_sell_amount,
                    total_return_amount=context.total_return_amount,
                )
            )
            context.buy_status.remove(sell_order)

        return selled_order_list

    def _output_dataframe_to_csv_mflow(self, df: DataFrame, df_name: str) -> str:
        """DataFrameをCSV出力してmlflowで記録

        Args:
            df (DataFrame): 出力したいdataフレーム
            df_name (str): ファイル名（拡張子なし）

        Returns:
            str: 出力パス
        """
        path = f"../results/output/{df_name}.csv"
        df.to_csv(path)
        mlflow.log_artifact(path)

        return path

    def _create_step_output_data(self, return_dict: dict) -> DataFrame:
        """ステップ（インデックスidx）ごとの各種値をDataFrameとして作成

        Args:
            return_dict (dict): バックテストの最終リターンのdict

        Returns:
            DataFrame: ステップidxをインデックスとしたdataフレーム
        """
        res = return_dict

        start_idx = res["start_idx"]
        ohlcv_data = res["ohlcv_data"]
        judge_res_list = res["res_list"]
        buy_res_list = res["buy_res_list"]
        sell_res_list = res["sell_res_list"]
        metric_list = res["metric_list"]

        # 売買判定結果
        judge_res = pd.DataFrame(judge_res_list)
        judge_res = (
            judge_res
            .assign(sell_order_count=judge_res["sell_order_list"].str.len())
            .rename(
                columns={
                    "buy_flg": "buy_judge_flg",
                    "sell_flg": "sell_judge_flg",
                }
            )
            .loc[:, ["idx", "buy_judge_flg", "sell_judge_flg", "sell_order_count"]]
            .set_index('idx')
        )

        # 購買結果
        buy_res = pd.DataFrame(buy_res_list)
        buy_res = (
            buy_res[buy_res["buy_success_flg"] == True]
            .rename(columns={"amount": "buy_amount", })
            .loc[:, ["idx", "buy_amount", "buy_price", "buy_volume", "buy_success_flg"]]
            .set_index('idx')
        )

        # 売却結果
        sell_res = pd.DataFrame(sell_res_list)
        sell_res = (
            sell_res[sell_res["sell_success_flg"] == True]
            .rename(columns={"amount": "sell_amount", })
            .loc[:, ["idx", "sell_amount", "sell_price", "sell_volume", "sell_return"]]
        )
        sell_res = (
            sell_res
            .groupby(["idx"]).agg('sum')
            .reset_index()
            .set_index('idx')
        )

        # 出力指標
        metric = (
            pd.DataFrame(metric_list)
            .loc[:, ["idx", "total_return_amount", "balance"]]
            .set_index('idx')
        )

        # 購買判定のエビデンス
        evidence = self.back_test_setting.get_judge_evidence_data(res)

        # 結合
        ohlcv_data_ed = (
            ohlcv_data
            .iloc[start_idx:]
            .join(judge_res, how='left')
            .join(buy_res, how='left')
            .join(sell_res, how='left')
            .join(metric, how='left')
            .join(evidence, how='left')
            .assign(win_flg=lambda x: x.sell_return >= 0)
            .fillna({"buy_success_flg": False})
        )

        # 勝率推移
        ohlcv_data_win_cumsum = (
            ohlcv_data_ed
            .loc[:, ["win_flg", "buy_success_flg"]]
            .cumsum()
            .rename(columns={"win_flg": "win_count", "buy_success_flg": "buy_count", })
            .assign(win_rate=lambda x: x.win_count / x.buy_count)
        )

        # 最終アウトプット
        ohlcv_data_ed = (
            ohlcv_data_ed
            .join(ohlcv_data_win_cumsum, how='left')
        )

        return ohlcv_data_ed

    def _create_buy_order_output_data(self, return_dict: dict) -> Tuple[DataFrame, dict]:
        res = return_dict

        buy_res_list = res["buy_res_list"]
        sell_res_list = res["sell_res_list"]

        buy_res = pd.DataFrame(buy_res_list)
        sell_res = pd.DataFrame(sell_res_list)

        buy_res = (
            buy_res[buy_res["buy_success_flg"] == True]
            .rename(columns={"amount": "buy_amount", })
            .loc[:, ["idx", "buy_amount", "buy_price", "buy_volume", "buy_success_flg"]]
            .set_index('idx')
        )
        sell_res = (
            sell_res[sell_res["sell_success_flg"] == True]
            .loc[:, ["buy_idx", "amount", "sell_price", "sell_volume", "sell_return"]]
            .rename(columns={"amount": "sell_amount", "buy_idx": "idx"})
            .set_index('idx')
        )

        buy_res_joined = (
            buy_res
            .join(sell_res, how='left')
            .assign(retur_rate=lambda x: (x.sell_amount / x.buy_amount) - 1)
        )

        return_rate_list = buy_res_joined["retur_rate"].to_numpy()
        return_rate_mean = np.mean(return_rate_list)  # 期待値
        return_rate_std = np.std(return_rate_list, ddof=1)  # 不偏標準偏差
        shape_ratio = return_rate_mean / return_rate_std  # 無リスク資産の収益率は0とする。

        sell_return_list = buy_res_joined["sell_return"].to_numpy()
        sell_return_mean = np.mean(sell_return_list)  # 期待値
        sell_return_std = np.std(sell_return_list, ddof=1)  # 不偏標準偏差

        # 帰無仮説　リターンの期待値は、0よりも小さい
        t_res = stats.ttest_1samp(sell_return_list, 0.0, alternative="less")
        t_value = t_res.statistic
        p_value = t_res.pvalue

        t_result = None
        if p_value <= 0.005:
            t_result = "0.5%有意"
        elif p_value <= 0.01:
            t_result = "1%有意"
        elif p_value <= 0.05:
            t_result = "5%有意"
        else:
            t_result = "有意でない"

        return buy_res_joined, dict(
            sell_rtn_mean=sell_return_mean,
            sell_rtn_std=sell_return_std,
            sell_rtn_t_value=t_value,
            sell_rtn_p_value=p_value,
            sell_rtn_t_result=t_result,
            return_rate_mean=return_rate_mean,
            return_rate_std=return_rate_std,
            shape_ratio=shape_ratio
        )

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
        metric_list = []
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
            metric_dict = dict(
                idx=idx,
                total_return_amount=self.context.total_return_amount,
                # this_return=this_return,
                balance=self.context.balance,
                # total_buy_count=self.context.total_buy_count,
                # total_sell_count=self.context.total_sell_count,
                # total_buy_amount=self.context.total_buy_amount,
                # total_sell_amount=self.context.total_sell_amount,
                # now_price=ohlcv_data.loc[idx][bt_stng.price_col],
            )
            # mlflow.log_metrics(metric_dict, step=idx) # ここの記録をやめるだけで、6分→30秒になる...。なんということだ。
            metric_list.append(metric_dict)

        # mlflow.log_figure(fig, "figure.png")
        # mlflow.log_param("k", list(range(2, 50)))
        # mlflow.log_metric("Silhouette Score", score, step=i)

        return_dict = dict(
            ohlcv_data=ohlcv_data,
            start_idx=start_idx,
            success_flg=success_flg,
            res_list=res_list,
            buy_res_list=buy_res_list,
            sell_res_list=sell_res_list,
            metric_list=metric_list,
        )

        step_output_data = self._create_step_output_data(return_dict)
        res_path = self._output_dataframe_to_csv_mflow(step_output_data, "step_output_data")

        buy_order_output_data, statistic_dict = self._create_buy_order_output_data(return_dict)
        res_path = self._output_dataframe_to_csv_mflow(buy_order_output_data, "buy_order_output_data")

        final_win_count = step_output_data.iloc[-1]["win_count"]
        final_buy_count = step_output_data.iloc[-1]["buy_count"]
        final_win_rate = step_output_data.iloc[-1]["win_rate"]

        sell_rtn_t_result = statistic_dict.pop("sell_rtn_t_result")
        final_metric = dict(
            final_total_return=self.context.total_return_amount,
            final_total_return_rate=self.context.total_return_amount / bt_stng.initial_balance,
            period=(bt_stng.read_to_dt - bt_stng.start_dt).days,
            total_win_count=final_win_count,
            final_buy_count=final_buy_count,
            final_win_rate=final_win_rate,
            **statistic_dict,
        )
        mlflow.log_metrics(final_metric)
        mlflow.set_tag("sell_rtn_t_result", sell_rtn_t_result)

        return_dict.update(dict(
            final_metric=final_metric,
            step_output_data=step_output_data,
            buy_order_output_data=buy_order_output_data,
        ))

        step_fig = plot.create_step_backtest_results(return_dict, bt_stng)
        mlflow.log_figure(step_fig, "step_fig.html")

        self._finish_mlflow()
        return return_dict
