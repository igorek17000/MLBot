# os.chdir("./src")
import numpy as np
import copy
import pandas as pd

from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from lib import util
from typing import List


class makeBarProcessing():
    def __init__(self, market: str, dl_site: str, raw_formated_data_dir: str = "../data/processing/raw_formart/", out_dir: str = "../data/processing/bar/") -> None:
        """rawデータのフォーマットを統一するクラス。
        インスタンス生成→process_raw_format呼び出しでOKの設計。（process_raw_formatはラッパー関数）

        Args:
            market (str): マーケットの指定（BTC,...）
            dl_site (str): ダウンロード元の設定（GMO,...）
            raw_formated_data_dir (str, optional): rawデータ(フォーマット済）の格納フォルダ. Defaults to "../data/processing/raw_formart/".
            out_dir (str, optional): 加工したものの出力先フォルダ. Defaults to "../data/processing/bar/".
        """
        self.market = market
        self.dl_site = dl_site
        self.raw_formated_data_dir = raw_formated_data_dir
        self.out_dir = out_dir

        self.BAR_FUNC_DICT = {
            "doll": self._make_doll_bar
        }
        self.FORMATED_FILE_NAME = "process_raw_format.pkl"

    def make_bar(self, from_dt: date, to_dt: date, bar_setting: dict) -> bool:
        """バーを作成するラッパー関数。

        Args:
            from_dt (date):データの開始日付
            to_dt (date): データの終了日付
            bar_setting (dict): バーの設定
                {
                    "type":str ,  # バーの種類("doll",...)
                    "threshold"(option):int , # 例：閾値(作成するバーに応じたオプションを指定)
                }

        Returns:
            bool: 成功でTrue、失敗でFalse
        """
        bar_type = bar_setting["type"]

        args = copy.deepcopy(bar_setting)
        del args["type"]

        return self.BAR_FUNC_DICT[bar_type](from_dt=from_dt, to_dt=to_dt, **args)

    def _write_bar_list_by_dt(self, bar_list: List[dict], out_file_format: str, from_dt: date, to_dt: date) -> None:
        """日付ごとにファイルを分割して出力する関数

        Args:
            bar_list (List[dict]): 出力するbarのリスト
            out_file_format (str): 出力するパスのフォーマット
            from_dt (date): 開始日
            to_dt (date): 終了日
        """
        tg_dt_list = util.create_date_list(from_dt=from_dt, to_dt=to_dt)

        for tg_dt in tg_dt_list:
            tg_dt_bar = [bar for bar in bar_list if bar["date"] == tg_dt]

            out_file_path = out_file_format.format(
                y=tg_dt.year,
                m=tg_dt.month,
                d=tg_dt.day
            )
            util.mkdir(out_file_path)
            pd.DataFrame(tg_dt_bar).to_pickle(out_file_path)

    def _make_doll_bar(self, from_dt: date, to_dt: date, threshold: int) -> bool:
        """ドルバーを作成する関数。

        Args:
            from_dt (date):ダウンロードの開始日付
            to_dt (date): ダウンロードの終了日付
            threshold(int): 閾値

        Returns:
            bool: 成功でTrue、失敗でFalse
        """

        MARKET = self.market
        SITE = self.dl_site
        FORMATED_DIR = self.raw_formated_data_dir + f"{SITE}/{MARKET}/"

        OUT_DIR = self.out_dir + f"{SITE}/{MARKET}/doll/threshold={threshold}/"
        OUT_FILE_FORMAT = (
            OUT_DIR + "bar/y={y}/m={m:0=2}/d={d:0=2}/process_bar.pkl"
        )
        TMP_OUT_FILE_FORMAT = (
            OUT_DIR + "tmp/y={y}/m={m:0=2}/d={d:0=2}/tmp_bar.pkl"
        )

        print(MARKET, FORMATED_DIR, OUT_DIR, from_dt, to_dt)

        pdf, success_flg = util.read_pickle_data_to_df(
            dir_path=FORMATED_DIR, file_name=self.FORMATED_FILE_NAME, from_dt=from_dt, to_dt=to_dt)

        print("■ 一度のトラフィックでの閾値越えを例外処理")
        pre_traffic = pdf.to_numpy()
        traffic = []
        for volume, price, timestamp in pre_traffic:
            sum_price = volume * price
            unit_volume = (volume * threshold) / sum_price  # 1閾値あたりの出来高を計算
            now_volume = volume
            now_sum_price = sum_price

            # 1閾値を超えているものは、複数のレコードに分割する。
            # 出来高を調整して、合計取引額を1閾値以下に調整するイメージ
            while now_sum_price > threshold:

                traffic.append((
                    unit_volume,
                    price,
                    timestamp
                ))
                now_sum_price -= threshold
                now_volume -= unit_volume

            traffic.append((now_volume, price, timestamp))

        # pdf["sum_prece"] = pdf["volume"] * pdf["price"]
        # pdf.to_csv('pre_traffic.csv')

        # pdf2 = pd.DataFrame(traffic)
        # pdf2["sum_prece"] = pdf2[0] * pdf2[1]
        # pdf2.to_csv('traffic.csv')

        print("■ ohlcv形式でバーを作成")
        print("■■ 初期値の設定。前日の記録データがあればそれを使用")
        from_dt_yesterday = from_dt + relativedelta(days=-1)
        tmp_file_path = TMP_OUT_FILE_FORMAT.format(
            y=from_dt_yesterday.year,
            m=from_dt_yesterday.month,
            d=from_dt_yesterday.day
        )
        if util.get_file_exists(tmp_file_path):
            yesterday_tmp = pd.read_pickle(tmp_file_path)
            sum_sum_price = yesterday_tmp.loc[0, "sum_sum_price"]
            sum_volume = yesterday_tmp.loc[0, "volume"]
            open_price = yesterday_tmp.loc[0, "open"]
            min_price = yesterday_tmp.loc[0, "low"]
            max_price = yesterday_tmp.loc[0, "high"]
            today = yesterday_tmp.loc[0, "date"]
        else:
            sum_sum_price = 0
            sum_volume = 0
            open_price = 0
            min_price = 0
            max_price = 0
            today = traffic[0][2].date()

        # 中途半端なレコードを一時記録する関数。使いまわしは出来ない想定。
        def tmp_record():
            tmp_file_path = TMP_OUT_FILE_FORMAT.format(
                y=today.year, m=today.month, d=today.day
            )
            util.mkdir(tmp_file_path)
            pd.DataFrame([{
                "sum_sum_price": sum_sum_price,
                "open": open_price,
                "low": min_price,
                "high": max_price,
                "volume": sum_volume,
                "date": today
            }]).to_pickle(tmp_file_path)

        print("■■ barの作成")
        bar_list = []
        for i, tr in enumerate(traffic):
            volume, price, timestamp = tr

            # 日の変わるタイミング or 最後のレコードの時点で中途半端なものは記録しておく。
            if timestamp.date() > today:
                tmp_record()
                today = timestamp.date()

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

                bar_list.append({
                    "open": open_price,
                    "low": min_price,
                    "high": max_price,
                    "close": price,
                    "volume": sum_volume,
                    "timestamp": timestamp,
                    "date": timestamp.date()
                })
                open_price = price
                min_price = open_price
                max_price = open_price
                sum_sum_price = left_price
                sum_volume = left_volumne

            else:
                sum_volume += volume

            if (i + 1) == len(traffic):
                tmp_record()
                today = timestamp.date()
            # ohlcv = pd.DataFrame(bar_list)
            # ohlcv["sum_price"] = ohlcv["volume"] * ohlcv["close"]
            # ohlcv.to_csv('ohlcv.csv')

        self._write_bar_list_by_dt(
            bar_list=bar_list,
            out_file_format=OUT_FILE_FORMAT,
            from_dt=from_dt,
            to_dt=to_dt
        )
        print("Finish")

        return success_flg


# c = makeBarProcessing("BTC", "GMO")
# c.make_bar(date(2018, 9, 5), date(2018, 9, 10), {
#            "type": "doll", "threshold": 1000000})
