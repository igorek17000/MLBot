from time import sleep
from datetime import datetime, date
import requests
import os
from dateutil.relativedelta import relativedelta
from sqlalchemy import true


class getRawData():
    def __init__(self, market: str, dl_site: str, dl_dir: str = "../data/raw/") -> None:
        """rawデータを取得するクラス。
        インスタンス生成→get_date呼び出しでOKの設計。（get_dataはラッパー関数）

        Args:
            market (str): マーケットの指定（BTC,...）
            dl_site (str): ダウンロード元の設定（GMO,...）
            dl_dir (str, optional): ダウウンロード先のフォルダ. Defaults to "../data/raw/".
        """
        self.market = market
        self.dl_site = dl_site
        self.dl_dir = dl_dir

        self.get_data_func = {
            "BTC_GMO": self.get_BTC_raw_data_from_GMO
        }

    def get_data(self, from_dt: date, to_dt: date) -> bool:
        """rawデータダウンロードのラッパー関数。

        Args:
            from_dt (date):ダウンロードの開始日付
            to_dt (date): ダウンロードの終了日付

        Returns:
            bool: 成功でTrue、失敗でFalse
        """
        return self.get_data_func[
            self.market + "_" + self.dl_site
        ](from_dt, to_dt)

    def get_BTC_raw_data_from_GMO(self, from_dt: date, to_dt: date) -> bool:
        """BTC GMOのデータダウンロード

        Args:
            get_data共通

        Returns:
            get_data共通
        """

        MARKET = "BTC_JPY"
        DL_DIR = self.dl_dir + "GMO/"
        FROM_URL = 'https://api.coin.z.com/data/trades/'
        START_DT = from_dt
        TO_DATE = to_dt

        print(MARKET, DL_DIR, START_DT, TO_DATE)

        success_flg = True
        date = START_DT
        while date <= TO_DATE:

            info_dict = dict(
                mkt=MARKET,
                y=date.year,
                m=date.month,
                d=date.day,
            )

            file_name = '{mkt}/{y}/{m:02}/{y}{m:02}{d:02}_{mkt}.csv.gz'.format(
                **info_dict
            )

            url = FROM_URL + file_name
            print(url)

            sleep(1.5)
            # url = "https://api.coin.z.com/data/trades/BTC/2022/05/20220514_BTC.csv.gz"
            res = requests.get(url)

            if res.status_code == 403:
                print(str(date), ": 403 ERROR")
                success_flg = False
                date += relativedelta(days=1)
                continue

            dl_file_name = '{mkt}/y={y}/m={m:02}/{y}{m:02}{d:02}_{mkt}.csv.gz'.format(
                **info_dict
            )
            dl_dir_fllpath = os.path.dirname(DL_DIR + dl_file_name)

            if not os.path.exists(dl_dir_fllpath):
                os.makedirs(dl_dir_fllpath)

            urlData = res.content
            with open(DL_DIR + dl_file_name, mode='wb') as f:  # wb でバイト型を書き込める
                f.write(urlData)

            date += relativedelta(days=1)

        return success_flg
