# os.chdir("./src")

import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from lib import util


class rawDataProcessing():
    def __init__(self, market: str, dl_site: str, raw_data_dir: str = "../data/raw/", out_dir: str = "../data/processing/raw_formart/") -> None:
        """rawデータのフォーマットを統一するクラス。
        インスタンス生成→process_raw_format呼び出しでOKの設計。（process_raw_formatはラッパー関数）

        Args:
            market (str): マーケットの指定（BTC,...）
            dl_site (str): ダウンロード元の設定（GMO,...）
            raw_data_dir (str, optional): rawデータの格納フォルダ. Defaults to "../data/raw/".
            out_dir (str, optional): 加工したものの出力先フォルダ. Defaults to "../data/processing/raw_formart/".
        """
        self.market = market
        self.dl_site = dl_site
        self.raw_data_dir = raw_data_dir
        self.out_dir = out_dir

        self.get_data_func = {
            "BTC_GMO": self._process_raw_format_BTC_GMO
        }

    def process_raw_format(self, from_dt: date, to_dt: date) -> bool:
        """rawデータのフォーマットを統一するラッパー関数。

        Args: 
            from_dt (date):ダウンロードの開始日付
            to_dt (date): ダウンロードの終了日付

        Returns:
            bool: 成功でTrue、失敗でFalse
        """
        return self.get_data_func[
            self.market + "_" + self.dl_site
        ](from_dt, to_dt)

    def _process_raw_format_BTC_GMO(self, from_dt: date, to_dt: date) -> bool:
        """BTC GMOのデータ加工

        Args:
            process_raw_format共通

        Returns:
            process_raw_format共通
        """
        MARKET = "BTC_JPY"
        MARKET_S = "BTC"
        SITE = "GMO"
        RAW_DIR = self.raw_data_dir + f"{SITE}/{MARKET}/"
        OUT_DIR = self.out_dir + f"{SITE}/{MARKET_S}/"
        FILE_NAME_FORMAT = (
            RAW_DIR + "y={y}/m={m:0=2}/{y}{m:0=2}{d:0=2}_{MARKET}.csv.gz"
        )
        OUT_FILE_FORMAT = (
            OUT_DIR + "y={y}/m={m:0=2}/d={d:0=2}/process_raw_format.pkl"
        )

        print(MARKET, RAW_DIR, OUT_DIR, from_dt, to_dt)

        success_flg = True
        date = from_dt
        while date <= to_dt:
            print(date)
            info_dict = dict(MARKET=MARKET, y=date.year,
                             m=date.month, d=date.day,)

            ndate = date + relativedelta(days=1)
            nd_info_dict = dict(MARKET=MARKET, y=ndate.year,
                                m=ndate.month, d=ndate.day,)

            raw_path = FILE_NAME_FORMAT.format(**info_dict)
            nd_raw_path = FILE_NAME_FORMAT.format(**nd_info_dict)
            if not util.get_file_exists(raw_path):
                print(str(date), ": File Not Exists ERROR")
                success_flg = False
                date += relativedelta(days=1)
                continue

            if not util.get_file_exists(nd_raw_path):
                print(str(date), ": Next DT File Not Exists WARN")
                pdf = pd.read_csv(raw_path)
            else:
                pdf = pd.concat(
                    [pd.read_csv(raw_path), pd.read_csv(nd_raw_path)])

            # 売り注文のみにする。（両方入れるとダブルカウントになる予感...）
            pdf_ed = pdf.loc[pdf['side'] == "BUY"].reset_index()

            pdf_ed = pdf_ed.assign(timestamp=pd.to_datetime(
                pdf_ed.timestamp, format="%Y-%m-%d %H:%M:%S.%f"))
            pdf_ed = pdf_ed.assign(date=pdf_ed.timestamp.dt.date)

            pdf_ed = pdf_ed.loc[pdf_ed['date'] == date].reset_index()

            pdf_ed = (
                pdf_ed.loc[:, ["size", "price", "timestamp"]]
                .rename(columns={"size": "volume"})
            )

            out_file_path = OUT_FILE_FORMAT.format(**info_dict)
            util.mkdir(out_file_path)
            pdf_ed.to_pickle(out_file_path)

            date += relativedelta(days=1)
        print("Finish")

        return success_flg


# c = rawDataProcessing("BTC", "GMO")
# c.process_raw_format(date(2022, 5, 1), date(2022, 5, 2))
