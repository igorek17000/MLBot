import os
from firebase_admin import firestore
from firebase_admin import credentials
import firebase_admin
from datetime import datetime, date
from typing import List
from lib import util
import pandas as pd


class downloadFireStore:
    def __init__(self, market: str, dl_site: str, from_bar_name: str, out_dir: str = "../data/processing/bar/") -> None:

        self.db_name = dl_site
        self.market = market
        self.from_bar_name = from_bar_name
        self.out_dir = out_dir

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
            if len(tg_dt_bar) == 0:
                continue

            out_file_path = out_file_format.format(
                y=tg_dt.year,
                m=tg_dt.month,
                d=tg_dt.day
            )
            util.mkdir(out_file_path)
            pd.DataFrame(tg_dt_bar).to_pickle(out_file_path)

    def download_bar(self, from_dt: date) -> bool:

        OUT_DIR = self.out_dir + f"{self.db_name}/{self.market}/doll/threshold={self.from_bar_name.split('_')[-1]}/"
        OUT_FILE_FORMAT = OUT_DIR + "bar/y={y}/m={m:0=2}/d={d:0=2}/process_bar.pkl"

        print(self.market, self.from_bar_name, OUT_DIR, from_dt)

        if not firebase_admin._apps:
            cred = credentials.Certificate('./serviceAccount.json')
            firebase_admin.initialize_app(cred)

        db = firestore.client()
        doc_ref = (
            db
            .collection('Exchanger')
            .document(self.db_name)
        )

        target_rows = [d.to_dict() for d in doc_ref.collection(self.from_bar_name).where("date", ">=", str(from_dt)).get()]
        bar_list = [
            {
                "open":  row["open"],
                "low": row["low"],
                "high": row["high"],
                "close": row["close"],
                "volume": row["volume"],
                "timestamp": row["timestamp"],
                "date": datetime.strptime(row["date"], '%Y-%m-%d').date()
            }
            for row in target_rows
        ]

        self._write_bar_list_by_dt(
            bar_list=bar_list,
            out_file_format=OUT_FILE_FORMAT,
            from_dt=bar_list[0]["date"],
            to_dt=bar_list[-1]["date"]
        )

        return len(bar_list) > 0


# dfs = downloadFireStore(
#     market='BTC',
#     dl_site='bitFlyer',
#     from_bar_name='processing_doll_bar_300000000'
# )
# dfs.download_bar(from_dt=date(2022, 5, 11))
