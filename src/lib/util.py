from pathlib import Path
from typing import List, Union
from pandas import DataFrame
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta


def get_files_dir(dir_path: Union[str, Path], pattern="*") -> List[Path]:
    """指定フォルダ内に存在するファイルのパスを返す

    Args:
        dir_path (Union[str,Path]): フォルダのパス
        pattern (str, optional): 一致パターン。ワイルドカード。 Defaults to "*".

    Returns:
        List[Path]: _description_
    """
    if type(dir_path) == str:
        dir_path = Path(dir_path)

    return list(dir_path.glob(pattern))


def get_file_exists(file_path: Union[str, Path]) -> bool:
    """指定のパスの存在確認

    Args:
        file_path (Union[str, Path]): 存在確認したいパス

    Returns:
        bool: 存在すればTrue、そうでなければFalse
    """
    if type(file_path) == str:
        file_path = Path(file_path)
    return file_path.exists()


def mkdir(file_path: Union[str, Path]) -> None:
    """指定のパスに必要な中間のフォルダを含めてフォルダ作成する

    Args:
        file_path (Union[str, Path]): 作成したいファイルのパス

    """

    if type(file_path) == str:
        file_path = Path(file_path)
    dir_path = file_path.parent
    dir_path.mkdir(parents=True, exist_ok=True)


def create_date_list(from_dt: date, to_dt: date) -> List[date]:
    """開始～終了までの日付のリストを返す関数

    Args:
        from_dt (date): 開始日
        to_dt (date): 終了日

    Returns:
        List[date]: 日付のリスト
    """
    date_list = []
    now_date = from_dt
    while now_date <= to_dt:
        date_list.append(now_date)
        now_date += relativedelta(days=1)
    return date_list


def read_pickle_data_to_df(dir_path: str, file_name: str, from_dt: date, to_dt: date) -> DataFrame:
    READ_FILE_FORMAT = (
        dir_path + "y={y}/m={m:0=2}/d={d:0=2}/" + file_name
    )

    tg_dt_list = create_date_list(from_dt=from_dt, to_dt=to_dt)

    pdf_list = []
    success_flg = True
    for now_dt in tg_dt_list:
        file_path = READ_FILE_FORMAT.format(
            y=now_dt.year, m=now_dt.month, d=now_dt.day)

        if not get_file_exists(file_path):
            print(file_path, ": File Not Exists ERROR")
            success_flg = False
            continue

        pdf_list.append(pd.read_pickle(file_path))

    return pd.concat(pdf_list), success_flg


# read_pickle_data_to_df(
#     dir_path="/mnt/g/workspace/MLBot/data/processing/raw_formart/GMO/BTC_JPY/",
#     file_name="process_raw_format.pkl",
#     from_dt=date(2022,5,1),
#     to_dt=date(2022,5,2)
# )
