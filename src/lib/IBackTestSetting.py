from datetime import date
from pandas import DataFrame
from lib.context import Context
import abc


class IBackTestSetting(metaclass=abc.ABCMeta):
    def __init__(
        self, rule_name: str, version: str,
        dir_path: str, file_name: str,
        read_from_dt: date, read_to_dt: date, start_dt: date,
        initial_balance: int, price_col: str,
        experiment_name: str = "MLBot",
    ) -> None:

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

    @abc.abstractmethod
    def get_start_idx(self, ohlcv_data):
        raise NotImplementedError()

    @abc.abstractmethod
    def judge_buysell_timing(self, now_idx: int, ohlcv_data: DataFrame, context: Context) -> dict:
        raise NotImplementedError()
