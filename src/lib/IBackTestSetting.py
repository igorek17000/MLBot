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
        """バックテスト設定のインターフェース

        Args:
            rule_name (str): シミュレーションのルール名称
            version (str): シミュレーションのルールのバージョン
            dir_path (str): ohlcv形式データのフォルダパス
            file_name (str): ohlcv形式データのファイル名
            read_from_dt (date): 読み込み開始日
            read_to_dt (date): 読み込み終了日
            start_dt (date): シミュレーション開始日付
            initial_balance (int): 初期所持金
            price_col (str): 価格に使用するカラム名
            experiment_name (str, optional): シミュレーションの実験名. Defaults to "MLBot".
        """
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
    def get_mlflow_params(self) -> dict:
        """mlflowに記録するタグを設定。
           オプションの設定を記録するためのもの。必須の設定は自動で記録される。

        Returns:
            dict : タグを記載したディクショナリ
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_start_idx(self, ohlcv_data: DataFrame) -> int:
        """シミュレーションの開始インデックスを返す関数

        Args:
            ohlcv_data (DataFrame): 読み込んだohlcvデータ

        Returns:
            int: 開始インデックス
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def judge_buysell_timing(self, now_idx: int, ohlcv_data: DataFrame, context: Context) -> dict:
        """売買タイミングを判定する関数

        Args:
            now_idx (int): 現時点の処理インデックス
            ohlcv_data (DataFrame): ohlcv形式のデータ
            context (Context): コンテキスト

        Returns:
            dict(
                idx (int) : 現時点の処理インデックス
                buy_flg (bool) : 買いタイミングと判定した時、True
                sell_flg (bool) : 売りタイミングと判定した時、True
                buy_idx (int) : 実際に買うidx ※現時点よりも未来とする
                sell_idx (int) : 実際に売るidx ※現時点よりも未来とする
                buy_volume (float64) : 買うときのボリューム
                sell_order_list (List[Order]) : 売るオーダーのリスト
                evidence (dict) : 根拠となる値（中身は自由に設定化）
            )
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_judge_evidence_data(self, res: dict) -> DataFrame:
        """アウトプット用のエビデンス出力

        Args:
            res (dict): バックテストのリターンと同様のdict

        Raises:
            DataFrame: idxをインデックスとしたpd.DataFrame
        """
        raise NotImplementedError()
