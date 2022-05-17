
class Context:
    def __init__(self, initial_balance) -> None:
        """シミュレーションのコンテキスト

        Args:
            initial_balance (_type_): 初期所持金
        """

        # 残高
        self.balance = initial_balance

        # 買ったオーダーリスト
        self.buy_status = []
        # 買った回数
        self.total_buy_count = 0
        # 売った回数
        self.total_sell_count = 0
        # 買った金額の合計
        self.total_buy_amount = 0
        # 売った金額の合計
        self.total_sell_amount = 0
        # トータルリターン
        self.total_return_amount = 0
        pass
