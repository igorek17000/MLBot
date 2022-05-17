
class Context:
    def __init__(self, initial_balance) -> None:
        self.balance = initial_balance

        self.buy_status = []
        self.total_buy_count = 0
        self.total_sell_count = 0
        self.total_buy_amount = 0
        self.total_sell_amount = 0
        self.total_return_amount = 0
        pass
