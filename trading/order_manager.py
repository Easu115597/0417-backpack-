# trading/order_manager.py
class OrderManager:
    def __init__(self, client, symbol, use_market_order=False):
        self.client = client
        self.symbol = symbol
        self.use_market_order = use_market_order

    def submit_order(self, side: str, quantity: float, price: float = None):
        """統一處理市價/限價下單"""
        symbol = self.symbol.replace("_", "-")

        order_payload = {
            "symbol": symbol,
            "side": side,
            "orderType": "Market" if self.use_market_order else "Limit",
            "timeInForce": "IOC" if not self.use_market_order else None
        }

        if self.use_market_order:
            order_payload["quoteQuantity"] = round(quantity, 6)
        else:
            order_payload["quantity"] = quantity
            order_payload["price"] = round(price, 6)

        logger.info(f"📤 提交訂單 API Payload: {order_payload}")
        return self.client.execute_order(order_payload)
