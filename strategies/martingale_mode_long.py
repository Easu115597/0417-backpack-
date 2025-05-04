"""
馬丁策略模塊
"""
import time
import threading
import logging
import math
import asyncio
from trading.Ordermonitor import OrderMonitor
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union, Any
from concurrent.futures import ThreadPoolExecutor

from api.client import (
    get_balance, execute_order, get_open_orders, cancel_all_orders, 
    cancel_order, get_market_limits, get_klines, get_ticker, get_order_book
)
from ws_client.client import BackpackWebSocket
from database.db import Database
from utils.helpers import round_to_precision, round_to_tick_size, calculate_volatility
from strategies.volatility import calculate_historical_volatility
from logger import setup_logger
from trading.order_manager import OrderManager

import inspect
print(f"execute_order: {execute_order}, type: {type(execute_order)}, from: {inspect.getsourcefile(execute_order)}")

logger = setup_logger("martingale_long")

class MartingaleLongTrader:
    def __init__(
        self,
        api_key,
        secret_key,
        symbol,
        entry_type,
        base_asset,
        quote_asset,
        entry_price=None,
        db_instance=None,
        total_capital_usdt=100,
        price_step_down=0.05,
        take_profit_pct=0.012,
        stop_loss_pct=-0.33,
        current_layer=0,
        max_layers=3,
        martingale_multiplier=1.3,
        use_market_order=True,
        target_price=None,
        runtime=None,        
        monitor=None
        
        
        ):
        self.api_key = api_key
        self.secret_key = secret_key
        self.symbol = symbol 
        self.total_capital = total_capital_usdt
        self.price_step_down = price_step_down
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.current_layer = current_layer
        self.max_layers = max_layers
        self.multiplier = martingale_multiplier
        self.use_market_order = use_market_order
        self.target_price = target_price
        self.maker_buy_volume = 0
        self.maker_sell_volume = 0
        self.taker_buy_volume = 0
        self.taker_sell_volume = 0
        base_quote = self.symbol.upper().split("_")
        self.base_asset = base_quote[0]
        self.quote_asset = base_quote[1]
        self.runtime = runtime if runtime is not None else -1
        self.start_time = time.time()
        self.entry_price = None
        self.entry_type = entry_type
        self.open_orders = []
        self.filled_orders = []
        self.current_position = 0
        self.poll_interval = 5
        self.direction = 'long'
        self.monitor = monitor  # 新增監控器

        self.fund_allocation = []

        # 初始化數據庫
        self.db = db_instance if db_instance else Database()
        
        # 統計屬性
        self.session_start_time = datetime.now()
        self.session_fees = 0.0        
        self.session_quantities = []
        self.session_level = 0
        self.session_average_price = 0.0
        self.session_total_invested = 0.0
        self.session_maker_buy_volume = 0.0
        self.session_maker_sell_volume = 0.0

        # 初始化市場限制
        self.market_limits = get_market_limits(self.symbol)
        if not self.market_limits:
            raise ValueError(f"無法獲取 {symbol} 的市場限制")
        
        
        self.base_precision = self.market_limits['base_precision']
        self.quote_precision = self.market_limits['quote_precision']
        self.min_order_size = float(self.market_limits['min_order_size'])
        self.tick_size = float(self.market_limits['tick_size'])
        
        # 交易量統計
                
        self.total_fees = 0
        
        
        # 建立WebSocket連接
        self.ws = BackpackWebSocket(api_key=self.api_key,secret_key=self.secret_key,symbol=self.symbol,strategy=self)
        self.ws.connect()

        # 執行緒池用於後台任務
        self.executor = ThreadPoolExecutor(max_workers=3)
        
        # 等待WebSocket連接建立並進行初始化訂閲
        self._initialize_websocket()

        logger.info(f"初始化增強型馬丁策略 | 總資金: {total_capital_usdt} | 最大層級: {max_layers}")
        logger.info(f"基礎資產: {self.base_asset}, 報價資產: {self.quote_asset}")
        logger.info(f"基礎精度: {self.base_precision}, 報價精度: {self.quote_precision}")
        logger.info(f"最小訂單大小: {self.min_order_size}, 價格步長: {self.tick_size}")

    def _initialize_websocket(self):
        """等待WebSocket連接建立並進行初始化訂閲"""
        logger.info("WebSocket連接已建立，初始化行情和訂單更新...")
        self.ws.subscribe_bookTicker()
        success = self.ws.private_subscribe(f"account.orderUpdate.{self.symbol}")
        if not success:            
            logger.warning("訂閲訂單更新失敗，嘗試重試... (1/3)")
            for i in range(2, 4):
                time.sleep(1)
                success = self.ws.private_subscribe(f"account.orderUpdate.{self.symbol}")
                if success:
                    break
            else:
                logger.error("在 3 次嘗試後仍無法訂閲訂單更新")
                logger.warning("⚠️ WebSocket 訂閲部分失敗")

    def check_ws_connection(self):
        """檢查並恢復WebSocket連接"""
        ws_connected = self.ws and self.ws.is_connected()
        
        if not ws_connected:
            logger.warning("WebSocket連接已斷開或不可用，嘗試重新連接...")
            
            # 嘗試關閉現有連接
            if self.ws:
                try:
                    if hasattr(self.ws, 'running') and self.ws.running:
                        self.ws.running = False
                    if hasattr(self.ws, 'ws') and self.ws.ws:
                        try:
                            self.ws.ws.close()
                        except:
                            pass
                    self.ws.close()
                    time.sleep(0.5)
                except Exception as e:
                    logger.error(f"關閉現有WebSocket時出錯: {e}")
            
            # 創建新的連接
            try:
                logger.info("創建新的WebSocket連接...")
                self.ws = BackpackWebSocket(
                    self.api_key, 
                    self.secret_key, 
                    self.symbol, 
                    self.on_ws_message, 
                    auto_reconnect=True
                )
                self.ws.connect()
                
                # 等待連接建立
                wait_time = 0
                max_wait_time = 5
                while not self.ws.is_connected() and wait_time < max_wait_time:
                    time.sleep(0.5)
                    wait_time += 0.5
                    
                if self.ws.is_connected():
                    logger.info("WebSocket重新連接成功")
                    
                    # 重新初始化
                    
                    
                    self.ws.subscribe_bookTicker()
                    self.subscribe_order_updates()
                else:
                    logger.warning("WebSocket重新連接嘗試中，將在下次迭代再次檢查")
                    
            except Exception as e:
                logger.error(f"創建新WebSocket連接時出錯: {e}")
                return False
        
        return self.ws and self.ws.is_connected()
    
    def _dynamic_size_adjustment(self):
        try:
            volatility = calculate_historical_volatility(self.symbol, period=24)
            if volatility is not None:
                return max(0.5, min(1.5, 1 + (volatility - 0.02)))
        except Exception as e:
            logger.warning(f"波動率調整失敗，使用預設: {e}")
        return 1.0

    def allocate_funds(self):
        adjustment_factor = self._dynamic_size_adjustment()
        base = 1
        levels = [base * (self.multiplier ** i) for i in range(self.max_layers)]
        total_units = sum(levels)
        allocation = [(self.total_capital * (units / total_units)) * adjustment_factor for units in levels]
        logger.info(f"資金分配完成 | 各層金額: {allocation}")
        return allocation

    def on_ws_message(self, stream, data):
        """處理WebSocket消息回調"""
        if stream.startswith("account.orderUpdate."):
            event_type = data.get('e')
            
            # 「訂單成交」事件
            if event_type == 'orderFill':
                try:
                    side = data.get('S')
                    quantity = float(data.get('l', '0'))  # 此次成交數量
                    price = float(data.get('L', '0'))     # 此次成交價格
                    order_id = data.get('i')             # 訂單 ID
                    maker = data.get('m', False)         # 是否是 Maker
                    fee = float(data.get('n', '0'))      # 手續費
                    fee_asset = data.get('N', '')        # 手續費資產

                    logger.info(f"訂單成交: ID={order_id}, 方向={side}, 數量={quantity}, 價格={price}, Maker={maker}, 手續費={fee:.8f}")
                    
                    # 判斷交易類型
                    trade_type = 'market_making'  # 默認為做市行為
                    
                    
                    
                    # 準備訂單數據
                    order_data = {
                        'order_id': order_id,
                        'symbol': self.symbol,
                        'side': side,
                        'quantity': quantity,
                        'price': price,
                        'maker': maker,
                        'fee': fee,
                        'fee_asset': fee_asset,
                        'trade_type': trade_type
                    }
                    
                    # 安全地插入數據庫
                    def safe_insert_order():
                        try:
                            self.db.insert_order(order_data)
                        except Exception as db_err:
                            logger.error(f"插入訂單數據時出錯: {db_err}")
                    
                    # 直接在當前線程中插入訂單數據，確保先寫入基本數據
                    safe_insert_order()
                    
                    # 更新買賣量和馬丁策略成交量統計
                    if side == 'Bid':  # 買入
                        self.total_bought += quantity
                        self.buy_trades.append((price, quantity))
                        logger.info(f"買入成交: {quantity} {self.base_asset} @ {price} {self.quote_asset}")
                        
                        # 更新馬丁策略成交量
                        if maker:
                            self.maker_buy_volume += quantity
                            self.session_maker_buy_volume += quantity
                        else:
                            self.taker_buy_volume += quantity
                            self.session_taker_buy_volume += quantity
                        
                        self.session_buy_trades.append((price, quantity))
                            
                    elif side == 'Ask':  # 賣出
                        self.total_sold += quantity
                        self.sell_trades.append((price, quantity))
                        logger.info(f"賣出成交: {quantity} {self.base_asset} @ {price} {self.quote_asset}")
                        
                        # 更新馬丁策略成交量
                        if maker:
                            self.maker_sell_volume += quantity
                            self.session_maker_sell_volume += quantity
                        else:
                            self.taker_sell_volume += quantity
                            self.session_taker_sell_volume += quantity
                            
                        self.session_sell_trades.append((price, quantity))
                    
                    # 更新累計手續費
                    self.total_fees += fee
                    self.session_fees += fee
                        
                    # 在單獨的線程中更新統計數據，避免阻塞主回調
                    def safe_update_stats_wrapper():
                        try:
                            self._update_trading_stats()
                        except Exception as e:
                            logger.error(f"更新交易統計時出錯: {e}")
                    
                    self.executor.submit(safe_update_stats_wrapper)
                    
                    # 重新計算利潤（基於數據庫記錄）
                    # 也在單獨的線程中進行計算，避免阻塞
                    def update_profit():
                        try:
                            profit = self._calculate_db_profit()
                            self.total_profit = profit
                        except Exception as e:
                            logger.error(f"更新利潤計算時出錯: {e}")
                    
                    self.executor.submit(update_profit)
                    
                    # 計算本次執行的簡單利潤（不涉及數據庫查詢）
                    session_profit = self._calculate_session_profit()
                    
                    # 執行簡要統計
                    logger.info(f"累計利潤: {self.total_profit:.8f} {self.quote_asset}")
                    logger.info(f"本次執行利潤: {session_profit:.8f} {self.quote_asset}")
                    logger.info(f"本次執行手續費: {self.session_fees:.8f} {self.quote_asset}")
                    logger.info(f"本次執行淨利潤: {(session_profit - self.session_fees):.8f} {self.quote_asset}")
                    
                    self.trades_executed += 1
                    logger.info(f"總買入: {self.total_bought} {self.base_asset}, 總賣出: {self.total_sold} {self.base_asset}")
                    logger.info(f"Maker買入: {self.maker_buy_volume} {self.base_asset}, Maker賣出: {self.maker_sell_volume} {self.base_asset}")
                    logger.info(f"Taker買入: {self.taker_buy_volume} {self.base_asset}, Taker賣出: {self.taker_sell_volume} {self.base_asset}")
                    
                except Exception as e:
                    logger.error(f"處理訂單成交消息時出錯: {e}")
                    import traceback
                    traceback.print_exc()

    def _calculate_db_profit(self):
        """基於數據庫記錄計算已實現利潤（FIFO方法）"""
        try:
            # 獲取訂單歷史，注意這裡將返回一個列表
            order_history = self.db.get_order_history(self.symbol)
            if not order_history:
                return 0
            
            buy_trades = []
            sell_trades = []
            for side, quantity, price, maker, fee in order_history:
                if side == 'Bid':
                    buy_trades.append((float(price), float(quantity), float(fee)))
                elif side == 'Ask':
                    sell_trades.append((float(price), float(quantity), float(fee)))

            if not buy_trades or not sell_trades:
                return 0

            buy_queue = buy_trades.copy()
            total_profit = 0
            total_fees = 0

            for sell_price, sell_quantity, sell_fee in sell_trades:
                remaining_sell = sell_quantity
                total_fees += sell_fee

                while remaining_sell > 0 and buy_queue:
                    buy_price, buy_quantity, buy_fee = buy_queue[0]
                    matched_quantity = min(remaining_sell, buy_quantity)

                    trade_profit = (sell_price - buy_price) * matched_quantity
                    allocated_buy_fee = buy_fee * (matched_quantity / buy_quantity)
                    total_fees += allocated_buy_fee

                    net_trade_profit = trade_profit
                    total_profit += net_trade_profit

                    remaining_sell -= matched_quantity
                    if matched_quantity >= buy_quantity:
                        buy_queue.pop(0)
                    else:
                        remaining_fee = buy_fee * (1 - matched_quantity / buy_quantity)
                        buy_queue[0] = (buy_price, buy_quantity - matched_quantity, remaining_fee)

            self.total_fees = total_fees
            return total_profit

        except Exception as e:
            logger.error(f"計算數據庫利潤時出錯: {e}")
            import traceback
            traceback.print_exc()
            return 0

    def _calculate_session_profit(self):
        """計算本次執行的已實現利潤"""
        if not self.session_buy_trades or not self.session_sell_trades:
            return 0

        buy_queue = self.session_buy_trades.copy()
        total_profit = 0

        for sell_price, sell_quantity in self.session_sell_trades:
            remaining_sell = sell_quantity

            while remaining_sell > 0 and buy_queue:
                buy_price, buy_quantity = buy_queue[0]
                matched_quantity = min(remaining_sell, buy_quantity)

                # 計算這筆交易的利潤
                trade_profit = (sell_price - buy_price) * matched_quantity
                total_profit += trade_profit

                remaining_sell -= matched_quantity
                if matched_quantity >= buy_quantity:
                    buy_queue.pop(0)
                else:
                    buy_queue[0] = (buy_price, buy_quantity - matched_quantity)

        return total_profit

    def get_current_price(self):
        """獲取當前價格（優先使用WebSocket數據）"""
        self.check_ws_connection()
        price = None
        if self.ws and self.ws.connected:
            price = self.ws.get_current_price()
        
        if price is None:
            ticker = get_ticker(self.symbol)
            if isinstance(ticker, dict) and "error" in ticker:
                logger.error(f"獲取價格失敗: {ticker['error']}")
                return None
            
            if "lastPrice" not in ticker:
                logger.error(f"獲取到的價格數據不完整: {ticker}")
                return None
            return float(ticker['lastPrice'])
        return price
    
    def calculate_quantity(self, price, level=0):
        base_qty = self.total_capital / price
        multiplier = self.multiplier ** level
        return base_qty * multiplier
    
    def calculate_avg_entry_price(self):
        total_cost = sum(order['price'] * order['quantity'] for order in self.filled_orders)
        total_qty = sum(order['quantity'] for order in self.filled_orders)
        return total_cost / total_qty if total_qty > 0 else 0

    def subscribe_order_updates(self):
        """訂閲訂單更新流"""
        if not self.ws or not self.ws.is_connected():
            logger.warning("無法訂閲訂單更新：WebSocket連接不可用")
            return False
        
        # 嘗試訂閲訂單更新流
        stream = f"account.orderUpdate.{self.symbol}"
        if stream not in self.ws.subscriptions:
            retry_count = 0
            max_retries = 3
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    success = self.ws.private_subscribe(stream)
                    if success:
                        logger.info(f"成功訂閲訂單更新: {stream}")
                        return True
                    else:
                        logger.warning(f"訂閲訂單更新失敗，嘗試重試... ({retry_count+1}/{max_retries})")
                except Exception as e:
                    logger.error(f"訂閲訂單更新時發生異常: {e}")
                
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(1)  # 重試前等待
            
            if not success:
                logger.error(f"在 {max_retries} 次嘗試後仍無法訂閲訂單更新")
                return False
        else:
            logger.info(f"已經訂閲了訂單更新: {stream}")
            return True
    
    def generate_martingale_orders(self):
        """生成马丁策略订单"""
        orders = []
        current_price = self.get_current_price()
        if not current_price:
            return []

        # 动态计算加仓价格
        for layer in range(self.current_layer + 1):
            # 价格步长随层数增加
            price_step = self.base_spread_percentage * (1 + layer*0.2)
            
            # 买单价差递增
            buy_price = current_price * (1 - price_step/100)
            buy_price = round_to_tick_size(buy_price, self.tick_size)
            
            # 卖单价差递增
            sell_price = current_price * (1 + price_step/100) 
            sell_price = round_to_tick_size(sell_price, self.tick_size)
            
            # 订单量指数增长
            buy_size = self.base_order_size * (self.martingale_multiplier ** layer)
            sell_size = buy_size  # 对称下单
            
            orders.append(('Bid', buy_price, buy_size))
            orders.append(('Ask', sell_price, sell_size))
            
        return orders

    def place_martingale_orders(self, entry_price, price_step_down, layers):
        print(f"⛓️ 一次掛好 {layers - 1} 層補單，entry_price={entry_price}")

        for i in range(1, layers):
            try:
                next_price = entry_price * (1 - price_step_down * i)
                next_price = max(next_price, 0.01)  # 保底，防止負數
                
                # 計算這層要下多少 quantity
                layer_fund = self.fund_allocation[i]  # 每層分配資金
                qty = layer_fund / next_price
                qty = max(self.min_order_size, round_to_precision(qty, self.base_precision))

                logger.info(f"📈 掛第 {i} 層：價格={next_price}，數量={qty}")
                
                order_type = "Limit"  # 或你自己定義
                order = self.place_order(order_type, next_price, qty)
                order_id = order.get["order_id"]

                if order and order.get('status') == 'NEW':
                    print(f"✅ 成功掛第 {i} 層，order_id={order.get('orderId')}")
                else:
                    print(f"⚠️ 掛第 {i} 層失敗，order={order}")

            except Exception as e:
                print(f"❗ 掛第 {i} 層失敗，錯誤訊息: {str(e)}")

    print("🏁 補單流程結束")
    
    def execute_first_entry(self):
        """執行首單並直接一次掛好後續馬丁單"""

        logger.info("🚀 開始執行 execute_first_entry()")

        self.check_ws_connection()

        current_price = self.get_current_price()
        if not current_price:
            logger.error("❌ 無法獲取當前價格，跳過首單")
            logger.debug("🔍 當前 current_price 取得結果為 None")
            return

        retries = 3
        delay_seconds = 5

        # 取得第一層資金分配
        allocated_funds = self.allocate_funds()
        self.fund_allocation = allocated_funds
        first_layer_fund = allocated_funds[0]

        logger.info(f"✅ 使用第一層分配資金: {first_layer_fund}")
        logger.debug(f"🔍 分配資金列表: {allocated_funds}")

        for attempt in range(1, retries + 1):
            logger.info(f"📤 嘗試第 {attempt} 次提交首單...")

            self.cancel_existing_orders()

            # 計算價格與訂單型態
            if self.entry_type == "manual":
                price = self.entry_price
                order_type = "limit"
                logger.debug(f"📝 entry_type=manual，使用手動價格 {price}")
            elif self.entry_type == "market":
                price = current_price
                order_type = "market"
                logger.debug(f"📝 entry_type=market，使用市價 current_price={current_price}")
            elif self.entry_type == "offset":
                price = current_price * (1 - self.price_step_down)
                order_type = "limit"
                logger.debug(f"📝 entry_type=offset，使用偏移價格 {price} (current_price={current_price}, step_down={self.price_step_down})")
            else:
                logger.error(f"❌ 未知的 entry_type: {self.entry_type}")
                raise ValueError(f"Unknown entry_type: {self.entry_type}")

            qty = first_layer_fund / price
            qty = max(self.min_order_size, round_to_precision(qty, self.base_precision))

            logger.debug(f"🔢 計算下單數量 qty={qty} (first_layer_fund={first_layer_fund}, price={price})")

            try:
                order_response = self.place_order(order_type, price, qty)
                order_id = order.get["order_id"]

                monitor.add_order(
                    order_id=order_id,
                    symbol=symbol,
                    side="buy",
                    price=price,
                    size=size
                )
                logger.debug(f"📩 下單回應: {order_response}")
            except Exception as e:
                logger.warning(f"⚠️ 首單第 {attempt} 次下單異常: {e}")
                order_response = None

            if isinstance(order_response, dict):
                order_status = order_response.get("status", "").upper()
                logger.debug(f"🔍 取得回應 order_status={order_status}")

                if order_status in ["FILLED", "PARTIALLY_FILLED", "NEW"]:
                    self.entry_price = float(order_response.get("price", price))
                    logger.info(f"✅ 首單下單成功，entry_price 設為 {self.entry_price}")

                    # --- 🛠️ 首單成功後，直接掛後面幾層馬丁單 ---
                    qty = max(self.min_order_size, round_to_precision(qty, self.base_precision))
                    layers = self.max_layers
                    price_step_down_value = self.price_step_down  # 每層下降比例（不是價格）

                    logger.debug(f"🛠️ 掛單參數 qty=qty={qty}, layers={layers}, price_step_down_value={price_step_down_value}")

                    self.place_martingale_orders(
                        entry_price=self.entry_price,
                        price_step_down=price_step_down_value,
                        layers=layers
                    )
                    return
                else:
                    logger.warning(f"⚠️ 首單第 {attempt} 次下單失敗，Status: {order_status}, Response: {order_response}")
            else:
                logger.warning(f"⚠️ 首單第 {attempt} 次下單失敗，Response 非 dict 格式: {order_response}")

            time.sleep(delay_seconds)

        # --- 如果全部失敗，走市價備案 ---
        logger.warning("⚠️ 首單所有嘗試失敗，使用市價單進場")

        fallback_qty = first_layer_fund / current_price
        fallback_qty = max(self.min_order_size, round_to_precision(fallback_qty, self.base_precision))

        logger.debug(f"🆘 市價備案下單 fallback_qty={fallback_qty}, current_price={current_price}")

        try:
            fallback_order = self.place_order("market", current_price, fallback_qty)
            logger.debug(f"📩 市價備案下單回應: {fallback_order}")

            if fallback_order and fallback_order.get("status", "").upper() in ["FILLED", "PARTIALLY_FILLED", "NEW"]:
                self.entry_price = float(fallback_order.get("price", current_price))
                logger.info(f"✅ 市價單備案成功，entry_price 設為 {self.entry_price}")

                
                layers = self.max_layers
                price_step_down_value = self.price_step_down

                logger.debug(f"🛠️ 市價備案後掛單參數  layers={layers}, price_step_down_value={price_step_down_value}")

                self.place_martingale_orders(
                    entry_price=self.entry_price,
                    price_step_down=price_step_down_value,
                    layers=layers
                )
            else:
                logger.error(f"❌ 市價單備案仍失敗: {fallback_order}")
        except Exception as e:
            logger.error(f"❌ 市價備案下單錯誤: {e}")


    def place_order(self, order_type, price, quantity,side="Bid", reduce_only=False, post_only=True):
        order_details = {
            "side": "Bid" ,
            "symbol": self.symbol,
        }

        if self.use_market_order or order_type.lower() == "market":
            order_details["orderType"] = "Market"
            order_details["quoteQuantity"] = round(quantity * price, self.quote_precision)
        else:
            order_details["orderType"] = "Limit"
            order_details["price"] = round(price, self.quote_precision)
            order_details["quantity"] = round(quantity, self.base_precision)
            order_details["timeInForce"] = "GTC"
            order_details["postOnly"] = True
            

        print("[DEBUG] order_details ready to sign:", order_details)

        try:
            result = execute_order(self.api_key, self.secret_key, order_details)
            
            print("[DEBUG] Order placed result:", result)
            return result
        except Exception as e:
            logger.error(f"❌ 下單失敗: {e}")

    
            return None
        

    def check_exit_condition(self):
        if not self.entry_price:
            logger.warning("⚠️ 尚無成交單價，跳過出場判斷。")
            return False

        current_price = self.get_current_price()
        avg_price = self._calculate_weighted_avg()
        
        # 防零除
        if avg_price <= 0:
            logger.error("❌ 無效的平均價格，跳過出場判斷")
            return False
        
        profit_pct = (current_price - avg_price) / avg_price
        
        # 止盈條件
        if profit_pct >= self.take_profit_pct:
            self.close_all_positions()
            logger.info("🎯 達成止盈條件，結束交易")
            return True
        
        # 止損條件（注意stop_loss_pct是負值）
        elif current_price <= avg_price * (1 + self.stop_loss_pct):
            self.close_all_positions()
            logger.info("🛑 達成止損條件，結束交易")
            return True
        
        return False

    def close_all_positions(self):
        current_price = self.get_current_price()
        # 市價單平倉
        order_details = {
            "symbol": self.symbol,
            "side": "Ask",
            "orderType": "Market",
            "quantity": self.total_bought - self.total_sold
        }
        self.client.execute_order(order_details)
        logger.info("🚀 觸發止盈/止損，市價平倉")

    def on_order_update(self, data: dict):
        """處理WebSocket訂單更新"""
        if data.get('e') == 'orderFill':
            order_id = data.get('i')
            filled_qty = float(data.get('l', '0'))
            price = float(data.get('L', '0'))
            
            # 更新持倉與均價
            self.total_bought += filled_qty
            self._update_average_price(price, filled_qty)
            
            logger.info(f"訂單成交: {order_id} | 數量: {filled_qty} @ {price}")

    def _update_average_price(self, price: float, qty: float):
        """動態更新持倉均價"""
        total_cost = self.entry_price * self.total_bought + price * qty
        self.total_bought += qty
        self.entry_price = total_cost / self.total_bought if self.total_bought > 0 else 0
   
    
    def check_order_status(self):
        """每30秒檢查一次訂單狀態"""
        while self.running:
            for order in self.open_orders.copy():
                response = self.client.get_order(order['id'])
                if response['status'] == 'Filled':
                    self._handle_filled_order(response)
                elif response['status'] in ['Canceled', 'Expired']:
                    self.open_orders.remove(order)
            time.sleep(30)

    def _calculate_weighted_avg(self):
        total_cost = 0.0
        total_qty = 0.0
        for fill in self.filled_orders:
            total_cost += float(fill['price']) * float(fill['quantity'])
            total_qty += float(fill['quantity'])
        return total_cost / total_qty if total_qty > 0 else 0.0

    def on_order_filled(self, order_id):
        order = self.db.get_order(order_id)
        self.entry_price = self._calculate_average_price()
        logger.info(f"📊 持倉均價更新: {self.entry_price}")
    
    def calculate_avg_price(self):
        """
        計算目前倉位的加權平均進場價格
        """
        if not self.positions:
            return 0

        total_cost = 0
        total_qty = 0
        for position in self.positions:
            total_cost += position['price'] * position['quantity']
            total_qty += position['quantity']
        
        if total_qty == 0:
            return 0

        return total_cost / total_qty
    
    def handle_order_fill(order):
        symbol = order["symbol"]
        side = order["side"]
        price = float(order["price"])
        size = float(order["size"])

        if side == "buy":
            session_buy_trades.append({"price": price, "size": size})
        else:
            session_sell_trades.append({"price": price, "size": size})

        print(f"[FILLED] {side.upper()} {size} {symbol} @ {price}")
        # 可加入後續開倉/平倉策略

    def handle_order_reject(order):
        print(f"[REJECTED] Order {order['order_id']} was rejected.")
        # 可選擇重下或記錄異常

    def _check_risk(self):
        """马丁策略风控"""
        unrealized_pnl = self._calculate_unrealized_pnl()
        
        # 动态止损
        stop_loss = self._dynamic_stop_level()
        if unrealized_pnl <= -stop_loss:
            logger.critical(f"触发动态止损 {stop_loss}%")
            self.close_all_positions()
            self.current_layer = 0
            
        # 层级控制
        if self.current_layer >= self.max_layers:
            logger.warning("达到最大加仓层级")
            self.adjust_spread(self.base_spread_percentage * 1.5)
            self.current_layer = self.max_layers - 1

    def _dynamic_stop_level(self):
        """动态止损计算"""
        return max(
            self.stop_loss_pct, 
            -0.04 * (self.current_layer + 1)
        )

    def _calculate_unrealized_pnl(self):
        """计算未实现盈亏"""
        avg_cost = self._average_cost()
        current_price = self.get_current_price()
        position = self.total_bought - self.total_sold
        return (current_price - avg_cost) * position if current_price else 0

    def cancel_existing_orders(self):
        """取消所有現有訂單"""
        open_orders = get_open_orders(self.api_key, self.secret_key, self.symbol)
        
        if isinstance(open_orders, dict) and "error" in open_orders:
            logger.error(f"獲取訂單失敗: {open_orders['error']}")
            return
        
        if not open_orders:
            logger.info("沒有需要取消的現有訂單")
            self.active_buy_orders = []
            self.active_sell_orders = []
            return
        
        logger.info(f"正在取消 {len(open_orders)} 個現有訂單")
        
        try:
            # 嘗試批量取消
            result = cancel_all_orders(self.api_key, self.secret_key, self.symbol)
            
            if isinstance(result, dict) and "error" in result:
                logger.error(f"批量取消訂單失敗: {result['error']}")
                logger.info("嘗試逐個取消...")
                
                # 初始化線程池
                with ThreadPoolExecutor(max_workers=5) as executor:
                    cancel_futures = []
                    
                    # 提交取消訂單任務
                    for order in open_orders:
                        order_id = order.get('id')
                        if not order_id:
                            continue
                        
                        future = executor.submit(
                            cancel_order, 
                            self.api_key, 
                            self.secret_key, 
                            order_id, 
                            self.symbol
                        )
                        cancel_futures.append((order_id, future))
                    
                    # 處理結果
                    for order_id, future in cancel_futures:
                        try:
                            res = future.result()
                            if isinstance(res, dict) and "error" in res:
                                logger.error(f"取消訂單 {order_id} 失敗: {res['error']}")
                            else:
                                logger.info(f"取消訂單 {order_id} 成功")
                                self.orders_cancelled += 1
                        except Exception as e:
                            logger.error(f"取消訂單 {order_id} 時出錯: {e}")
            else:
                logger.info("批量取消訂單成功")
                self.orders_cancelled += len(open_orders)
        except Exception as e:
            logger.error(f"取消訂單過程中發生錯誤: {str(e)}")
        
        # 等待一下確保訂單已取消
        time.sleep(1)
        
        # 檢查是否還有未取消的訂單
        remaining_orders = get_open_orders(self.api_key, self.secret_key, self.symbol)
        if remaining_orders and len(remaining_orders) > 0:
            logger.warning(f"警告: 仍有 {len(remaining_orders)} 個未取消的訂單")
        else:
            logger.info("所有訂單已成功取消")
        
        # 重置活躍訂單列表
        self.active_buy_orders = []
        self.active_sell_orders = []
    
    def check_order_fills(self):
        """檢查訂單成交情況"""
        open_orders = get_open_orders(self.api_key, self.secret_key, self.symbol)
        
        if isinstance(open_orders, dict) and "error" in open_orders:
            logger.error(f"獲取訂單失敗: {open_orders['error']}")
            return
        
        # 獲取當前所有訂單ID
        current_order_ids = set()
        if open_orders:
            for order in open_orders:
                order_id = order.get('id')
                if order_id:
                    current_order_ids.add(order_id)
        
        # 記錄更新前的訂單數量
        prev_buy_orders = len(self.active_buy_orders)
        prev_sell_orders = len(self.active_sell_orders)
        
        # 更新活躍訂單列表
        active_buy_orders = []
        active_sell_orders = []
        
        if open_orders:
            for order in open_orders:
                if order.get('side') == 'Bid':
                    active_buy_orders.append(order)
                elif order.get('side') == 'Ask':
                    active_sell_orders.append(order)
        
        # 檢查買單成交
        filled_buy_orders = []
        for order in self.active_buy_orders:
            order_id = order.get('id')
            if order_id and order_id not in current_order_ids:
                price = float(order.get('price', 0))
                quantity = float(order.get('quantity', 0))
                logger.info(f"買單已成交: {price} x {quantity}")
                filled_buy_orders.append(order)
        
        # 檢查賣單成交
        filled_sell_orders = []
        for order in self.active_sell_orders:
            order_id = order.get('id')
            if order_id and order_id not in current_order_ids:
                price = float(order.get('price', 0))
                quantity = float(order.get('quantity', 0))
                logger.info(f"賣單已成交: {price} x {quantity}")
                filled_sell_orders.append(order)
        
        # 更新活躍訂單列表
        self.active_buy_orders = active_buy_orders
        self.active_sell_orders = active_sell_orders
        
        # 輸出訂單數量變化，方便追踪
        if prev_buy_orders != len(active_buy_orders) or prev_sell_orders != len(active_sell_orders):
            logger.info(f"訂單數量變更: 買單 {prev_buy_orders} -> {len(active_buy_orders)}, 賣單 {prev_sell_orders} -> {len(active_sell_orders)}")
        
        logger.info(f"當前活躍訂單: 買單 {len(self.active_buy_orders)} 個, 賣單 {len(self.active_sell_orders)} 個")

    def _ensure_data_streams(self):
        """確保所有必要的數據流訂閲都是活躍的"""
       
        
        # 檢查行情數據訂閲
        if "bookTicker" not in self.ws.subscriptions:
            logger.info("重新訂閲行情數據...")
            self.ws.subscribe_bookTicker()
        
        # 檢查私有訂單更新流
        if f"account.orderUpdate.{self.symbol}" not in self.ws.subscriptions:
            logger.info("重新訂閲私有訂單更新流...")
            self.subscribe_order_updates()
    
    def run(self, duration_seconds=-1, interval_seconds=60):
        """執行馬丁策略"""

        logger.info(f"開始運行馬丁策略: {self.symbol}")
        logger.info(f"運行時間: {duration_seconds if duration_seconds > 0 else '不限'} 秒, 間隔: {interval_seconds} 秒")

        self.session_start_time = datetime.now()
        self.session_buy_trades = []
        self.session_sell_trades = []
        self.session_fees = 0.0
        self.session_maker_buy_volume = 0.0
        self.session_maker_sell_volume = 0.0
        self.session_taker_buy_volume = 0.0
        self.session_taker_sell_volume = 0.0

        start_time = time.time()
        iteration = 0
        last_report_time = start_time
        report_interval = 300  # 每5分鐘打印一次報表

        level = 1
        max_layers = self.max_layers if hasattr(self, 'max_layers') else 10  # 預設最多10層
        self.filled_orders = []

        # 先下第一層入場單
        self.execute_first_entry()

        while duration_seconds == -1 or (time.time() - start_time) < duration_seconds:

            now = time.time()
            iteration += 1
            logger.info(f"\n=== 第 {iteration} 次循環 ===")
            logger.info(f"時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # 檢查 WebSocket 連接狀態
            if not self.check_ws_connection():
                self.reconnect_ws()
                self._ensure_data_streams()

            # 取得當前市場價格
            price = self.get_current_price()
            logger.info(f"當前市場價格: {price}")

            # 如果尚無任何成交，跳過
            if not self.filled_orders:
                logger.warning("⚠️ 尚無成交單，跳過加碼和出場判斷")
                time.sleep(interval_seconds)
                continue

            # 檢查是否達到止盈或止損條件
            if self.check_exit_condition():
                total_qty = sum(order['quantity'] for order in self.filled_orders)
                target_price = price
                sell_order = self.api.place_martingale_orders(
                    symbol=self.symbol,
                    side="sell",
                    price=target_price,
                    quantity=total_qty,
                    order_type="market" if self.use_market_order else "limit",
                    use_market=self.use_market_order
                )
                logger.info(f"Exit order placed: {sell_order}")
                break

            # 檢查是否有新成交
            self.check_order_fills()

            # 如果最新持倉量增加了，且層數還沒到上限，則加碼下一層
            if len(self.filled_orders) >= level and level < max_layers:
                level += 1
                next_price = self.entry_price * (1 - self.price_step_down * level)
                qty = self.calculate_quantity(next_price, level)

                order = self.api.place_martingale_orders(
                    symbol=self.symbol,
                    side="buy",
                    price=next_price,
                    quantity=qty,
                    order_type="limit",
                    use_market=False  # 加碼單通常用限價
                )

                if order:
                    logger.info(f"Layer {level} 掛單成功，價格: {next_price}，數量: {qty}")
                else:
                    logger.error(f"Layer {level} 掛單失敗")

            # 每隔一段時間打印一次報表
            if now - last_report_time > report_interval:
                self.report_session_statistics()
                last_report_time = now

            time.sleep(interval_seconds)

        logger.info("✅ 運行結束")
                # 估算利潤
        self.estimate_profit()
                
        


