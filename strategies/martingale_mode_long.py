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
from decimal import Decimal
from api.client import (
    get_balance, execute_order, get_open_orders, cancel_all_orders, 
    cancel_order, get_market_limits, get_klines, get_ticker, get_order_book
)
from ws_client.client import BackpackWebSocket
from database.db import Database
from utils.helpers import round_to_precision, round_to_tick_size, calculate_volatility
from strategies.volatility import calculate_historical_volatility
from logger import setup_logger
from trading.Ordermonitor import OrderMonitor

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
        order_monitor=None,
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
        monitor=None,
              
        ):
        self.api_key = api_key
        self.secret_key = secret_key
       
        self.symbol = symbol 
        
        self.total_capital = total_capital_usdt
        self.price_step_down = price_step_down
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.current_layer = 0
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
        self.orders_cancelled = []
        self.fund_allocation = []
        self.order_monitor = order_monitor
        self.success_orders = []
        self.active_orders = []
        

        
    

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

    async def place_martingale_orders(self, entry_price, price_step_down, layers):
        logger.info("🚀 Placing Martingale Ladder Orders")

        self.entry_price = entry_price
        self.price_step_down = price_step_down
        self.max_layers = layers

        self.active_orders = []

        for layer in range(layers):  # 從第0層開始，包含首單
            try:
                price = entry_price * (1 - price_step_down * layer)
                price = round_to_tick_size(price, self.tick_size)
                size = self.base_order_size * (self.martingale_multiplier ** layer)

                res = await self.place_order(
                    order_type="Limit",
                    price=price,
                    quantity=size,
                    side="Bid",
                    reduce_only=False,
                    post_only=True
                )

                if res and "order_id" in res:
                    logger.info(f"✅ Placed Tier {layer + 1} Order: {res['order_id']}")
                    self.active_orders.append({
                        "order_id": res["order_id"],
                        "price": price,
                        "size": size,
                        "tier": layer + 1
                    })
                else:
                    logger.warning(f"⚠️ No order_id returned for Tier {layer + 1} order.")
            except Exception as e:
                logger.error(f"❌ Error placing Tier {layer + 1} order: {e}")


    
    
    async def place_order(self, order_type, price, quantity, side="Bid", reduce_only=False, post_only=True):
        order_details = {
            "side": side,
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
            order_details["postOnly"] = post_only

        logger.debug(f"[DEBUG] order_details ready to sign: {order_details}")

        try:
            result = await self.client.place_order(order_details)
            logger.debug(f"[DEBUG] Order placed result: {result}")
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
        execute_order(order_details)
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
                response = get_order(order['id'])
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
    
    def calculate_average_price(self):
        """加權平均計算防呆"""
        if not self.filled_orders:
            return 0.0
        
        total_cost = sum(o['price']*o['quantity'] for o in self.filled_orders)
        total_qty = sum(o['quantity'] for o in self.filled_orders)
        
        return total_cost / total_qty if total_qty != 0 else 0.0
    
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
                        order_id = order_response.get("order_id")
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
                order_id = order_response.get("order_id")
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
            order_id = order_response.get("order_id")
            if order_id and order_id not in current_order_ids:
                price = float(order.get('price', 0))
                quantity = float(order.get('quantity', 0))
                logger.info(f"買單已成交: {price} x {quantity}")
                filled_buy_orders.append(order)
        
        # 檢查賣單成交
        filled_sell_orders = []
        for order in self.active_sell_orders:
            order_id = order_response.get("order_id")
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
    
    async def run(self, duration_seconds=-1, interval_seconds=60):
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
        self.filled_orders = []

        start_time = time.time()
        iteration = 0
        last_report_time = start_time
        report_interval = 300  # 每5分鐘打印一次報表

        

        # 初始化 entry_price
        if self.entry_type in ("offset", "market"):
            ticker = await self.client.get_ticker(self.symbol)
            self.entry_price = float(ticker["price"])
        elif self.entry_type == "manual":
            if self.entry_price is None:
                raise ValueError("manual entry requires --entry-price")
        else:
            raise ValueError(f"Unsupported entry_type: {self.entry_type}")

        # ✅ 一次掛好所有層數的馬丁單（包含首單）
        await self.place_martingale_orders(
            entry_price=self.entry_price,
            price_step_down=self.price_step_down,
            layers=self.max_layers
        )

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
                await asyncio.sleep(interval_seconds)
                continue

            # 檢查是否達到止盈或止損條件
            if self.check_exit_condition():
                total_qty = sum(order['quantity'] for order in self.filled_orders)
                target_price = price
                sell_order = self.api.place_order(
                    order_type="market" if self.use_market_order else "limit",
                    price=target_price,
                    quantity=total_qty,
                    side="Ask",  # 賣出
                    reduce_only=True
                )
                logger.info(f"Exit order placed: {sell_order}")
                break

            # 檢查是否有新成交
            self.check_order_fills()

            # 每隔一段時間打印一次報表
            if now - last_report_time > report_interval:
                self.report_session_statistics()
                last_report_time = now

            await asyncio.sleep(interval_seconds)

            
        logger.info("✅ 運行結束")
                # 估算利潤
        self.estimate_profit()

    async def start(self):
        logger.info("Starting Martingale Strategy")
        await self.place_entry_orders()
                
    async def place_entry_orders(self):
        logger.info("Placing entry orders")
        tasks = []
        for i in range(3):
            tier_price = self.entry_price - (i * self.price_step_down)
            tier_qty = self.base_qty * (self.multiplier ** i)
            logger.info(f"Tier {i+1}: price={tier_price}, qty={tier_qty}")
            tasks.append(self.execute_limit_order(price=tier_price, qty=tier_qty))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict) and result.get("status") == "filled":
                self.success_orders.append(result)

        if not self.success_orders and self.use_market_order:
            logger.warning("No limit orders filled. Falling back to market order.")
            market_result = await self.execute_market_order()
            if market_result:
                self.success_orders.append(market_result)

        if self.success_orders:
            logger.info(f"Entry filled: {self.success_orders}")
            await self.place_next_orders()
        else:
            logger.error("No orders were filled. Strategy stops here.")

    async def execute_limit_order(self, price, qty):
        try:
            logger.info(f"Placing limit order at {price} for qty {qty}")
            order = await place_order(
                symbol=self.symbol,
                side="buy",
                type="limit",
                price=price,
                size=qty,
                post_only=True,
            )
            logger.info(f"Limit order placed: {order['order_id']}")
            status = await self.order_monitor.wait_for_status(order['order_id'])
            logger.info(f"Order status: {status}")
            if status == "filled":
                return order
        except Exception as e:
            logger.error(f"Limit order error: {e}")
        return None

    async def execute_market_order(symbol, base_qty, multiplier, place_order_func, order_monitor):
        try:
            logger.info("Placing market order")
            qty = base_qty * (multiplier ** 1)
            order = await place_order_func(
                symbol=symbol,
                side="buy",
                type="market",
                size=qty
            )
            logger.info(f"Market order placed: {order['order_id']}")
            status = await order_monitor.wait_for_status(order['order_id'])
            if status == "filled":
                return order
        except Exception as e:
            logger.error(f"Market order error: {e}")
        return None

    async def place_next_orders(symbol, success_orders, take_profit_pct, stop_loss_pct, place_order_func):
        logger.info("Placing TP and SL orders")
        for order in success_orders:
            price = Decimal(order['price'])
            qty = Decimal(order['size'])
            take_profit_price = float(price * (1 + Decimal(take_profit_pct)))
            stop_loss_price = float(price * (1 - Decimal(stop_loss_pct)))

            logger.info(f"TP: {take_profit_price}, SL: {stop_loss_price}, qty: {qty}")

            await place_order_func(
                symbol=symbol,
                side="sell",
                type="limit",
                price=take_profit_price,
                size=qty
            )
            await place_order_func(
                symbol=symbol,
                side="sell",
                type="stop_market",
                stop_price=stop_loss_price,
                size=qty
            )


