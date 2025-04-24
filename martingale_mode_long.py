"""
馬丁策略模塊
"""
import time
import threading
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
from logger import setup_logger


logger = setup_logger("martingale_long")

class MartingaleLongTrader:
    def __init__(
        self, 
        api_key, 
        secret_key, 
        symbol, 
        db_instance=None,
        total_capital_usdt=100,
        price_step_down=0.008,
        take_profit_pct=0.012,
        stop_loss_pct=-0.33,
        current_layer = 0,
        max_layers=5,
        martingale_multiplier=1.3,
        use_market_order=True,
        target_price=None
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
        self.market_limits = get_market_limits(symbol)
        if not self.market_limits:
            raise ValueError(f"無法獲取 {symbol} 的市場限制")
        
        
        self.base_precision = self.market_limits['base_precision']
        self.quote_precision = self.market_limits['quote_precision']
        self.min_order_size = float(self.market_limits['min_order_size'])
        self.tick_size = float(self.market_limits['tick_size'])
        
        # 交易量統計
        self.maker_buy_volume = 0
        self.maker_sell_volume = 0        
        self.total_fees = 0
        
        
        # 建立WebSocket連接
        self.ws = BackpackWebSocket(api_key, secret_key, symbol, self.on_ws_message, auto_reconnect=True)
        self.ws.connect()
        
        # 跟蹤活躍訂單
        self.active_buy_orders = []
        self.active_sell_orders = []
        
        # 記錄買賣數量以便重新平衡
        self.total_bought = 0
        self.total_sold = 0
        
        # 交易記錄 - 用於計算利潤
        self.buy_trades = []
        self.sell_trades = []
        
        # 利潤統計
        self.total_profit = 0
        self.trades_executed = 0
        self.orders_placed = 0
        self.orders_cancelled = 0
        
        # 執行緒池用於後台任務
        self.executor = ThreadPoolExecutor(max_workers=3)
        
        # 等待WebSocket連接建立並進行初始化訂閲
        self._initialize_websocket()
        
        # 載入交易統計和歷史交易
        self._load_trading_stats()
        self._load_recent_trades()
        
        logger.info(f"初始化增強型馬丁策略 | 總資金: {total_capital_usdt} | 最大層級: {max_levels}")
        logger.info(f"基礎資產: {self.base_asset}, 報價資產: {self.quote_asset}")
        logger.info(f"基礎精度: {self.base_precision}, 報價精度: {self.quote_precision}")
        logger.info(f"最小訂單大小: {self.min_order_size}, 價格步長: {self.tick_size}")
        
    
    def _initialize_websocket(self):
        """等待WebSocket連接建立並進行初始化訂閲"""
        wait_time = 0
        max_wait_time = 10
        while not self.ws.connected and wait_time < max_wait_time:
            time.sleep(0.5)
            wait_time += 0.5
        
        if self.ws.connected:
            logger.info("WebSocket連接已建立，初始化行情和訂單更新...")
            
            ticker_subscribed = self.ws.subscribe_bookTicker()
            order_subscribed = self.subscribe_order_updates()

            if ticker_subscribed and order_subscribed:
                logger.info("✅ WebSocket 訂閲成功 (價格與訂單更新)")
            else:
                logger.warning("⚠️ WebSocket 訂閲部分失敗")
        else:
            logger.warning(f"WebSocket連接建立超時，將在運行過程中繼續嘗試連接")
    
    def _load_trading_stats(self):
        """從數據庫加載交易統計數據"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            # 查詢今天的統計數據
            stats = self.db.get_trading_stats(self.symbol, today)
            
            if stats and len(stats) > 0:
                stat = stats[0]
                self.maker_buy_volume = stat['maker_buy_volume']
                self.maker_sell_volume = stat['maker_sell_volume']
                self.taker_buy_volume = stat['taker_buy_volume']
                self.taker_sell_volume = stat['taker_sell_volume']
                self.total_profit = stat['realized_profit']
                self.total_fees = stat['total_fees']
                
                logger.info(f"已從數據庫加載今日交易統計")
                logger.info(f"Maker買入量: {self.maker_buy_volume}, Maker賣出量: {self.maker_sell_volume}")
                logger.info(f"Taker買入量: {self.taker_buy_volume}, Taker賣出量: {self.taker_sell_volume}")
                logger.info(f"已實現利潤: {self.total_profit}, 總手續費: {self.total_fees}")
            else:
                logger.info("今日無交易統計記錄，將創建新記錄")
        except Exception as e:
            logger.error(f"加載交易統計時出錯: {e}")
    
    def _load_recent_trades(self):
        """從數據庫加載歷史成交記錄"""
        try:
            # 獲取訂單歷史
            trades = self.db.get_order_history(self.symbol, 1000)
            trades_count = len(trades) if trades else 0
            
            if trades_count > 0:
                for side, quantity, price, maker, fee in trades:
                    quantity = float(quantity)
                    price = float(price)
                    fee = float(fee)
                    
                    if side == 'Bid':  # 買入
                        self.buy_trades.append((price, quantity))
                        self.total_bought += quantity
                        if maker:
                            self.maker_buy_volume += quantity
                        else:
                            self.taker_buy_volume += quantity
                    elif side == 'Ask':  # 賣出
                        self.sell_trades.append((price, quantity))
                        self.total_sold += quantity
                        if maker:
                            self.maker_sell_volume += quantity
                        else:
                            self.taker_sell_volume += quantity
                    
                    self.total_fees += fee
                
                logger.info(f"已從數據庫載入 {trades_count} 條歷史成交記錄")
                logger.info(f"總買入: {self.total_bought} {self.base_asset}, 總賣出: {self.total_sold} {self.base_asset}")
                logger.info(f"Maker買入: {self.maker_buy_volume} {self.base_asset}, Maker賣出: {self.maker_sell_volume} {self.base_asset}")
                logger.info(f"Taker買入: {self.taker_buy_volume} {self.base_asset}, Taker賣出: {self.taker_sell_volume} {self.base_asset}")
                
                # 計算精確利潤
                self.total_profit = self._calculate_db_profit()
                logger.info(f"計算得出已實現利潤: {self.total_profit:.8f} {self.quote_asset}")
                logger.info(f"總手續費: {self.total_fees:.8f} {self.quote_asset}")
            else:
                logger.info("數據庫中沒有歷史成交記錄，嘗試從API獲取")
                self._load_trades_from_api()
                
        except Exception as e:
            logger.error(f"載入歷史成交記錄時出錯: {e}")
            import traceback
            traceback.print_exc()
    
    def _load_trades_from_api(self):
        """從API加載歷史成交記錄"""
        from api.client import get_fill_history
        
        fill_history = get_fill_history(self.api_key, self.secret_key, self.symbol, 100)
        
        if isinstance(fill_history, dict) and "error" in fill_history:
            logger.error(f"載入成交記錄失敗: {fill_history['error']}")
            return
            
        if not fill_history:
            logger.info("沒有找到歷史成交記錄")
            return
        
        # 批量插入準備
        for fill in fill_history:
            price = float(fill.get('price', 0))
            quantity = float(fill.get('quantity', 0))
            side = fill.get('side')
            maker = fill.get('maker', False)
            fee = float(fill.get('fee', 0))
            fee_asset = fill.get('feeAsset', '')
            order_id = fill.get('orderId', '')
            
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
                'trade_type': 'manual'
            }
            
            # 插入數據庫
            self.db.insert_order(order_data)
            
            if side == 'Bid':  # 買入
                self.buy_trades.append((price, quantity))
                self.total_bought += quantity
                if maker:
                    self.maker_buy_volume += quantity
                else:
                    self.taker_buy_volume += quantity
            elif side == 'Ask':  # 賣出
                self.sell_trades.append((price, quantity))
                self.total_sold += quantity
                if maker:
                    self.maker_sell_volume += quantity
                else:
                    self.taker_sell_volume += quantity
            
            self.total_fees += fee
        
        if fill_history:
            logger.info(f"已從API載入並存儲 {len(fill_history)} 條歷史成交記錄")
            
            # 更新總計
            logger.info(f"總買入: {self.total_bought} {self.base_asset}, 總賣出: {self.total_sold} {self.base_asset}")
            logger.info(f"Maker買入: {self.maker_buy_volume} {self.base_asset}, Maker賣出: {self.maker_sell_volume} {self.base_asset}")
            logger.info(f"Taker買入: {self.taker_buy_volume} {self.base_asset}, Taker賣出: {self.taker_sell_volume} {self.base_asset}")
            
            # 計算精確利潤
            self.total_profit = self._calculate_db_profit()
            logger.info(f"計算得出已實現利潤: {self.total_profit:.8f} {self.quote_asset}")
            logger.info(f"總手續費: {self.total_fees:.8f} {self.quote_asset}")
    
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
        volatility = calculate_historical_volatility(self.symbol, period=24)
        if volatility > self.volatility_threshold:
            return 0.7
        return 1.0

    def allocate_funds(self):
        adjustment_factor = self._dynamic_size_adjustment()
        weights = [self.multiplier ** i for i in range(self.max_levels)]
        total_weight = sum(weights)
        return [
            (self.total_capital * (self.multiplier ** i) / total_weight) * adjustment_factor 
            for i in range(self.max_levels)
        ]

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
                    
                    # 安全地檢查訂單是否是重平衡訂單
                    try:
                        is_rebalance = self.db.is_rebalance_order(order_id, self.symbol)
                        if is_rebalance:
                            trade_type = 'rebalance'
                    except Exception as db_err:
                        logger.error(f"檢查重平衡訂單時出錯: {db_err}")
                    
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
    
    def _update_trading_stats(self):
        """更新每日交易統計數據"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            # 計算額外指標
            volatility = 0
            if self.ws and hasattr(self.ws, 'historical_prices'):
                volatility = calculate_volatility(self.ws.historical_prices)
            
            # 計算平均價差
            avg_spread = 0
            if self.ws and self.ws.bid_price and self.ws.ask_price:
                avg_spread = (self.ws.ask_price - self.ws.bid_price) / ((self.ws.ask_price + self.ws.bid_price) / 2) * 100
            
            # 準備統計數據
            stats_data = {
                'date': today,
                'symbol': self.symbol,
                'maker_buy_volume': self.maker_buy_volume,
                'maker_sell_volume': self.maker_sell_volume,
                'taker_buy_volume': self.taker_buy_volume,
                'taker_sell_volume': self.taker_sell_volume,
                'realized_profit': self.total_profit,
                'total_fees': self.total_fees,
                'net_profit': self.total_profit - self.total_fees,
                'avg_spread': avg_spread,
                'trade_count': self.trades_executed,
                'volatility': volatility
            }
            
            # 使用專門的函數來處理數據庫操作
            def safe_update_stats():
                try:
                    success = self.db.update_trading_stats(stats_data)
                    if not success:
                        logger.warning("更新交易統計失敗，下次再試")
                except Exception as db_err:
                    logger.error(f"更新交易統計時出錯: {db_err}")
            
            # 直接在當前線程執行，避免過多的並發操作
            safe_update_stats()
                
        except Exception as e:
            logger.error(f"更新交易統計數據時出錯: {e}")
            import traceback
            traceback.print_exc()
    
    def _calculate_average_buy_cost(self):
        """計算平均買入成本"""
        if not self.buy_trades:
            return 0
            
        total_buy_cost = sum(price * quantity for price, quantity in self.buy_trades)
        total_buy_quantity = sum(quantity for _, quantity in self.buy_trades)
        
        if not self.sell_trades or total_buy_quantity <= 0:
            return total_buy_cost / total_buy_quantity if total_buy_quantity > 0 else 0
        
        buy_queue = self.buy_trades.copy()
        consumed_cost = 0
        consumed_quantity = 0
        
        for _, sell_quantity in self.sell_trades:
            remaining_sell = sell_quantity
            
            while remaining_sell > 0 and buy_queue:
                buy_price, buy_quantity = buy_queue[0]
                matched_quantity = min(remaining_sell, buy_quantity)
                consumed_cost += buy_price * matched_quantity
                consumed_quantity += matched_quantity
                remaining_sell -= matched_quantity
                
                if matched_quantity >= buy_quantity:
                    buy_queue.pop(0)
                else:
                    buy_queue[0] = (buy_price, buy_quantity - matched_quantity)
        
        remaining_buy_quantity = total_buy_quantity - consumed_quantity
        remaining_buy_cost = total_buy_cost - consumed_cost
        
        if remaining_buy_quantity <= 0:
            if self.ws and self.ws.connected and self.ws.bid_price:
                return self.ws.bid_price
            return 0
        
        return remaining_buy_cost / remaining_buy_quantity
    
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

    def calculate_pnl(self):
        """計算已實現和未實現PnL"""
        # 總的已實現利潤
        realized_pnl = self._calculate_db_profit()
        
        # 本次執行的已實現利潤
        session_realized_pnl = self._calculate_session_profit()
        
        # 計算未實現利潤
        unrealized_pnl = 0
        net_position = self.total_bought - self.total_sold
        
        if net_position > 0:
            current_price = self.get_current_price()
            if current_price:
                avg_buy_cost = self._calculate_average_buy_cost()
                unrealized_pnl = (current_price - avg_buy_cost) * net_position
        
        # 返回總的PnL和本次執行的PnL
        return realized_pnl, unrealized_pnl, self.total_fees, realized_pnl - self.total_fees, session_realized_pnl, self.session_fees, session_realized_pnl - self.session_fees
    
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
    
    def get_market_depth(self):
        """獲取市場深度（優先使用WebSocket數據）"""
        self.check_ws_connection()
        bid_price, ask_price = None, None
        if self.ws and self.ws.connected:
            bid_price, ask_price = self.ws.get_bid_ask()
        
        if bid_price is None or ask_price is None:
            order_book = get_order_book(self.symbol)
            if isinstance(order_book, dict) and "error" in order_book:
                logger.error(f"獲取訂單簿失敗: {order_book['error']}")
                return None, None
            
            bids = order_book.get('bids', [])
            asks = order_book.get('asks', [])
            if not bids or not asks:
                return None, None
            
            highest_bid = float(bids[0][0]) if bids else None
            lowest_ask = float(asks[0][0]) if asks else None
            
            return highest_bid, lowest_ask
        
        return bid_price, ask_price
    
    def calculate_dynamic_spread(self):
        """計算動態價差基於市場情況"""
        base_spread = self.base_spread_percentage
        
        # 返回基礎價差，不再進行動態計算
        return base_spread
    
    def calculate_prices(self):
        """計算買賣訂單價格"""
        try:
            bid_price, ask_price = self.get_market_depth()
            if bid_price is None or ask_price is None:
                current_price = self.get_current_price()
                if current_price is None:
                    logger.error("無法獲取價格信息，無法設置訂單")
                    return None, None
                mid_price = current_price
            else:
                mid_price = (bid_price + ask_price) / 2
            
            logger.info(f"市場中間價: {mid_price}")
            
            # 使用基礎價差
            spread_percentage = self.base_spread_percentage
            exact_spread = mid_price * (spread_percentage / 100)
            
            base_buy_price = mid_price - (exact_spread / 2)
            base_sell_price = mid_price + (exact_spread / 2)
            
            base_buy_price = round_to_tick_size(base_buy_price, self.tick_size)
            base_sell_price = round_to_tick_size(base_sell_price, self.tick_size)
            
            actual_spread = base_sell_price - base_buy_price
            actual_spread_pct = (actual_spread / mid_price) * 100
            logger.info(f"使用的價差: {actual_spread_pct:.4f}% (目標: {spread_percentage}%), 絕對價差: {actual_spread}")
            
            # 計算梯度訂單價格
            buy_prices = []
            sell_prices = []
            
            # 優化梯度分佈：較小的梯度以提高成交率
            for i in range(self.max_orders):
                # 非線性遞增的梯度，靠近中間的訂單梯度小，越遠離中間梯度越大
                gradient_factor = (i ** 1.5) * 1.5
                
                buy_adjustment = gradient_factor * self.tick_size
                sell_adjustment = gradient_factor * self.tick_size
                
                buy_price = round_to_tick_size(base_buy_price - buy_adjustment, self.tick_size)
                sell_price = round_to_tick_size(base_sell_price + sell_adjustment, self.tick_size)
                
                buy_prices.append(buy_price)
                sell_prices.append(sell_price)
            
            final_spread = sell_prices[0] - buy_prices[0]
            final_spread_pct = (final_spread / mid_price) * 100
            logger.info(f"最終價差: {final_spread_pct:.4f}% (最低賣價 {sell_prices[0]} - 最高買價 {buy_prices[0]} = {final_spread})")
            
            return buy_prices, sell_prices
        
        except Exception as e:
            logger.error(f"計算價格時出錯: {str(e)}")
            return None, None
    
    def need_rebalance(self):
        """判斷是否需要重平衡倉位"""
        if self.total_bought == 0 and self.total_sold == 0:
            return False
        if self.total_bought == 0 or self.total_sold == 0:
            return True
        
        # 計算不平衡程度
        imbalance_percentage = abs(self.total_bought - self.total_sold) / max(self.total_bought, self.total_sold) * 100
        
        # 獲取淨倉位和方向
        net_position = self.total_bought - self.total_sold
        position_direction = 1 if net_position > 0 else -1 if net_position < 0 else 0
        
        logger.info(f"當前倉位: 買入 {self.total_bought} {self.base_asset}, 賣出 {self.total_sold} {self.base_asset}")
        logger.info(f"不平衡百分比: {imbalance_percentage:.2f}%")
        
        # 使用固定閾值
        return imbalance_percentage > self.rebalance_threshold
    
    def rebalance_position(self):
        """重平衡倉位"""
        logger.info("開始重新平衡倉位...")
        self.check_ws_connection()
        
        imbalance = self.total_bought - self.total_sold
        bid_price, ask_price = self.get_market_depth()
        
        if bid_price is None or ask_price is None:
            current_price = self.get_current_price()
            if current_price is None:
                logger.error("無法獲取價格，無法重新平衡")
                return
            bid_price = current_price * 0.998
            ask_price = current_price * 1.002
        
        if imbalance > 0:
            # 淨多頭，需要賣出
            quantity = round_to_precision(imbalance, self.base_precision)
            if quantity < self.min_order_size:
                logger.info(f"不平衡量 {quantity} 低於最小訂單大小 {self.min_order_size}，不進行重新平衡")
                return
            
            # 設定賣出價格
            price_factor = 1.0
            sell_price = round_to_tick_size(bid_price * price_factor, self.tick_size)
            logger.info(f"執行重新平衡: 賣出 {quantity} {self.base_asset} @ {sell_price}")
            
            # 構建訂單
            order_details = {
                "orderType": "Limit",
                "price": str(sell_price),
                "quantity": str(quantity),
                "side": "Ask",
                "symbol": self.symbol,
                "timeInForce": "GTC",
                "postOnly": True
            }
            
            # 嘗試執行訂單
            result = execute_order(self.api_key, self.secret_key, order_details)
            
            # 處理可能的錯誤
            if isinstance(result, dict) and "error" in result:
                error_msg = str(result['error'])
                logger.error(f"重新平衡賣單執行失敗: {error_msg}")
                
                # 如果因為訂單會立即成交而失敗，嘗試不使用postOnly
                if "POST_ONLY_TAKER" in error_msg or "Order would immediately match" in error_msg:
                    logger.info("嘗試使用非postOnly訂單進行重新平衡...")
                    order_details.pop("postOnly", None)
                    result = execute_order(self.api_key, self.secret_key, order_details)
                    
                    if isinstance(result, dict) and "error" in result:
                        logger.error(f"非postOnly賣單執行失敗: {result['error']}")
                    else:
                        logger.info(f"非postOnly賣單執行成功，價格: {sell_price}")
                        # 記錄這是一個重平衡訂單
                        if 'id' in result:
                            self.db.record_rebalance_order(result['id'], self.symbol)
            else:
                logger.info(f"重新平衡賣單已提交，作為maker")
                # 記錄這是一個重平衡訂單
                if 'id' in result:
                    self.db.record_rebalance_order(result['id'], self.symbol)
            
        elif imbalance < 0:
            # 淨空頭，需要買入
            quantity = round_to_precision(abs(imbalance), self.base_precision)
            if quantity < self.min_order_size:
                logger.info(f"不平衡量 {quantity} 低於最小訂單大小 {self.min_order_size}，不進行重新平衡")
                return
            
            # 設定買入價格
            price_factor = 1.0
            buy_price = round_to_tick_size(ask_price * price_factor, self.tick_size)
            logger.info(f"執行重新平衡: 買入 {quantity} {self.base_asset} @ {buy_price}")
            
            # 構建訂單
            order_details = {
                "orderType": "Limit",
                "price": str(buy_price),
                "quantity": str(quantity),
                "side": "Bid",
                "symbol": self.symbol,
                "timeInForce": "GTC",
                "postOnly": True
            }
            
            # 嘗試執行訂單
            result = execute_order(self.api_key, self.secret_key, order_details)
            
            # 處理可能的錯誤
            if isinstance(result, dict) and "error" in result:
                error_msg = str(result['error'])
                logger.error(f"重新平衡買單執行失敗: {error_msg}")
                
                # 如果因為訂單會立即成交而失敗，嘗試不使用postOnly
                if "POST_ONLY_TAKER" in error_msg or "Order would immediately match" in error_msg:
                    logger.info("嘗試使用非postOnly訂單進行重新平衡...")
                    order_details.pop("postOnly", None)
                    result = execute_order(self.api_key, self.secret_key, order_details)
                    
                    if isinstance(result, dict) and "error" in result:
                        logger.error(f"非postOnly買單執行失敗: {result['error']}")
                    else:
                        logger.info(f"非postOnly買單執行成功，價格: {buy_price}")
                        # 記錄這是一個重平衡訂單
                        if 'id' in result:
                            self.db.record_rebalance_order(result['id'], self.symbol)
            else:
                logger.info(f"重平衡買單已提交，作為maker")
                # 記錄這是一個重平衡訂單
                if 'id' in result:
                    self.db.record_rebalance_order(result['id'], self.symbol)
        
        logger.info("倉位重新平衡完成")
    
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

    def place_martingale_orders(self):
        """馬丁策略的下單方法（模仿做市下單邏輯）"""
        self.check_ws_connection()
        self.cancel_existing_orders()

        current_price = self.get_current_price()
        if not current_price:
            logger.error("無法獲取當前價格，跳過下單")
            return

        allocated_funds = self.allocate_funds()
        logger.info(f"資金分配完成 | 各層金額: {allocated_funds}")

        orders_placed = 0

        for layer in range(self.max_layers):
            target_price = current_price * (1 - self.price_step_down * layer)
            target_price = round_to_tick_size(target_price, self.tick_size)

            quote_amount = allocated_funds[layer]
            quantity = round_to_precision(quote_amount / target_price, self.base_precision)

            # 建構下單參數
            order_details = {
                "orderType": "Limit" if not self.use_market_order else "Market",
                "price": str(target_price) if not self.use_market_order else None,
                "quantity": str(quantity),
                "side": "Bid",
                "symbol": self.symbol.replace("_", "-").upper(),
                "timeInForce": "IOC",
            }

            logger.info(f"📤 提交第 {layer+1} 層訂單: {order_details}")
            result = execute_order(self.api_key, self.secret_key, order_details)

            if isinstance(result, dict) and "error" in result:
                logger.warning(f"❌ 層 {layer} 下單失敗: {result['error']}")
            else:
                logger.info(f"✅ 層 {layer} 下單成功: {result}")
                self.orders_placed += 1
                orders_placed += 1

        logger.info(f"📊 本次共下單 {orders_placed} 層馬丁訂單")

    def _adjust_quantity(self, quantity, side):
        """根据余额动态调整订单量"""
        balance = self.get_balance(self.base_asset if side == 'Ask' else self.quote_asset)
        max_qty = balance / (self.martingale_multiplier ** self.current_layer)
        return min(quantity, max_qty)

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
    
    def estimate_profit(self):
        """估算潛在利潤"""
        # 計算活躍買賣單的平均價格
        avg_buy_price = 0
        total_buy_quantity = 0
        for order in self.active_buy_orders:
            price = float(order.get('price', 0))
            quantity = float(order.get('quantity', 0))
            avg_buy_price += price * quantity
            total_buy_quantity += quantity
        
        if total_buy_quantity > 0:
            avg_buy_price /= total_buy_quantity
        
        avg_sell_price = 0
        total_sell_quantity = 0
        for order in self.active_sell_orders:
            price = float(order.get('price', 0))
            quantity = float(order.get('quantity', 0))
            avg_sell_price += price * quantity
            total_sell_quantity += quantity
        
        if total_sell_quantity > 0:
            avg_sell_price /= total_sell_quantity
        
        # 計算總的PnL和本次執行的PnL
        realized_pnl, unrealized_pnl, total_fees, net_pnl, session_realized_pnl, session_fees, session_net_pnl = self.calculate_pnl()
        
        # 計算活躍訂單的潛在利潤
        if avg_buy_price > 0 and avg_sell_price > 0:
            spread = avg_sell_price - avg_buy_price
            spread_percentage = (spread / avg_buy_price) * 100
            min_quantity = min(total_buy_quantity, total_sell_quantity)
            potential_profit = spread * min_quantity
            
            logger.info(f"估算利潤: 買入均價 {avg_buy_price:.8f}, 賣出均價 {avg_sell_price:.8f}")
            logger.info(f"價差: {spread:.8f} ({spread_percentage:.2f}%), 潛在利潤: {potential_profit:.8f} {self.quote_asset}")
            logger.info(f"已實現利潤(總): {realized_pnl:.8f} {self.quote_asset}")
            logger.info(f"總手續費(總): {total_fees:.8f} {self.quote_asset}")
            logger.info(f"凈利潤(總): {net_pnl:.8f} {self.quote_asset}")
            logger.info(f"未實現利潤: {unrealized_pnl:.8f} {self.quote_asset}")
            
            # 打印本次執行的統計信息
            logger.info(f"\n---本次執行統計---")
            logger.info(f"本次執行已實現利潤: {session_realized_pnl:.8f} {self.quote_asset}")
            logger.info(f"本次執行手續費: {session_fees:.8f} {self.quote_asset}")
            logger.info(f"本次執行凈利潤: {session_net_pnl:.8f} {self.quote_asset}")
            
            session_buy_volume = sum(qty for _, qty in self.session_buy_trades)
            session_sell_volume = sum(qty for _, qty in self.session_sell_trades)
            
            logger.info(f"本次執行買入量: {session_buy_volume} {self.base_asset}, 賣出量: {session_sell_volume} {self.base_asset}")
            logger.info(f"本次執行Maker買入: {self.session_maker_buy_volume} {self.base_asset}, Maker賣出: {self.session_maker_sell_volume} {self.base_asset}")
            logger.info(f"本次執行Taker買入: {self.session_taker_buy_volume} {self.base_asset}, Taker賣出: {self.session_taker_sell_volume} {self.base_asset}")
            
        else:
            logger.info(f"無法估算潛在利潤: 缺少活躍的買/賣訂單")
            logger.info(f"已實現利潤(總): {realized_pnl:.8f} {self.quote_asset}")
            logger.info(f"總手續費(總): {total_fees:.8f} {self.quote_asset}")
            logger.info(f"凈利潤(總): {net_pnl:.8f} {self.quote_asset}")
            logger.info(f"未實現利潤: {unrealized_pnl:.8f} {self.quote_asset}")
            
            # 打印本次執行的統計信息
            logger.info(f"\n---本次執行統計---")
            logger.info(f"本次執行已實現利潤: {session_realized_pnl:.8f} {self.quote_asset}")
            logger.info(f"本次執行手續費: {session_fees:.8f} {self.quote_asset}")
            logger.info(f"本次執行凈利潤: {session_net_pnl:.8f} {self.quote_asset}")
            
            session_buy_volume = sum(qty for _, qty in self.session_buy_trades)
            session_sell_volume = sum(qty for _, qty in self.session_sell_trades)
            
            logger.info(f"本次執行買入量: {session_buy_volume} {self.base_asset}, 賣出量: {session_sell_volume} {self.base_asset}")
            logger.info(f"本次執行Maker買入: {self.session_maker_buy_volume} {self.base_asset}, Maker賣出: {self.session_maker_sell_volume} {self.base_asset}")
            logger.info(f"本次執行Taker買入: {self.session_taker_buy_volume} {self.base_asset}, Taker賣出: {self.session_taker_sell_volume} {self.base_asset}")
    
    def print_trading_stats(self):
        """打印交易統計報表"""
        try:
            logger.info("\n=== 馬丁策略交易統計 ===")
            logger.info(f"交易對: {self.symbol}")
            
            today = datetime.now().strftime('%Y-%m-%d')
            
            # 獲取今天的統計數據
            today_stats = self.db.get_trading_stats(self.symbol, today)
            
            if today_stats and len(today_stats) > 0:
                stat = today_stats[0]
                maker_buy = stat['maker_buy_volume']
                maker_sell = stat['maker_sell_volume']
                taker_buy = stat['taker_buy_volume']
                taker_sell = stat['taker_sell_volume']
                profit = stat['realized_profit']
                fees = stat['total_fees']
                net = stat['net_profit']
                avg_spread = stat['avg_spread']
                volatility = stat['volatility']
                
                total_volume = maker_buy + maker_sell + taker_buy + taker_sell
                maker_percentage = ((maker_buy + maker_sell) / total_volume * 100) if total_volume > 0 else 0
                
                logger.info(f"\n今日統計 ({today}):")
                logger.info(f"Maker買入量: {maker_buy} {self.base_asset}")
                logger.info(f"Maker賣出量: {maker_sell} {self.base_asset}")
                logger.info(f"Taker買入量: {taker_buy} {self.base_asset}")
                logger.info(f"Taker賣出量: {taker_sell} {self.base_asset}")
                logger.info(f"總成交量: {total_volume} {self.base_asset}")
                logger.info(f"Maker佔比: {maker_percentage:.2f}%")
                logger.info(f"平均價差: {avg_spread:.4f}%")
                logger.info(f"波動率: {volatility:.4f}%")
                logger.info(f"毛利潤: {profit:.8f} {self.quote_asset}")
                logger.info(f"總手續費: {fees:.8f} {self.quote_asset}")
                logger.info(f"凈利潤: {net:.8f} {self.quote_asset}")
            
            # 獲取所有時間的總計
            all_time_stats = self.db.get_all_time_stats(self.symbol)
            
            if all_time_stats:
                total_maker_buy = all_time_stats['total_maker_buy']
                total_maker_sell = all_time_stats['total_maker_sell']
                total_taker_buy = all_time_stats['total_taker_buy']
                total_taker_sell = all_time_stats['total_taker_sell']
                total_profit = all_time_stats['total_profit']
                total_fees = all_time_stats['total_fees']
                total_net = all_time_stats['total_net_profit']
                avg_spread = all_time_stats['avg_spread_all_time']
                
                total_volume = total_maker_buy + total_maker_sell + total_taker_buy + total_taker_sell
                maker_percentage = ((total_maker_buy + total_maker_sell) / total_volume * 100) if total_volume > 0 else 0
                
                logger.info(f"\n累計統計:")
                logger.info(f"Maker買入量: {total_maker_buy} {self.base_asset}")
                logger.info(f"Maker賣出量: {total_maker_sell} {self.base_asset}")
                logger.info(f"Taker買入量: {total_taker_buy} {self.base_asset}")
                logger.info(f"Taker賣出量: {total_taker_sell} {self.base_asset}")
                logger.info(f"總成交量: {total_volume} {self.base_asset}")
                logger.info(f"Maker佔比: {maker_percentage:.2f}%")
                logger.info(f"平均價差: {avg_spread:.4f}%")
                logger.info(f"毛利潤: {total_profit:.8f} {self.quote_asset}")
                logger.info(f"總手續費: {total_fees:.8f} {self.quote_asset}")
                logger.info(f"凈利潤: {total_net:.8f} {self.quote_asset}")
            
            # 添加本次執行的統計
            session_buy_volume = sum(qty for _, qty in self.session_buy_trades)
            session_sell_volume = sum(qty for _, qty in self.session_sell_trades)
            session_total_volume = session_buy_volume + session_sell_volume
            session_maker_volume = self.session_maker_buy_volume + self.session_maker_sell_volume
            session_maker_percentage = (session_maker_volume / session_total_volume * 100) if session_total_volume > 0 else 0
            session_profit = self._calculate_session_profit()
            
            logger.info(f"\n本次執行統計 (從 {self.session_start_time.strftime('%Y-%m-%d %H:%M:%S')} 開始):")
            logger.info(f"Maker買入量: {self.session_maker_buy_volume} {self.base_asset}")
            logger.info(f"Maker賣出量: {self.session_maker_sell_volume} {self.base_asset}")
            logger.info(f"Taker買入量: {self.session_taker_buy_volume} {self.base_asset}")
            logger.info(f"Taker賣出量: {self.session_taker_sell_volume} {self.base_asset}")
            logger.info(f"總成交量: {session_total_volume} {self.base_asset}")
            logger.info(f"Maker佔比: {session_maker_percentage:.2f}%")
            logger.info(f"毛利潤: {session_profit:.8f} {self.quote_asset}")
            logger.info(f"總手續費: {self.session_fees:.8f} {self.quote_asset}")
            logger.info(f"凈利潤: {(session_profit - self.session_fees):.8f} {self.quote_asset}")
                
            # 查詢前10筆最新成交
            recent_trades = self.db.get_recent_trades(self.symbol, 10)
            
            if recent_trades and len(recent_trades) > 0:
                logger.info("\n最近10筆成交:")
                for i, trade in enumerate(recent_trades):
                    maker_str = "Maker" if trade['maker'] else "Taker"
                    logger.info(f"{i+1}. {trade['timestamp']} - {trade['side']} {trade['quantity']} @ {trade['price']} ({maker_str}) 手續費: {trade['fee']:.8f}")
        
        except Exception as e:
            logger.error(f"打印交易統計時出錯: {e}")
    
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
        logger.info(f"運行時間: {duration_seconds} 秒, 間隔: {interval_seconds} 秒")
        
        # 重置本次執行的統計數據
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
        report_interval = 300  # 5分鐘打印一次報表
        
        try:
            # 先確保 WebSocket 連接可用
            connection_status = self.check_ws_connection()
            if connection_status:
                
                
                # 檢查並確保所有數據流訂閲
                
                if "bookTicker" not in self.ws.subscriptions:
                    self.ws.subscribe_bookTicker()
                if f"account.orderUpdate.{self.symbol}" not in self.ws.subscriptions:
                    self.subscribe_order_updates()
            
            while time.time() - start_time < duration_seconds:
                iteration += 1
                current_time = time.time()
                logger.info(f"\n=== 第 {iteration} 次迭代 ===")
                logger.info(f"時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # 檢查連接並在必要時重連
                connection_status = self.check_ws_connection()
                
                # 如果連接成功，檢查並確保所有流訂閲
                if connection_status:
                    # 重新訂閲必要的數據流
                    self._ensure_data_streams()
                
                # 檢查訂單成交情況
                self.check_order_fills()
                
                # 檢查是否需要重平衡倉位
                if self.need_rebalance():
                    self.rebalance_position()
                
                # 下限價單
                self.place_martingale_orders()
                
                # 估算利潤
                self.estimate_profit()
                
                # 定期打印交易統計報表
                if current_time - last_report_time >= report_interval:
                    self.print_trading_stats()
                    last_report_time = current_time
                
                # 計算總的PnL和本次執行的PnL
                realized_pnl, unrealized_pnl, total_fees, net_pnl, session_realized_pnl, session_fees, session_net_pnl = self.calculate_pnl()
                
                logger.info(f"\n統計信息:")
                logger.info(f"總交易次數: {self.trades_executed}")
                logger.info(f"總下單次數: {self.orders_placed}")
                logger.info(f"總取消訂單次數: {self.orders_cancelled}")
                logger.info(f"買入總量: {self.total_bought} {self.base_asset}")
                logger.info(f"賣出總量: {self.total_sold} {self.base_asset}")
                logger.info(f"Maker買入: {self.maker_buy_volume} {self.base_asset}, Maker賣出: {self.maker_sell_volume} {self.base_asset}")
                logger.info(f"Taker買入: {self.taker_buy_volume} {self.base_asset}, Taker賣出: {self.taker_sell_volume} {self.base_asset}")
                logger.info(f"總手續費: {total_fees:.8f} {self.quote_asset}")
                logger.info(f"已實現利潤: {realized_pnl:.8f} {self.quote_asset}")
                logger.info(f"凈利潤: {net_pnl:.8f} {self.quote_asset}")
                logger.info(f"未實現利潤: {unrealized_pnl:.8f} {self.quote_asset}")
                logger.info(f"WebSocket連接狀態: {'已連接' if self.ws and self.ws.is_connected() else '未連接'}")
                
                # 打印本次執行的統計數據
                logger.info(f"\n---本次執行統計---")
                session_buy_volume = sum(qty for _, qty in self.session_buy_trades)
                session_sell_volume = sum(qty for _, qty in self.session_sell_trades)
                logger.info(f"買入量: {session_buy_volume} {self.base_asset}, 賣出量: {session_sell_volume} {self.base_asset}")
                logger.info(f"Maker買入: {self.session_maker_buy_volume} {self.base_asset}, Maker賣出: {self.session_maker_sell_volume} {self.base_asset}")
                logger.info(f"Taker買入: {self.session_taker_buy_volume} {self.base_asset}, Taker賣出: {self.session_taker_sell_volume} {self.base_asset}")
                logger.info(f"本次執行已實現利潤: {session_realized_pnl:.8f} {self.quote_asset}")
                logger.info(f"本次執行手續費: {session_fees:.8f} {self.quote_asset}")
                logger.info(f"本次執行凈利潤: {session_net_pnl:.8f} {self.quote_asset}")
                
                wait_time = interval_seconds
                logger.info(f"等待 {wait_time} 秒後進行下一次迭代...")
                time.sleep(wait_time)
                
            # 結束運行時打印最終報表
            logger.info("\n=== 馬丁策略運行結束 ===")
            self.print_trading_stats()
            
            # 打印本次執行的最終統計摘要
            logger.info("\n=== 本次執行統計摘要 ===")
            session_buy_volume = sum(qty for _, qty in self.session_buy_trades)
            session_sell_volume = sum(qty for _, qty in self.session_sell_trades)
            session_total_volume = session_buy_volume + session_sell_volume
            session_profit = self._calculate_session_profit()
            
            # 計算執行時間
            td = datetime.now() - self.session_start_time
            total_seconds = int(td.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            run_time = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            logger.info(f"執行時間: {run_time}")
            
            logger.info(f"總成交量: {session_total_volume} {self.base_asset}")
            logger.info(f"買入量: {session_buy_volume} {self.base_asset}, 賣出量: {session_sell_volume} {self.base_asset}")
            logger.info(f"Maker買入: {self.session_maker_buy_volume} {self.base_asset}, Maker賣出: {self.session_maker_sell_volume} {self.base_asset}")
            logger.info(f"Taker買入: {self.session_taker_buy_volume} {self.base_asset}, Taker賣出: {self.session_taker_sell_volume} {self.base_asset}")
            logger.info(f"已實現利潤: {session_profit:.8f} {self.quote_asset}")
            logger.info(f"總手續費: {self.session_fees:.8f} {self.quote_asset}")
            logger.info(f"凈利潤: {(session_profit - self.session_fees):.8f} {self.quote_asset}")
            
            if session_total_volume > 0:
                logger.info(f"每單位成交量利潤: {((session_profit - self.session_fees) / session_total_volume):.8f} {self.quote_asset}/{self.base_asset}")
        
        except KeyboardInterrupt:
            logger.info("\n用户中斷，停止馬丁策略")
            
            # 中斷時也打印本次執行的統計數據
            logger.info("\n=== 本次執行統計摘要(中斷) ===")
            session_buy_volume = sum(qty for _, qty in self.session_buy_trades)
            session_sell_volume = sum(qty for _, qty in self.session_sell_trades)
            session_total_volume = session_buy_volume + session_sell_volume
            session_profit = self._calculate_session_profit()
            
            # 計算執行時間
            td = datetime.now() - self.session_start_time
            total_seconds = int(td.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            run_time = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            logger.info(f"執行時間: {run_time}")
            
            logger.info(f"總成交量: {session_total_volume} {self.base_asset}")
            logger.info(f"買入量: {session_buy_volume} {self.base_asset}, 賣出量: {session_sell_volume} {self.base_asset}")
            logger.info(f"Maker買入: {self.session_maker_buy_volume} {self.base_asset}, Maker賣出: {self.session_maker_sell_volume} {self.base_asset}")
            logger.info(f"Taker買入: {self.session_taker_buy_volume} {self.base_asset}, Taker賣出: {self.session_taker_sell_volume} {self.base_asset}")
            logger.info(f"已實現利潤: {session_profit:.8f} {self.quote_asset}")
            logger.info(f"總手續費: {self.session_fees:.8f} {self.quote_asset}")
            logger.info(f"凈利潤: {(session_profit - self.session_fees):.8f} {self.quote_asset}")
            
            if session_total_volume > 0:
                logger.info(f"每單位成交量利潤: {((session_profit - self.session_fees) / session_total_volume):.8f} {self.quote_asset}/{self.base_asset}")
        
        finally:
            logger.info("取消所有未成交訂單...")
            self.cancel_existing_orders()
            
            # 關閉 WebSocket
            if self.ws:
                self.ws.close()
            
            # 關閉數據庫連接
            if self.db:
                self.db.close()
                logger.info("數據庫連接已關閉")
