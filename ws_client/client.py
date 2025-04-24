"""
WebSocket客户端模塊
"""
import json
import time
import threading
import websocket as ws
from typing import Dict, List, Tuple, Any, Optional, Callable
from config import WS_URL, DEFAULT_WINDOW
from api.auth import create_signature
from api.client import get_order_book
from utils.helpers import calculate_volatility
from logger import setup_logger


logger = setup_logger("backpack_ws")

class BackpackWebSocket:
    def __init__(self, api_key, secret_key, symbol, on_message_callback=None, auto_reconnect=True, proxy=None):
        """
        初始化WebSocket客户端
        
        Args:
            api_key: API密鑰
            secret_key: API密鑰
            symbol: 交易對符號
            on_message_callback: 消息回調函數
            auto_reconnect: 是否自動重連
            proxy:  wss代理 支持格式为 http://user:pass@host:port/ 或者 http://host:port

        """
        self.api_key = api_key
        self.secret_key = secret_key
        self.symbol = symbol.upper().replace('-', '_')
        self.ws = None
        self.on_message_callback = on_message_callback
        self.connected = False
        self.last_price = None
        self.bid_price = None
        self.ask_price = None
        self.orderbook = {"bids": [], "asks": []}
        self.order_updates = []
        self.historical_prices = []  # 儲存歷史價格用於計算波動率
        self.max_price_history = 100  # 最多儲存的價格數量
        

        # 重連相關參數
        self.auto_reconnect = auto_reconnect
        self.reconnect_delay = 1
        self.max_reconnect_delay = 30
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.running = False
        self.ws_thread = None
        
        # 記錄已訂閲的頻道
        self.subscriptions = []
        
        # 添加WebSocket執行緒鎖
        self.ws_lock = threading.Lock()
        
        # 添加心跳檢測
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 30
        self.heartbeat_thread = None

        # 添加代理参数
        self.proxy = proxy

    def initialize_orderbook(self):
        """佔位函數（或用於未來擴展）"""
        logger.info("📄 跳過初始化訂單簿（馬丁策略不使用）")
        return True
    
    def add_price_to_history(self, price):
        """添加價格到歷史記錄用於計算波動率"""
        if price:
            self.historical_prices.append(price)
            # 保持歷史記錄在設定長度內
            if len(self.historical_prices) > self.max_price_history:
                self.historical_prices = self.historical_prices[-self.max_price_history:]
    
    def get_volatility(self, window=20):
        """獲取當前波動率"""
        return calculate_volatility(self.historical_prices, window)
    
    def start_heartbeat(self):
        """開始心跳檢測線程"""
        if self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_check, daemon=True)
            self.heartbeat_thread.start()
    
    def _heartbeat_check(self):
        """定期檢查WebSocket連接狀態並在需要時重連"""
        while self.running:
            current_time = time.time()
            time_since_last_heartbeat = current_time - self.last_heartbeat
            
            if time_since_last_heartbeat > self.heartbeat_interval * 2:
                logger.warning(f"心跳檢測超時 ({time_since_last_heartbeat:.1f}秒)，嘗試重新連接")
                self.reconnect()
                
            time.sleep(5)  # 每5秒檢查一次
        
    def connect(self):
        """建立WebSocket連接"""
        with self.ws_lock:
            self.running = True
            self.reconnect_attempts = 0
            ws.enableTrace(False)  # 使用 ws.enableTrace 而不是 websocket.enableTrace
            self.ws = ws.WebSocketApp(  # 同樣使用 ws.WebSocketApp
                WS_URL,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_ping=self.on_ping,
                on_pong=self.on_pong
            )
            self.ws_thread = threading.Thread(target=self.ws_run_forever)
            self.ws_thread.daemon = True
            self.ws_thread.start()
            
            # 啟動心跳檢測
            self.start_heartbeat()
    
    def ws_run_forever(self):
        try:
            # 確保在運行前檢查socket狀態
            if hasattr(self.ws, 'sock') and self.ws.sock and self.ws.sock.connected:
                logger.debug("發現socket已經打開，跳過run_forever")
                return

            proxy_type=None
            http_proxy_auth=None
            http_proxy_host=None
            http_proxy_port=None
            if self.proxy and 3<=len(self.proxy.split(":"))<=4:
                arrs=self.proxy.split(":")
                proxy_type = arrs[0]
                arrs[1]=arrs[1][2:] #去掉 //
                if len(arrs)==3:
                    http_proxy_host = arrs[1]
                else:
                    password,http_proxy_host = arrs[2].split("@")
                    http_proxy_auth=(arrs[1],password)
                http_proxy_port = arrs[-1]

            # 添加ping_interval和ping_timeout參數
            self.ws.run_forever(ping_interval=self.heartbeat_interval, ping_timeout=10, http_proxy_auth=http_proxy_auth, http_proxy_host=http_proxy_host, http_proxy_port=http_proxy_port, proxy_type=proxy_type)

        except Exception as e:
            logger.error(f"WebSocket運行時出錯: {e}")
        finally:
            with self.ws_lock:
                if self.running and self.auto_reconnect and not self.connected:
                    self.reconnect()
    
    def on_pong(self, ws, message):
        """處理pong響應"""
        self.last_heartbeat = time.time()
        
    def reconnect(self):
        """完全斷開並重新建立WebSocket連接"""
        with self.ws_lock:
            if not self.running or self.reconnect_attempts >= self.max_reconnect_attempts:
                logger.warning(f"重連次數超過上限 ({self.max_reconnect_attempts})，停止重連")
                return False

            self.reconnect_attempts += 1
            delay = min(self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)), self.max_reconnect_delay)
            
            logger.info(f"嘗試第 {self.reconnect_attempts} 次重連，等待 {delay} 秒...")
            time.sleep(delay)
            
            # 確保完全斷開連接前先標記連接狀態
            self.connected = False
            
            # 完全斷開並清理之前的WebSocket連接
            if self.ws:
                try:
                    # 顯式設置內部標記表明這是用户主動關閉
                    if hasattr(self.ws, '_closed_by_me'):
                        self.ws._closed_by_me = True
                    
                    # 關閉WebSocket
                    self.ws.close()
                    self.ws.keep_running = False
                    
                    # 強制關閉socket
                    if hasattr(self.ws, 'sock') and self.ws.sock:
                        self.ws.sock.close()
                        self.ws.sock = None
                except Exception as e:
                    logger.error(f"關閉之前的WebSocket連接時出錯: {e}")
                
                # 給系統更多時間完全關閉連接
                time.sleep(1.0)  # 增加等待時間
                self.ws = None
                
            # 確保舊的線程已終止
            if self.ws_thread and self.ws_thread.is_alive():
                try:
                    # 更長的超時等待線程終止
                    self.ws_thread.join(timeout=2)
                except Exception as e:
                    logger.error(f"等待舊線程終止時出錯: {e}")
            
            # 重置所有相關狀態
            self.ws_thread = None
            self.subscriptions = []  # 清空訂閲列表，以便重新訂閲
            
            # 創建全新的WebSocket連接
            ws.enableTrace(False)
            self.ws = ws.WebSocketApp(
                WS_URL,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_ping=self.on_ping,
                on_pong=self.on_pong
            )
            
            # 創建新線程
            self.ws_thread = threading.Thread(target=self.ws_run_forever)
            self.ws_thread.daemon = True
            self.ws_thread.start()
            
            # 更新最後心跳時間，避免重連後立即觸發心跳檢測
            self.last_heartbeat = time.time()
            
            return True
        
    def on_ping(self, ws, message):
        """處理ping消息"""
        try:
            self.last_heartbeat = time.time()
            if ws and hasattr(ws, 'sock') and ws.sock:
                ws.sock.pong(message)
            else:
                logger.debug("無法迴應ping：WebSocket或sock為None")
        except Exception as e:
            logger.debug(f"迴應ping失敗: {e}")
        
    def on_open(self, ws):
        """WebSocket打開時的處理"""
        logger.info("WebSocket連接已建立")
        self.connected = True
        self.reconnect_attempts = 0
        self.last_heartbeat = time.time()
        
        # 添加短暫延遲確保連接穩定
        time.sleep(0.5)
        
        # 初始化訂單簿
        orderbook_initialized = self.initialize_orderbook()
        
        # 如果初始化成功，訂閲深度和行情數據
        if orderbook_initialized:
            if "bookTicker" in self.subscriptions or not self.subscriptions:
                self.subscribe_bookTicker()
            
            
        
        # 重新訂閲私有訂單更新流
        for sub in self.subscriptions:
            if sub.startswith("account."):
                self.private_subscribe(sub)
    
    def subscribe_bookTicker(self):
        """訂閲最優價格"""
        logger.info(f"訂閲 {self.symbol} 的bookTicker...")
        if not self.connected or not self.ws:
            logger.warning("WebSocket未連接，無法訂閲bookTicker")
            return False
            
        try:
            message = {
                "method": "SUBSCRIBE",
                "params": [f"bookTicker.{self.symbol}"]
            }
            self.ws.send(json.dumps(message))
            if "bookTicker" not in self.subscriptions:
                self.subscriptions.append("bookTicker")
            return True
        except Exception as e:
            logger.error(f"訂閲bookTicker失敗: {e}")
            return False
    
        
    def private_subscribe(self, stream: str) -> bool:
        """訂閱私有數據流 (需簽名驗證)"""
        if not self.connected or not self.ws:
            logger.warning("WebSocket未連接，無法訂閱私有流")
            return False

        try:
            timestamp = str(int(time.time() * 1000))
            window = DEFAULT_WINDOW
            message_to_sign = f"instruction=subscribe&timestamp={timestamp}&window={window}"
            signature = create_signature(self.secret_key, message_to_sign)

            if not signature:
                logger.error("❌ 簽名創建失敗，無法訂閱私有流")
                return False

            payload = {
                "method": "SUBSCRIBE",
                "params": [stream],
                "signature": [self.api_key, signature, timestamp, window]
            }

            self.ws.send(json.dumps(payload))
            logger.info(f"✅ 已訂閱私有數據流: {stream}")

            if stream not in self.subscriptions:
                self.subscriptions.append(stream)

            return True
        except Exception as e:
            logger.error(f"❌ 訂閱私有數據流失敗: {e}")
            return False
    
    def on_message(self, ws, message):
        """處理WebSocket消息"""
        try:
            data = json.loads(message)
            
            # 處理ping pong消息
            if isinstance(data, dict) and data.get("ping"):
                pong_message = {"pong": data.get("ping")}
                if self.ws and self.connected:
                    self.ws.send(json.dumps(pong_message))
                    self.last_heartbeat = time.time()
                return
            
            if "stream" in data and "data" in data:
                stream = data["stream"]
                event_data = data["data"]
                
                # 處理bookTicker
                if stream.startswith("bookTicker."):
                    if 'b' in event_data and 'a' in event_data:
                        self.bid_price = float(event_data['b'])
                        self.ask_price = float(event_data['a'])
                        self.last_price = (self.bid_price + self.ask_price) / 2
                        # 記錄歷史價格用於計算波動率
                        self.add_price_to_history(self.last_price)
                
                # 處理depth
                elif stream.startswith("depth."):
                    if 'b' in event_data and 'a' in event_data:
                        self._update_orderbook(event_data)
                
                # 訂單更新數據流
                elif stream.startswith("account.orderUpdate."):
                    self.order_updates.append(event_data)
                    
                if self.on_message_callback:
                    self.on_message_callback(stream, event_data)
            
        except Exception as e:
            logger.error(f"處理WebSocket消息時出錯: {e}")
    
    def _update_orderbook(self, data):
        """更新訂單簿（優化處理速度）"""
        # 處理買單更新
        if 'b' in data:
            for bid in data['b']:
                price = float(bid[0])
                quantity = float(bid[1])
                
                # 使用二分查找來優化插入位置查找
                if quantity == 0:
                    # 移除價位
                    self.orderbook["bids"] = [b for b in self.orderbook["bids"] if b[0] != price]
                else:
                    # 先查找是否存在相同價位
                    found = False
                    for i, b in enumerate(self.orderbook["bids"]):
                        if b[0] == price:
                            self.orderbook["bids"][i] = [price, quantity]
                            found = True
                            break
                    
                    # 如果不存在，插入並保持排序
                    if not found:
                        self.orderbook["bids"].append([price, quantity])
                        # 按價格降序排序
                        self.orderbook["bids"] = sorted(self.orderbook["bids"], key=lambda x: x[0], reverse=True)
        
        # 處理賣單更新
        if 'a' in data:
            for ask in data['a']:
                price = float(ask[0])
                quantity = float(ask[1])
                
                if quantity == 0:
                    # 移除價位
                    self.orderbook["asks"] = [a for a in self.orderbook["asks"] if a[0] != price]
                else:
                    # 先查找是否存在相同價位
                    found = False
                    for i, a in enumerate(self.orderbook["asks"]):
                        if a[0] == price:
                            self.orderbook["asks"][i] = [price, quantity]
                            found = True
                            break
                    
                    # 如果不存在，插入並保持排序
                    if not found:
                        self.orderbook["asks"].append([price, quantity])
                        # 按價格升序排序
                        self.orderbook["asks"] = sorted(self.orderbook["asks"], key=lambda x: x[0])
    
    def on_error(self, ws, error):
        """處理WebSocket錯誤"""
        logger.error(f"WebSocket發生錯誤: {error}")
        self.last_heartbeat = 0  # 強制觸發重連
    
    def on_close(self, ws, close_status_code, close_msg):
        """處理WebSocket關閉"""
        previous_connected = self.connected
        self.connected = False
        logger.info(f"WebSocket連接已關閉: {close_msg if close_msg else 'No message'} (狀態碼: {close_status_code if close_status_code else 'None'})")
        
        # 清理當前socket資源
        if hasattr(ws, 'sock') and ws.sock:
            try:
                ws.sock.close()
                ws.sock = None
            except Exception as e:
                logger.debug(f"關閉socket時出錯: {e}")
        
        if close_status_code == 1000 or getattr(ws, '_closed_by_me', False):
            logger.info("WebSocket正常關閉，不進行重連")
        elif previous_connected and self.running and self.auto_reconnect:
            logger.info("WebSocket非正常關閉，將自動重連")
            # 使用線程觸發重連，避免在回調中直接重連
            threading.Thread(target=self.reconnect, daemon=True).start()
    
    def close(self):
        """完全關閉WebSocket連接"""
        with self.ws_lock:
            logger.info("主動關閉WebSocket連接...")
            self.running = False
            self.connected = False
            
            # 停止心跳檢測線程
            if self.heartbeat_thread and self.heartbeat_thread.is_alive():
                try:
                    self.heartbeat_thread.join(timeout=1)
                except Exception:
                    pass
            self.heartbeat_thread = None
            
            if self.ws:
                # 標記為主動關閉
                if not hasattr(self.ws, '_closed_by_me'):
                    self.ws._closed_by_me = True
                else:
                    self.ws._closed_by_me = True
                    
                try:
                    # 關閉WebSocket
                    self.ws.close()
                    self.ws.keep_running = False
                    
                    # 強制關閉socket
                    if hasattr(self.ws, 'sock') and self.ws.sock:
                        self.ws.sock.close()
                except Exception as e:
                    logger.error(f"關閉WebSocket時出錯: {e}")
                
                # 等待完全關閉
                time.sleep(0.5)
                self.ws = None
            
            # 清理線程
            if self.ws_thread and self.ws_thread.is_alive():
                try:
                    self.ws_thread.join(timeout=1)
                except Exception:
                    pass
            self.ws_thread = None
            
            # 重置訂閲狀態
            self.subscriptions = []
            
            logger.info("WebSocket連接已完全關閉")
    
    def get_current_price(self):
        """獲取當前價格"""
        return self.last_price
    
    def get_bid_ask(self):
        """獲取買賣價"""
        return self.bid_price, self.ask_price
    
    def get_orderbook(self):
        """獲取訂單簿"""
        return self.orderbook

    def is_connected(self):
        """檢查連接狀態"""
        if not self.connected:
            return False
        if not self.ws:
            return False
        if not hasattr(self.ws, 'sock') or not self.ws.sock:
            return False
        
        # 檢查socket是否連接
        try:
            return self.ws.sock.connected
        except:
            return False
    
    def get_liquidity_profile(self, depth_percentage=0.01):
        """分析市場流動性特徵"""
        if not self.orderbook["bids"] or not self.orderbook["asks"]:
            return None
        
        mid_price = (self.bid_price + self.ask_price) / 2 if self.bid_price and self.ask_price else None
        if not mid_price:
            return None
        
        # 計算價格範圍
        min_price = mid_price * (1 - depth_percentage)
        max_price = mid_price * (1 + depth_percentage)
        
        # 分析買賣單流動性
        bid_volume = sum(qty for price, qty in self.orderbook["bids"] if price >= min_price)
        ask_volume = sum(qty for price, qty in self.orderbook["asks"] if price <= max_price)
        
        # 計算買賣比例
        ratio = bid_volume / ask_volume if ask_volume > 0 else float('inf')
        
        # 買賣壓力差異
        imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume) if (bid_volume + ask_volume) > 0 else 0
        
        return {
            'bid_volume': bid_volume,
            'ask_volume': ask_volume,
            'volume_ratio': ratio,
            'imbalance': imbalance,
            'mid_price': mid_price
        }
