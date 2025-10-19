# api/client.py
import time
import json
import base64
import hmac
import hashlib
import requests
import logging
import nacl.signing
import aiohttp
import asyncio
import websockets
from datetime import datetime


# 配置常量
API_URL = "https://api.backpack.exchange"
API_VERSION = "v1"
DEFAULT_WINDOW = 5000

logger = logging.getLogger(__name__)

class BackpackAPIClient:
    def __init__(self, api_key=None, secret_key=None, symbol=None):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = API_URL  # 確保使用正確的變數名
        self.default_window = DEFAULT_WINDOW
        self.symbol = symbol
        self.time_offset = 0
        self.logger = logging.getLogger(__name__)
        self._sync_server_time()
        
    
    def _sync_server_time(self):
        """同步服務器時間"""
        try:
            response = requests.get(f"{self.base_url}/api/v1/time")
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and 'serverTime' in data:
                    self.time_offset = data['serverTime'] - int(time.time() * 1000)
                    return True
            return False
        except Exception as e:
            logger.error(f"時間同步失敗: {str(e)}")
            return False

    def _generate_signature(self, params, instruction="orderExecute"):
        try:
            timestamp = str(int(time.time() * 1000))
            window = str(self.default_window)
            
            # 排序參數並轉換為查詢字符串
            if isinstance(params, dict):
                # 轉換布爾值
                params_copy = params.copy()
                for k, v in params_copy.items():
                    if isinstance(v, bool):
                        params_copy[k] = str(v).lower()
                
                # 按字母順序排序
                sorted_params = sorted(params_copy.items())
                param_str = "&".join([f"{k}={v}" for k, v in sorted_params])
            else:
                param_str = params
            
            # 構建簽名消息
            message = f"instruction={instruction}&{param_str}&timestamp={timestamp}&window={window}"
            
            # 使用PyNaCl生成ED25519簽名
            import nacl.signing
            import base64
            
            # 解碼私鑰
            private_key_bytes = base64.b64decode(self.secret_key)
            signing_key = nacl.signing.SigningKey(private_key_bytes)
            
            # 簽名
            signed = signing_key.sign(message.encode('ascii'))
            signature = base64.b64encode(signed.signature).decode()
            
            # 返回簽名、時間戳和窗口
            return {
                "signature": signature,
                "timestamp": timestamp,
                "window": window
            }
        except Exception as e:
            self.logger.error(f"簽名生成失敗: {str(e)}")
            return None
    
    def _generate_headers(self, instruction, params):
        sig_data = self._generate_signature(params, instruction)
        if not sig_data:
            return {}
        
        headers = {
            "X-API-KEY": self.api_key,
            "X-SIGNATURE": sig_data["signature"],
            "X-TIMESTAMP": sig_data["timestamp"],
            "X-WINDOW": sig_data["window"],
            "Content-Type": "application/json"
        }
        return headers
        
    async def public_request(self, endpoint, params=None):
        """發送公共API請求"""
        try:
            url = f"{self.base_url}/api/v1/{endpoint}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        self.logger.error(f"公共請求失敗: {response.status}, {await response.text()}")
                        return None
        except Exception as e:
            self.logger.error(f"公共請求異常: {e}")
            return None
        
    async def get_order(self, order_id, symbol):
        """獲取訂單狀態"""
        try:
            # 嘗試獲取單個訂單
            endpoint = "/api/v1/order"
            params = {"orderId": order_id, "symbol": symbol}
            instruction = "orderQuery"
            
            headers = self._generate_headers(instruction, params)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}{endpoint}", params=params, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 404:
                        # 如果訂單不存在，嘗試從訂單歷史中查詢
                        return await self.get_order_from_history(order_id, symbol)
                    else:
                        error_msg = f"狀態碼: {response.status}, 消息: {await response.text()}"
                        self.logger.warning(f"獲取訂單失敗: {error_msg}")
                        return None
        except Exception as e:
            self.logger.error(f"獲取訂單異常: {str(e)}")
            return None
        
    async def get_order_from_history(self, order_id, symbol):
        """從訂單歷史中查詢訂單"""
        try:
            endpoint = "/api/v1/orders/history"
            params = {"orderId": order_id, "symbol": symbol}
            instruction = "orderHistoryQueryAll"
            
            headers = self._generate_headers(instruction, params)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}{endpoint}", params=params, headers=headers) as response:
                    if response.status == 200:
                        orders = await response.json()
                        for order in orders:
                            if order.get('id') == order_id:
                                return order
                    return None
        except Exception as e:
            self.logger.error(f"獲取訂單歷史異常: {str(e)}")
            return None
        
    async def get_all_orders(self, symbol):
        """獲取所有訂單（包括活動和歷史）"""
        try:
            # 先獲取活動訂單
            active_orders = await self.get_active_orders(symbol)
            
            # 再獲取歷史訂單
            history_orders = await self.get_order_history(symbol)
            
            # 合併結果
            return active_orders + history_orders
        except Exception as e:
            self.logger.error(f"獲取所有訂單異常: {str(e)}")
            return []
    
    async def get_ticker(self, symbol):
        """獲取指定交易對的行情信息"""
        try:
            # 直接使用requests而非依賴public_request
            url = f"{self.base_url}/api/v1/ticker"
            params = {"symbol": symbol}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        self.logger.error(f"獲取行情失敗: {response.status}, {await response.text()}")
                        return None
        except Exception as e:
            self.logger.error(f"獲取行情異常: {e}")
            return None
    
    async def place_order(self, symbol, side, order_type, price=None, size=None):
        """兼容性方法，內部調用execute_order"""
        order_details = {
            "symbol": symbol,
            "side": side,
            "orderType": order_type
        }
        
        if order_type.lower() == "limit":
            order_details["price"] = str(price)
            order_details["quantity"] = str(size)
            order_details["timeInForce"] = "GTC"
        else:  # Market
            order_details["quantity"] = str(size)
        
        return await self.execute_order(order_details)
    
    def get_market_limits(self, symbol):
        """獲取市場限制"""
        endpoint = "/api/v1/markets"
        try:
            response = requests.get(f"{self.base_url}{endpoint}")
            if response.status_code == 200:
                normalized_symbol = symbol.replace('-', '_').upper()
                for market in response.json():
                    if market.get('symbol') == normalized_symbol:
                        return {
                            'base_precision': int(market.get('basePrecision', 8)),
                            'quote_precision': int(market.get('quotePrecision', 8)),
                            'min_order_size': float(market.get('minOrderSize', 0.00001)),
                            'tick_size': float(market.get('tickSize', 0.0001))
                        }
                logger.error(f"未找到交易對 {normalized_symbol}")
            return {}
        except Exception as e:
            logger.error(f"市場限制解析異常: {str(e)}")
            return {}
    
    async def execute_order(self, order_details):
        """執行訂單（異步方法）"""
        endpoint = "/api/v1/order"
        instruction = "orderExecute"
        
        # 確保交易對格式正確
        order_details['symbol'] = order_details['symbol'].replace('-', '_').upper()
        
        # 市價單處理
        if order_details.get('orderType') == 'Market':
            order_details.pop('price', None)
            if 'quoteQuantity' in order_details:
                order_details['quoteQuantity'] = str(order_details['quoteQuantity'])
        
        # 限價單處理
        else:
            if 'price' in order_details:
                order_details['price'] = str(order_details['price'])
            if 'quantity' in order_details:
                order_details['quantity'] = str(order_details['quantity'])
        
        # 布爾值處理 - 嘗試不同格式
        if 'postOnly' in order_details:
            # 移除postOnly參數，看看是否能解決問題
            del order_details['postOnly']
        
        # 生成請求頭
        headers = self._generate_headers(instruction, order_details)
        
        try:
            # 使用aiohttp進行異步請求
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}{endpoint}",
                    json=order_details,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_msg = f"狀態碼: {response.status}, 消息: {await response.text()}"
                        self.logger.warning(f"請求失敗 (1/3): {error_msg}")
                        return {"error": error_msg}
                    
        except Exception as e:
            self.logger.error(f"訂單執行失敗: {str(e)}")
            return {"error": str(e)}
    
    def get_balance(self, asset=None):
        """獲取賬戶餘額"""
        endpoint = "/api/v1/balance"
        instruction = "balanceQuery"
        params = {}
        if asset:
            params["asset"] = asset
        
        headers = self._generate_headers(instruction, params)
        
        try:
            response = requests.get(
                f"{self.base_url}{endpoint}",
                headers=headers,
                params=params
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                error_msg = f"狀態碼: {response.status_code}, 消息: {response.text}"
                logger.warning(f"請求失敗: {error_msg}")
                return {"error": error_msg}
                
        except Exception as e:
            logger.error(f"獲取餘額失敗: {str(e)}")
            return {"error": str(e)}
        
    async def get_order_history(self, symbol, order_id=None):
        """獲取訂單歷史"""
        try:
            endpoint = "/api/v1/order/history"  # 嘗試這個端點
            params = {"symbol": symbol}
            if order_id:
                params["orderId"] = order_id
            
            instruction = "orderHistoryQuery"  # 使用orderHistoryQuery而不是orderHistoryQueryAll
            
            # 生成請求頭
            headers = self._generate_headers(instruction, params)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}{endpoint}",
                    params=params,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.logger.info(f"獲取訂單歷史成功: {result}")
                        return result
                    else:
                        error_msg = f"狀態碼: {response.status}, 消息: {await response.text()}"
                        self.logger.warning(f"獲取訂單歷史失敗: {error_msg}")
                        return None
        except Exception as e:
            self.logger.error(f"獲取訂單歷史異常: {str(e)}")
            return None

    
    def get_open_orders(self, symbol=None):
        """獲取未成交訂單"""
        endpoint = "/api/v1/orders"
        instruction = "orderQueryAll"
        params = {}
        if symbol:
            params["symbol"] = symbol.replace('-', '_').upper()
        
        headers = self._generate_headers(instruction, params)
        
        try:
            response = requests.get(
                f"{self.base_url}{endpoint}",
                headers=headers,
                params=params
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                error_msg = f"狀態碼: {response.status_code}, 消息: {response.text}"
                logger.warning(f"請求失敗: {error_msg}")
                return {"error": error_msg}
                
        except Exception as e:
            logger.error(f"獲取未成交訂單失敗: {str(e)}")
            return {"error": str(e)}
        
    def get_order_book(symbol, limit=20):
        """獲取市場深度"""
        endpoint = f"/api/{API_VERSION}/depth"
        params = {"symbol": symbol, "limit": str(limit)}
        return make_request("GET", endpoint, params=params)
    
    async def cancel_all_orders(self, symbol):
        """取消指定交易對的所有未成交訂單"""
        try:
            endpoint = "/api/v1/orders"
            payload = {"symbol": symbol}
            instruction = "orderCancelAll"
            
            # 生成請求頭
            headers = self._generate_headers(instruction, payload)
            
            # 使用aiohttp進行異步請求
            async with aiohttp.ClientSession() as session:
                async with session.delete(
                    f"{self.base_url}{endpoint}",
                    json=payload,  # 使用json參數
                    headers=headers
                ) as response:
                    if response.status == 200:
                        return await response.json()  # 確保返回的是協程
                    else:
                        error_msg = f"狀態碼: {response.status}, 消息: {await response.text()}"
                        self.logger.warning(f"取消所有訂單失敗: {error_msg}")
                        return None
                                                        
        except Exception as e:
            self.logger.error(f"取消所有訂單異常: {str(e)}")
            return None
        
    async def cancel_order(self, order_id, symbol):
        """取消指定ID的訂單"""
        try:
            endpoint = "/api/v1/order"
            payload = {
                "symbol": symbol,
                "orderId": order_id
            }
            instruction = "orderCancel"
            
            # 生成請求頭
            headers = self._generate_headers(instruction, payload)
            
            # 使用aiohttp進行異步請求
            async with aiohttp.ClientSession() as session:
                async with session.delete(
                    f"{self.base_url}{endpoint}",
                    json=payload,  # 使用json參數
                    headers=headers
                ) as response:
                    if response.status == 200:
                        return await response.json()  # 確保返回的是協程
                    else:
                        error_msg = f"狀態碼: {response.status}, 消息: {await response.text()}"
                        self.logger.warning(f"取消訂單失敗: {error_msg}")
                        return None
                                                    
        except Exception as e:
            self.logger.error(f"取消訂單異常: {str(e)}")
            return None
    
    async def get_fill_history(self, symbol, order_id=None):
        """獲取成交歷史"""
        try:
            endpoint = "/wapi/v1/history/fills"  # 成交歷史端點
            params = {}
            if symbol:
                params["symbol"] = symbol
            if order_id:
                params["orderId"] = order_id
            
            instruction = "fillHistoryQueryAll"
            
            # 生成請求頭
            headers = self._generate_headers(instruction, params)
            
            self.logger.info(f"獲取成交歷史，參數: {params}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}{endpoint}",
                    params=params,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.logger.info(f"獲取成交歷史成功: {result}")
                        return result
                    else:
                        error_msg = f"狀態碼: {response.status}, 消息: {await response.text()}"
                        self.logger.warning(f"獲取成交歷史失敗: {error_msg}")
                        return None
        except Exception as e:
            self.logger.error(f"獲取成交歷史異常: {str(e)}")
            return None
        
    async def get_market_info(self, symbol):
        """獲取市場資訊，包括精度"""
        try:
            endpoint = "/api/v1/market"
            params = {"symbol": symbol}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}{endpoint}", params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        self.logger.error(f"獲取市場資訊失敗: {response.status}, {await response.text()}")
                        return None
        except Exception as e:
            self.logger.error(f"獲取市場資訊異常: {e}")
            return None
        
    
    
    async def connect_websocket(self, symbol, callback=None):
        """建立WebSocket連接並訂閱訂單更新"""
        try:
            ws_url = "wss://ws.backpack.exchange"
            self.logger.info(f"正在連接WebSocket: {ws_url}")
            
            async with websockets.connect(ws_url) as websocket:
                # 生成訂閱參數
                timestamp = int(time.time() * 1000)
                window = 5000  # 默認窗口值
                
                # 準備訂閱數據
                params = [f"account.orderUpdate.{symbol}"]
                
                # 計算簽名
                signature = self._generate_signature({}, timestamp, "subscribe", window)
                
                subscription_data = {
                    "method": "SUBSCRIBE",
                    "params": [
                        f"{channel}.{symbol}",
                        self.api_key,
                        signature,
                        str(timestamp),
                        str(window)
                    ]
                }
                
                # 發送訂閱請求
                await websocket.send(json.dumps(subscription_data))
                self.logger.info(f"已訂閱訂單更新: {params}")
                
                # 處理接收到的消息
                while True:
                    response = await websocket.recv()
                    data = json.loads(response)
                    self.logger.info(f"收到WebSocket消息: {data}")
                    
                    # 處理訂單成交消息
                    if data.get("e") == "orderFill":
                        self.logger.info(f"訂單成交: {data}")
                        if callback:
                            await callback(data)
        except Exception as e:
            self.logger.error(f"WebSocket連接錯誤: {e}")
            
    async def get_fill_history(self, symbol, order_id=None):
        """獲取成交歷史"""
        try:
            endpoint = "/api/v1/history/fills"
            params = {"symbol": symbol}
            if order_id:
                params["orderId"] = order_id
            
            instruction = "fillHistoryQueryAll"
            
            # 生成請求頭
            headers = self._generate_headers(instruction, params)
            
            self.logger.info(f"獲取成交歷史，參數: {params}")
            
            # 添加HTTP請求部分
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}{endpoint}",
                    params=params,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.logger.info(f"獲取成交歷史成功: {result}")
                        return result
                    else:
                        error_msg = f"狀態碼: {response.status}, 消息: {await response.text()}"
                        self.logger.warning(f"獲取成交歷史失敗: {error_msg}")
                        return None
        except Exception as e:
            self.logger.error(f"獲取成交歷史異常: {str(e)}")
            return None
            
    async def get_positions(self, symbol=None):
        """獲取當前持倉"""
        try:
            endpoint = "/api/v1/positions"
            params = {}
            if symbol:
                params["symbol"] = symbol
            
            instruction = "positionQuery"
            
            headers = self._generate_headers(instruction, params)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}{endpoint}",
                    params=params,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        positions = await response.json()
                        self.logger.info(f"當前持倉: {positions}")
                        return positions
                    else:
                        error_msg = f"狀態碼: {response.status}, 消息: {await response.text()}"
                        self.logger.warning(f"獲取持倉失敗: {error_msg}")
                        return None
        except Exception as e:
            self.logger.error(f"獲取持倉異常: {str(e)}")
            return None
        
    async def get_account_balance(self, asset="USDC"):
        """獲取賬戶餘額"""
        try:
            endpoint = "/api/v1/balance"
            params = {"asset": asset}
            instruction = "balanceQuery"
            
            headers = self._generate_headers(instruction, params)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}{endpoint}",
                    params=params,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_msg = f"狀態碼: {response.status}, 消息: {await response.text()}"
                        self.logger.warning(f"獲取賬戶餘額失敗: {error_msg}")
                        return None
        except Exception as e:
            self.logger.error(f"獲取賬戶餘額異常: {str(e)}")
            return None
        
    def connect_websocket(self, symbol, callback=None):
        """建立WebSocket連接並訂閱訂單更新"""
        def on_message(ws, message):
            """處理接收到的WebSocket消息"""
            data = json.loads(message)
            self.logger.info(f"收到WebSocket消息: {data}")
            
            # 處理訂單成交消息
            if data.get("e") == "orderFill":
                self.logger.info(f"訂單成交: {data}")
                if callback:
                    # 使用asyncio.create_task處理異步回調
                    import asyncio
                    asyncio.create_task(callback(data))
        
        def on_error(ws, error):
            """處理WebSocket錯誤"""
            self.logger.error(f"WebSocket錯誤: {error}")
        
        def on_close(ws, close_status_code, close_msg):
            """處理WebSocket連接關閉"""
            self.logger.info(f"WebSocket連接關閉: {close_status_code} {close_msg}")
        
        def on_open(ws):
            """處理WebSocket連接建立"""
            self.logger.info("WebSocket連接已建立")
            
            # 生成訂閱參數
            timestamp = int(time.time() * 1000)
            window = self.default_window
            
            # 準備訂閱數據
            params = [f"account.orderUpdate.{symbol}" if symbol else "account.orderUpdate"]
            
            # 計算簽名
            signature = self._generate_signature({}, timestamp, "subscribe", window)
            
            subscription_data = {
                "method": "SUBSCRIBE",
                "params": params,
                "signature": [
                    self.api_key,
                    signature,
                    str(timestamp),
                    str(window)
                ]
            }
            
            # 發送訂閱請求
            ws.send(json.dumps(subscription_data))
            self.logger.info(f"已訂閱訂單更新: {params}")
        
        # 創建WebSocket連接
        ws_url = "wss://ws.backpack.exchange"
        ws = websocket.WebSocketApp(ws_url,
                                  on_open=on_open,
                                  on_message=on_message,
                                  on_error=on_error,
                                  on_close=on_close)
        
        # 在新線程中運行WebSocket連接
        wst = threading.Thread(target=ws.run_forever)
        wst.daemon = True  # 設置為守護線程，主線程結束時自動結束
        wst.start()
        
        return ws
    
    

