"""
API請求客戶端模塊
"""
import json
import time
import requests
import os
import base64
import requests
import logging
from typing import Dict, Any, Optional, List, Union
from .auth import create_signature
from config import API_URL, API_VERSION, DEFAULT_WINDOW
from logger import setup_logger
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
MARKET_ENDPOINT = "https://api.backpack.exchange/api/v1/markets"


logger = setup_logger("api.client")
BASE_URL = "https://api.backpack.exchange"
logger = logging.getLogger(__name__)

class BackpackAPIClient:
    def __init__(self, api_key=None, secret_key=None):
        self.api_key = api_key or os.getenv('API_KEY')
        self.secret_key = secret_key or os.getenv('API_SECRET')
        self.base_url = "https://api.backpack.exchange"
        self.time_offset = 0
        self._sync_server_time()  # 初始化時自動同步時間
    
    def _sync_server_time(self):
        """強化版時間同步"""
        try:
            response = requests.get(f"{self.base_url}/api/v1/time")
            # 處理不同格式的返回值
            if isinstance(response.json(), dict):
                server_time = response.json().get('serverTime', int(time.time()*1000))
            else:
                server_time = int(response.json())
            
            local_time = int(time.time() * 1000)
            self.time_offset = server_time - local_time
            logger.info(f"時間同步成功 | 本地:{local_time} | 服務器:{server_time} | 偏移:{self.time_offset}ms")
        except Exception as e:
            logger.error(f"時間同步異常: {str(e)}")
            self.time_offset = 0  # 降級使用本地時間

    def get_klines(self, symbol: str, interval: str = "1h", limit: int = 100) -> list:
        """獲取K線數據（支持多時間週期）"""
        try:
            # 時間間隔映射表
            interval_map = {
                "1m": "1m", "5m": "5m", "15m": "15m",
                "30m": "30m", "1h": "1H", "4h": "4H",
                "1d": "1D", "1w": "1W", "1month": "1M"
            }
        
            # 構建請求參數
            params = {
                "symbol": symbol.replace('-', '_'),
                "interval": interval_map.get(interval, '1H'),
                "limit": limit
            }
        
            # 生成簽名頭部
            headers = self._generate_headers("klinesQuery", params)
        
            # 發送請求
            response = requests.get(
                f"{self.base_url}/api/{API_VERSION}/klines",
                params=params,
                headers=headers
            )
        
            # 處理響應
            if response.status_code == 200:
                return [{
                    'timestamp': int(kline[0]),
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                } for kline in response.json()]
            return []
        except Exception as e:
            logger.error(f"K線數據獲取異常: {str(e)}")
            return []
    
    def get_market_limits(self, symbol: str) -> dict:
        """獲取交易對限制信息（修正結構完整性）"""
        print("🟢 get_market_limits() 被呼叫")
        endpoint = f"/api/v1/markets"
        try:
            response = requests.get(MARKET_ENDPOINT)
            response.raise_for_status()
            normalized_symbol = symbol.replace('-', '_').upper()
            
            # 添加調試日誌
            logger.debug(f"API原始響應: {response.text}")
            
            for market in response.json():
                if market.get('symbol') == normalized_symbol:
                    result = {
                        "base_precision": int(market.get("quantityPrecision", 6)),
                        "quote_precision": int(market.get("pricePrecision", 6)),
                        "min_order_size": float(market.get("minNotional", 0)),
                        "tick_size": float(market.get("tickSize", 0.0001)) 
                    }
                    logger.info(f"✅ 取得市場限制成功: {symbol} -> {result}")
                    print(f"✅ 取得市場限制: {result}")
                    return result

            logger.error(f"未找到交易對 {symbol}")
            return None  # ⚠️ 別 return 字串！
        except Exception as e:
            logger.error(f"市場限制查詢異常: {e}")
            return None
    
    # 在api/client.py中添加全局格式转换方法
    def normalize_symbol(symbol: str) -> str:
        """统一交易对格式为 API 标准格式（大写短横线）"""
        return symbol.replace('_', '-').upper( )

    def get_open_orders(self, symbol: str = None) -> list:
        """獲取未成交訂單"""
        endpoint = f"/api/{API_VERSION}/orders"
        params = {}
        if symbol:
            params["symbol"] = symbol.replace('-', '_')
        
        try:
            headers = self._generate_headers("orderQueryAll", params)
            response = requests.get(
                f"{self.base_url}{endpoint}",
                params=params,
                headers=headers
            )
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            logger.error(f"獲取未成交訂單失敗: {str(e)}")
            return []
        
    def place_martingale_orders(self):
        # ...計算target_price和allocated_funds...
        quantity = allocated_funds[layer] / target_price
        quantity = round_to_precision(quantity, self.base_precision)
    
        # 強制符合交易所精度要求
        quantity_str = f"{quantity:.{self.base_precision}f}"
        quantity = float(quantity_str)
    
        if quantity < self.min_order_size:
            logger.warning(f"層級{layer}訂單量{quantity}低於最小值{self.min_order_size}，跳過")
        
        
    def _generate_headers(self, instruction: str, params: dict = None) -> dict:
        """生成API簽名頭部"""
        timestamp = str(int(time.time() * 1000) + self.time_offset)
        window = "5000"
        message = f"instruction={instruction}"
        
        if params:
            sorted_params = sorted(params.items())
            param_str = "&".join([f"{k}={v}" for k, v in sorted_params])
            message += f"&{param_str}"
        message += f"&timestamp={timestamp}&window={window}"

        try:
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
            private_key = Ed25519PrivateKey.from_private_bytes(
                base64.b64decode(self.secret_key)
            )
            signature = base64.b64encode(private_key.sign(message.encode())).decode()
            return {
                "X-API-KEY": self.api_key,
                "X-SIGNATURE": signature,
                "X-TIMESTAMP": timestamp,
                "X-WINDOW": window
            }
        except Exception as e:
            logger.error(f"簽名生成失敗: {str(e)}")
            return {}
        
    def execute_order(self, order_details: dict) -> dict:
        """执行订单"""
        endpoint = f"/api/{API_VERSION}/order"
        try:
            headers = self._generate_headers("orderExecute", order_details)
            response = requests.post(
                f"{self.base_url}{endpoint}",
                json=order_details,
                headers=headers
            )
            return response.json()
        except Exception as e:
            logger.error(f"订单执行失败: {str(e)}")
            return {"error": str(e)}





def get_balance(self, asset: str) -> dict:
    """獲取餘額"""
    headers = self._generate_headers("balanceQuery")
    response = requests.get(f"{self.base_url}/api/v1/capital", headers=headers)
    # ...處理響應...

def execute_order(self, order_details: dict) -> dict:
    """下單"""
    headers = self._generate_headers("orderExecute", order_details)
    response = requests.post(f"{self.base_url}/api/v1/order", json=order_details, headers=headers)
    # ...處理響應...

def _generate_headers(self, instruction: str, params=None) -> dict:
    """生成簽名頭部"""
    timestamp = str(int(time.time()*1000) + self.time_offset)
    # ...簽名生成邏輯...

def make_request(method: str, endpoint: str, api_key=None, secret_key=None, instruction=None, 
                 params=None, data=None, retry_count=3) -> Dict:
    """
    執行API請求，支持重試機制
    
    Args:
        method: HTTP方法 (GET, POST, DELETE)
        endpoint: API端點
        api_key: API密鑰
        secret_key: API密鑰
        instruction: API指令
        params: 查詢參數
        data: 請求體數據
        retry_count: 重試次數
        
    Returns:
        API響應數據
    """
    url = f"{API_URL}{endpoint}"
    headers = {'Content-Type': 'application/json'}
    
    # 構建簽名信息（如需要）
    if api_key and secret_key and instruction:
        timestamp = str(int(time.time() * 1000))
        window = DEFAULT_WINDOW
        
        # 構建簽名消息
        query_string = ""
        if params:
            sorted_params = sorted(params.items())
            query_string = "&".join([f"{k}={v}" for k, v in sorted_params])
        
        sign_message = f"instruction={instruction}"
        if query_string:
            sign_message += f"&{query_string}"
        sign_message += f"&timestamp={timestamp}&window={window}"
    
        signature = create_signature(secret_key, sign_message)
        if not signature:
            return {"error": "簽名創建失敗"}
        
        headers.update({
            'X-API-KEY': api_key,
            'X-SIGNATURE': signature,
            'X-TIMESTAMP': timestamp,
            'X-WINDOW': window
        })
    
    # 添加查詢參數到URL
    if params and method.upper() in ['GET', 'DELETE']:
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        url += f"?{query_string}"
    
    # 實施重試機制
    for attempt in range(retry_count):
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, timeout=10)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, data=json.dumps(data) if data else None, timeout=10)
            elif method.upper() == 'DELETE':
                response = requests.delete(url, headers=headers, data=json.dumps(data) if data else None, timeout=10)
            else:
                return {"error": f"不支持的請求方法: {method}"}
            
            # 處理響應
            if response.status_code in [200, 201]:
                return response.json() if response.text.strip() else {}
            elif response.status_code == 429:  # 速率限制
                wait_time = 1 * (2 ** attempt)  # 指數退避
                logger.warning(f"遇到速率限制，等待 {wait_time} 秒後重試")
                time.sleep(wait_time)
                continue
            else:
                error_msg = f"狀態碼: {response.status_code}, 消息: {response.text}"
                if attempt < retry_count - 1:
                    logger.warning(f"請求失敗 ({attempt+1}/{retry_count}): {error_msg}")
                    time.sleep(1)  # 簡單重試延遲
                    continue
                return {"error": error_msg}
        
        except requests.exceptions.Timeout:
            if attempt < retry_count - 1:
                logger.warning(f"請求超時 ({attempt+1}/{retry_count})，重試中...")
                continue
            return {"error": "請求超時"}
        except requests.exceptions.ConnectionError:
            if attempt < retry_count - 1:
                logger.warning(f"連接錯誤 ({attempt+1}/{retry_count})，重試中...")
                time.sleep(2)  # 連接錯誤通常需要更長等待
                continue
            return {"error": "連接錯誤"}
        except Exception as e:
            if attempt < retry_count - 1:
                logger.warning(f"請求異常 ({attempt+1}/{retry_count}): {str(e)}，重試中...")
                continue
            return {"error": f"請求失敗: {str(e)}"}
    
    return {"error": "達到最大重試次數"}

# 各API端點函數
def get_deposit_address(api_key, secret_key, blockchain):
    """獲取存款地址"""
    endpoint = f"/wapi/{API_VERSION}/capital/deposit/address"
    instruction = "depositAddressQuery"
    params = {"blockchain": blockchain}
    return make_request("GET", endpoint, api_key, secret_key, instruction, params)

def get_balance(api_key, secret_key):
    """獲取賬戶餘額"""
    endpoint = f"/api/{API_VERSION}/capital"
    instruction = "balanceQuery"
    return make_request("GET", endpoint, api_key, secret_key, instruction)

def execute_order(api_key, secret_key, order_details):
    """執行訂單"""
    endpoint = f"/api/{API_VERSION}/order"
    instruction = "orderExecute"
    
    # 提取所有參數用於簽名
    params = {
        "orderType": order_details["orderType"],
        "price": order_details.get("price", "0"),
        "quantity": order_details["quantity"],
        "side": order_details["side"],
        "symbol": order_details["symbol"],
        "timeInForce": order_details.get("timeInForce", "GTC")
    }
    
    # 添加可選參數
    for key in ["postOnly", "reduceOnly", "clientId", "quoteQuantity", 
                "autoBorrow", "autoLendRedeem", "autoBorrowRepay", "autoLend"]:
        if key in order_details:
            params[key] = str(order_details[key]).lower() if isinstance(order_details[key], bool) else str(order_details[key])
    
    return make_request("POST", endpoint, api_key, secret_key, instruction, params, order_details)

def get_open_orders(api_key, secret_key, symbol=None):
    """獲取未成交訂單"""
    endpoint = f"/api/{API_VERSION}/orders"
    instruction = "orderQueryAll"
    params = {}
    if symbol:
        params["symbol"] = symbol
    return make_request("GET", endpoint, api_key, secret_key, instruction, params)

def cancel_all_orders(api_key, secret_key, symbol):
    """取消所有訂單"""
    endpoint = f"/api/{API_VERSION}/orders"
    instruction = "orderCancelAll"
    params = {"symbol": symbol}
    data = {"symbol": symbol}
    return make_request("DELETE", endpoint, api_key, secret_key, instruction, params, data)

def cancel_order(api_key, secret_key, order_id, symbol):
    """取消指定訂單"""
    endpoint = f"/api/{API_VERSION}/order"
    instruction = "orderCancel"
    params = {"orderId": order_id, "symbol": symbol}
    data = {"orderId": order_id, "symbol": symbol}
    return make_request("DELETE", endpoint, api_key, secret_key, instruction, params, data)

def get_ticker(symbol: str) -> float:
    try:
        symbol = symbol.replace('-', '_').upper()  # ✅ 自動格式轉換
        endpoint = f"/api/v1/ticker?symbol={symbol}"  # ✅ 請求全部 ticker
        response = requests.get(f"{BASE_URL}{endpoint}")
        response.raise_for_status()
        ticker_data = response.json()
        logger.info(f"📊 取得報價: {ticker_data}")
        logger.info(f"🔧 lastPrice 型別: {type(price)}, 值: {price}")

        return float(ticker_data.get('lastPrice', 0))
    
        tickers = res.json()
        for ticker in tickers:
            if ticker.get("market", "").upper() == symbol:
                return float(ticker["price"])
        logger.error(f"❌ 未找到價格資訊: {symbol}")
        return 0.0
    except Exception as e:
        logger.error(f"獲取價格失敗: {e}")
        return 0.0

def get_markets():
    """獲取所有交易對信息"""
    endpoint = f"/api/{API_VERSION}/markets"
    return make_request("GET", endpoint)

def get_order_book(symbol, limit=20):
    """獲取市場深度"""
    endpoint = f"/api/{API_VERSION}/depth"
    params = {"symbol": symbol, "limit": str(limit)}
    return make_request("GET", endpoint, params=params)

def get_fill_history(api_key, secret_key, symbol=None, limit=100):
    """獲取歷史成交記錄"""
    endpoint = f"/wapi/{API_VERSION}/history/fills"
    instruction = "fillHistoryQueryAll"
    params = {"limit": str(limit)}
    if symbol:
        params["symbol"] = symbol
    return make_request("GET", endpoint, api_key, secret_key, instruction, params)

def get_klines(symbol, interval="1h", limit=100):
    """獲取K線數據"""
    data = public_client.get_klines(
        symbol=symbol,
        interval=interval,
        limit_count=limit_count  # 參數名根據SDK文檔修正
    )
    
    # 計算起始時間 (秒)
    current_time = int(time.time())
    
    # 各間隔對應的秒數
    interval_seconds = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800,
        "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800, "1month": 2592000
    }
    
    # 計算合適的起始時間
    duration = interval_seconds.get(interval, 3600)
    start_time = current_time - (duration * limit)
    
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": str(start_time)
    }
    
    return make_request("GET", endpoint, params=params)

def get_market_limits(symbol: str) -> dict:
    """取得單一交易對的市場限制資訊"""
    try:
        symbol = symbol.replace("_", "-").upper()
        logger.info("🟢 get_market_limits() 被呼叫")
        url = f"{BASE_URL}/api/v1/spot/markets"
        res = requests.get(url)
        res.raise_for_status()
        markets = res.json()
        for item in markets:
            if item["id"].upper() == symbol:
                limits = {
                    "base_precision": item["baseIncrement"],
                    "quote_precision": item["quoteIncrement"],
                    "min_order_size": item["minOrderSize"],
                    "tick_size": item["tickSize"],
                }
                logger.info(f"✅ 取得市場限制成功: {symbol} -> {limits}")
                return limits
        logger.error(f"❌ 未找到交易對 {symbol}")
        return {}
    except Exception as e:
        logger.error(f"❌ 市場限制查詢異常: {e}")
        return {}


# 在api/client.py中確保全局實例
client = BackpackAPIClient()  # 模塊級別單例
