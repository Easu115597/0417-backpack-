"""
API請求客戶端模塊
"""
import json
import time
import requests
import os
import base64
import logging
import hmac
import hashlib
import time
import base64
import nacl.signing
from typing import Dict, Any, Optional, List, Union
from .auth import create_signature
from config import API_URL, API_VERSION, DEFAULT_WINDOW
from logger import setup_logger
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from .auth import create_hmac_signature



MARKET_ENDPOINT = "https://api.backpack.exchange/api/v1/markets"

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("SECRET_KEY")

logger = setup_logger("api.client")
BASE_URL = "https://api.backpack.exchange"
logger = logging.getLogger(__name__)



class BackpackAPIClient:
    def __init__(self, api_key=None, secret_key=None, time_offset=0):
        from dotenv import load_dotenv
        load_dotenv()
        
        self.api_key = api_key or os.getenv('API_KEY')
        self.secret_key = secret_key or os.getenv('secret_key')
        self.base_url = "https://api.backpack.exchange"
        self.time_offset = time_offset
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
    
    def generate_signature(secret, timestamp, method, request_path, body=''):
        message = f'{timestamp}{method.upper()}{request_path}{body}'
        signature = hmac.new(self.secret_key.encode(), message.encode(), hashlib.sha256).digest()
        return base64.b64encode(signature).decode()

    
    def get_headers(self, payload: Optional[dict] = None) -> dict:
        timestamp = str(int(time.time() * 1000))
        body = json.dumps(payload) if payload else ''
        message = f"{timestamp}{body}"
        signature = self.create_signature(message)

        return {
            "BP-API-KEY": self.api_key,
            "BP-API-TIMESTAMP": timestamp,
            "BP-API-SIGNATURE": signature,
            "Content-Type": "application/json",
        }
    
    def _generate_ed25519_headers(self, instruction: str, params: dict):
        self._sync_server_time() 
        """用 Ed25519 生成 instruction API headers"""
        timestamp = str(int(time.time() * 1000) + self.time_offset)
        window = "5000"
        message = f"instruction={instruction}"
        
        if params:
            sorted_params = sorted(params.items())
            param_str = "&".join([f"{k}={v}" for k, v in sorted_params])
            message += f"&{param_str}"
        message += f"&timestamp={timestamp}&window={window}"

        try:
            private_key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(self.secret_key))
            signature = base64.b64encode(private_key.sign(message.encode())).decode()
            return {
                "X-API-KEY": self.api_key,
                "X-SIGNATURE": signature,
                "X-TIMESTAMP": timestamp,
                "X-WINDOW": "5000"
            }
        except Exception as e:
            logger.error(f"Ed25519 簽名生成失敗: {str(e)}")
            return {}

    def _generate_hmac_headers(self, method: str, path: str, body: str = "") -> dict:
        """用 HMAC-SHA256 生成 REST API headers"""
        timestamp = str(int(time.time() * 1000) + self.time_offset)
        message = f"{timestamp}{method.upper()}{path}{body}"
        try:
            signature = hmac.new(
                self.secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256
            ).hexdigest()

            return {
                "BP-API-KEY": self.api_key,
                "BP-API-TIMESTAMP": timestamp,
                "BP-API-SIGNATURE": signature,
                "Content-Type": "application/json"
            }
        except Exception as e:
            logger.error(f"HMAC 簽名生成失敗: {e}")
            return {}
        
    # 在api/client.py中添加全局格式转换方法
    def normalize_symbol(symbol: str) -> str:
        """统一交易对格式为 API 标准格式（大写短横线）"""
        return symbol.replace('_', '-').upper( )
        
    def get_klines(self, symbol: str, interval: str = "1h", limit: int = 100) -> list:    
        """獲取K線數據（支持多時間週期）"""
        try:
            interval_map = {
                "1m": "1m", "5m": "5m", "15m": "15m",
                "30m": "30m", "1h": "1H", "4h": "4H",
                "1d": "1D", "1w": "1W", "1month": "1M"
            }

            params = {
                "symbol": symbol.replace('-', '_'),
                "interval": interval_map.get(interval, '1H'),
                "limit": limit
            }

            headers = self.get_headers()  # ✅ 不需再傳 api_type 等參數

            response = requests.get(
                f"{self.base_url}/api/{API_VERSION}/klines",
                params=params,
                headers=headers
            )

            if response.status_code == 200:
                return [{
                    'timestamp': int(kline[0]),
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                } for kline in response.json()]
            else:
                logger.warning(f"K線獲取失敗: {response.status_code} - {response.text}")
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
                        'base_precision': int(market.get("quantityPrecision", 6)),
                        'quote_precision': int(market.get("pricePrecision", 6)),
                        'min_order_size': float(market.get("minNotional", 0)),
                        'tick_size': float(market.get("tickSize", 0.0001))
                    }
                    logger.info(f"✅ 取得市場限制成功: {symbol} -> {result}")
                    print(f"✅ 取得市場限制: {result}")
                    return result

            logger.error(f"未找到交易對 {symbol}")
            return None  # ⚠️ 別 return 字串！
        except Exception as e:
            logger.error(f"市場限制查詢異常: {e}")
            return None
        
    def place_martingale_orders(self):
        # ...計算target_price和allocated_funds...
        quantity = allocated_funds[layer] / target_price
        quantity = round_to_precision(quantity, self.base_precision)
    
        # 強制符合交易所精度要求
        quantity_str = f"{quantity:.{self.base_precision}f}"
        quantity = float(quantity_str)
    
        if quantity < self.min_order_size:
            logger.warning(f"層級{layer}訂單量{quantity}低於最小值{self.min_order_size}，跳過")
        
    def make_request(self, method, endpoint, instruction=None, params=None, data=None, retry_count=3):
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
        url = f"{self.base_url}{endpoint}"
        headers = {"Content-Type": "application/json"}
    
        # 構建簽名信息（如需要）
        if instruction:
            timestamp = str(int(time.time() * 1000))
            window = "5000"
            query_string = ""

            if params:
                sorted_params = sorted(params.items())
                query_string = "&".join([f"{k}={v}" for k, v in sorted_params])

            sign_message = f"instruction={instruction}"
            if query_string:
                sign_message += f"&{query_string}"
            sign_message += f"&timestamp={timestamp}&window={window}"

            signature = create_signature(self.secret_key, sign_message)
            if not signature:
                return {"error": "簽名失敗"}
        
            headers.update({
                "X-API-KEY": self.api_key,
                "X-SIGNATURE": signature,
                "X-TIMESTAMP": timestamp,
                "X-WINDOW": "5000" 
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
    

    def get_balance(self, asset: str) -> dict:
        """獲取資產餘額"""
        try:
            headers = self.get_headers()  # ✅ 同樣改簡潔版
            response = requests.get(f"{self.base_url}/api/{API_VERSION}/capital", headers=headers)

            if response.status_code == 200:
                balances = response.json().get("data", [])
                return next((b for b in balances if b["asset"] == asset), {})
            else:
                logger.warning(f"餘額獲取失敗: {response.status_code} - {response.text}")
                return {}

        except Exception as e:
            logger.error(f"餘額查詢異常: {str(e)}")
            return {}

          

    def get_open_orders(self, symbol=None):
        """獲取未成交訂單"""
        endpoint = f"/api/{API_VERSION}/orders"
        instruction = "orderQueryAll"
        params = {}
        if symbol:
            params["symbol"] = symbol
        return self.make_request("GET", endpoint,  instruction, params)

    def cancel_all_orders(self, symbol):
        """取消所有訂單"""
        endpoint = f"/api/{API_VERSION}/orders"
        instruction = "orderCancelAll"
        params = {"symbol": symbol}
        data = {"symbol": symbol}
        return self.make_request("DELETE", endpoint, instruction, params, data)

    def cancel_order(self, order_id, symbol):
        """取消指定訂單"""
        endpoint = f"/api/{API_VERSION}/order"
        instruction = "orderCancel"
        params = {"orderId": order_id, "symbol": symbol}
        data = {"orderId": order_id, "symbol": symbol}
        return self.make_request("DELETE", endpoint, instruction, params, data)
    
    def get_fill_history(self, symbol: str = None, limit: int = 100) -> dict:
        endpoint = "/wapi/v1/history/fills"
        instruction = "fillHistoryQueryAll"  # 明確指定instruction參數
        params = {"limit": str(limit)}
        if symbol:
            params["symbol"] = symbol.replace('-', '_').upper()  # 強制轉換交易對格式
        return self.make_request(
            method="GET",
            endpoint=endpoint,
            instruction=instruction,  # 補齊缺失參數
            params=params
        )
    
    def execute_order(self, order_details: dict) -> dict:
        """執行下單請求"""
        from .logger import logger  # 確保有 log

        order_details['symbol'] = order_details['symbol'].replace('-', '_').upper()

        # 市價單只能選一種數量類型
        if order_details.get('orderType') == 'Market':
            if 'quantity' in order_details and 'quoteQuantity' in order_details:
                order_details.pop('quantity')  # 優先使用 quoteQuantity

        # ✅ 調試日誌
        logger.debug(f"📤 提交訂單 API Payload: {json.dumps(order_details, indent=2)}")

        endpoint = f"/api/{API_VERSION}/order"
        try:
            # ✅ 使用正確 payload 傳入 headers
            headers = self.get_headers(payload=order_details)

            response = requests.post(
                f"{self.base_url}{endpoint}",
                json=order_details,
                headers=headers
            )

            response_data = response.json()

            # ✅ API 錯誤回報
            if response.status_code != 200:
                logger.error(f"API 回應失敗: {response.status_code} - {response.text}")
            return response_data

        except Exception as e:
            logger.error(f"订单执行失败: {str(e)}")
            return {"error": str(e)}

def get_ticker(symbol: str) -> float:
    try:
        symbol = symbol.replace('-', '_').upper()  # ✅ 自動格式轉換
        endpoint = f"/api/v1/ticker?symbol={symbol}"  # ✅ 請求全部 ticker
        response = requests.get(f"{BASE_URL}{endpoint}")
        response.raise_for_status()
        ticker_data = response.json()
        price = float(ticker_data.get('lastPrice', 0))
        logger.info(f"📊 取得報價: {ticker_data}")
        logger.info(f"🔧 lastPrice 型別: {type(price)}, 值: {price}")

        return price
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




    
def format_symbol(symbol: str, for_order: bool = False) -> str:
    return symbol.replace("_", "-") if for_order else symbol

# 在api/client.py中確保全局實例
client = BackpackAPIClient()  # 模塊級別單例
