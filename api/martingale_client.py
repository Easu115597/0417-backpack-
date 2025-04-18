"""
马丁策略专用API客户端模块
"""

import os
import time
import requests
import base64
import hmac
from typing import Dict, List
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from logger import setup_logger

logger = setup_logger("martingale_api")

class MartingaleAPIClient:
    def __init__(self):
        self.api_key = os.getenv('MARTINGALE_API_KEY')
        self.secret_key = os.getenv('MARTINGALE_SECRET_KEY')
        self.time_offset = 0
        self.base_url = "https://api.backpack.exchange"
        self._sync_server_time()

    def _sync_server_time(self):
        """同步交易所服务器时间"""
        try:
            response = requests.get(f"{self.base_url}/api/v1/time")
            server_time = response.json()['serverTime']
            local_time = int(time.time() * 1000)
            self.time_offset = server_time - local_time
            logger.debug(f"时间同步完成 | 偏移量: {self.time_offset}ms")
        except Exception as e:
            logger.error(f"时间同步失败: {str(e)}")
            self.time_offset = 0

    def _generate_signature(self, instruction: str, params: dict = None) -> dict:
        """生成API请求签名头"""
        timestamp = str(int(time.time() * 1000) + self.time_offset)
        window = "5000"
        
        # 构建签名消息
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
                "X-WINDOW": window
            }
        except Exception as e:
            logger.error(f"签名生成失败: {str(e)}")
            return {}

    def get_balance(self, asset: str) -> Dict[str, float]:
        """获取指定资产余额"""
        endpoint = "/api/v1/capital"
        headers = self._generate_signature("balanceQuery")
        
        try:
            response = requests.get(
                f"{self.base_url}{endpoint}",
                headers=headers
            )
            if response.status_code == 200:
                for balance in response.json().get('balances', []):
                    if balance.get('asset') == asset:
                        return {
                            'total': float(balance['total']),
                            'available': float(balance['available'])
                        }
            return {'total': 0.0, 'available': 0.0}
        except Exception as e:
            logger.error(f"余额查询异常: {str(e)}")
            return {'total': 0.0, 'available': 0.0}

    def get_historical_klines(self, symbol: str, interval: str = "1h", limit: int = 100) -> List[Dict]:
        """获取K线数据（马丁策略专用版本）"""
        endpoint = "/api/v1/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit  # 修正参数名为limit
        }
        
        try:
            headers = self._generate_signature("klinesQuery", params)
            response = requests.get(
                f"{self.base_url}{endpoint}",
                params=params,
                headers=headers
            )
            
            if response.status_code == 200:
                return [{
                    'timestamp': kline[0],
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                } for kline in response.json()]
            return []
        except Exception as e:
            logger.error(f"K线获取异常: {str(e)}")
            return []

    def execute_martingale_order(self, order_details: Dict) -> Dict:
        """执行马丁策略订单"""
        endpoint = "/api/v1/order"
        required_params = ["symbol", "side", "orderType", "quantity"]
        
        # 验证必要参数
        if not all(k in order_details for k in required_params):
            logger.error("缺失必要订单参数")
            return {"error": "Missing required parameters"}
        
        # 生成签名
        headers = self._generate_signature("orderExecute", order_details)
        
        try:
            response = requests.post(
                f"{self.base_url}{endpoint}",
                json=order_details,
                headers=headers
            )
            return response.json()
        except Exception as e:
            logger.error(f"订单执行异常: {str(e)}")
            return {"error": str(e)}

    def get_order_book(self, symbol: str, depth: int = 20) -> Dict:
        """获取市场深度数据"""
        endpoint = "/api/v1/depth"
        params = {"symbol": symbol, "limit": depth}
        
        try:
            response = requests.get(
                f"{self.base_url}{endpoint}",
                params=params
            )
            return response.json()
        except Exception as e:
            logger.error(f"订单簿获取异常: {str(e)}")
            return {"error": str(e)}

    def get_current_price(self, symbol: str) -> float:
        """获取当前最新价格"""
        endpoint = "/api/v1/ticker"
        params = {"symbol": symbol}
        
        try:
            response = requests.get(
                f"{self.base_url}{endpoint}",
                params=params
            )
            return float(response.json()['lastPrice'])
        except Exception as e:
            logger.error(f"价格获取异常: {str(e)}")
            return 0.0

# 全局客户端实例
martingale_client = MartingaleAPIClient()
