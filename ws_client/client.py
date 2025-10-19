# ws_client/client.py
import asyncio
import json
import logging
import time
import base64
import nacl.signing
import websockets
import threading
from typing import List, Dict, Any, Callable, Optional


logging.getLogger("backpack_ws").setLevel(logging.DEBUG)

class BackpackWebSocketClient:
    def __init__(self, api_key, secret_key, symbol, logger=None):
        self.api_key = api_key
        self.secret_key = secret_key
        self.symbol = symbol
        self.ws_url = "wss://ws.backpack.exchange" # Backpack WebSocket URL
        self.ws = None
        self.connected = False
        self.subscriptions = []
        self.logger = logger or logging.getLogger("backpack_ws")
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5  # 初始重連延遲（秒）
        self.callbacks = {}
        
    async def connect(self):
        """建立WebSocket連接"""
        try:
            self.ws = await websockets.connect(self.ws_url)
            self.connected = True
            self.reconnect_attempts = 0
            self.logger.info(f"WebSocket連接成功: {self.ws_url}")
            
            # 啟動心跳檢測 - 使用asyncio.create_task而不是threading
            self.heartbeat_task = asyncio.create_task(self._heartbeat())
            
            # 啟動訊息處理循環
            self.message_task = asyncio.create_task(self._message_handler())
            
            return True
        except Exception as e:
            self.logger.error(f"WebSocket連接失敗: {e}")
            return False
    
    async def _heartbeat(self):
        """心跳檢測（不發送任何消息）"""
        if not hasattr(self, 'running'):
            self.running = True
        while self.connected and self.running:
            # 不發送任何心跳消息，只是保持任務運行
            await asyncio.sleep(30)
        self.logger.debug("心跳任務結束")
        
    async def _reconnect(self):
        """重新連接WebSocket"""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.error(f"達到最大重連嘗試次數({self.max_reconnect_attempts})，停止重連")
            return False
        
        self.reconnect_attempts += 1
        delay = self.reconnect_delay * (2 ** (self.reconnect_attempts - 1))  # 指數退避
        self.logger.info(f"嘗試重連 ({self.reconnect_attempts}/{self.max_reconnect_attempts})，等待 {delay} 秒")
        
        await asyncio.sleep(delay)
        
        try:
            await self.disconnect()
            success = await self.connect()
            if success:
                # 重新訂閱之前的頻道
                for sub in self.subscriptions:
                    await self.subscribe(sub["channel"], sub["symbols"])
                return True
        except Exception as e:
            self.logger.error(f"重連失敗: {e}")
        
        return False
    
    async def disconnect(self):
        """關閉WebSocket連接"""
        if self.ws:
            # 取消所有任務
            if hasattr(self, 'heartbeat_task') and self.heartbeat_task:
                self.heartbeat_task.cancel()
            if hasattr(self, 'message_task') and self.message_task:
                self.message_task.cancel()
                
            # 設置running為False
            self.running = False
            
            # 關閉連接
            await self.ws.close()
            self.connected = False
            self.logger.info("WebSocket連接已關閉")
    
    async def subscribe(self, channel, symbols=None):
        """訂閱特定頻道的數據（最簡化版）"""
        if not self.connected:
            await self.connect()
        
        symbols = symbols or [self.symbol]
        
        try:
            # 最簡單的訂閱格式
            subscription_data = {
            "method": "SUBSCRIBE",
            "params": [f"{channel}.{symbol}" for symbol in symbols]
        }
            
            self.logger.debug(f"訂閱數據: {json.dumps(subscription_data)}")
            
            await self.ws.send(json.dumps(subscription_data))
            self.subscriptions.append({"channel": channel, "symbols": symbols})
            self.logger.info(f"已訂閱: {channel} - {symbols}")
            return True
        except Exception as e:
            self.logger.error(f"訂閱失敗: {e}", exc_info=True)
            return False
    
    async def _message_handler(self):
        """處理接收到的WebSocket訊息"""
        while self.connected:
            try:
                if self.ws:
                    message = await self.ws.recv()
                    self.logger.debug(f"收到原始消息: {message}")
                    
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError as e:
                        self.logger.error(f"解析JSON失敗: {e}, 原始消息: {message}")
                        continue
                    
                    # 處理ping消息
                    if isinstance(data, dict) and "ping" in data:
                        pong_message = {"pong": data["ping"]}
                        await self.ws.send(json.dumps(pong_message))
                        self.logger.debug(f"回應ping: {pong_message}")
                        continue
                    
                    # 處理訂閱確認
                    if "result" in data and data["result"] == "subscribed":
                        self.logger.info(f"訂閱確認: {data}")
                        continue
                    
                    # 處理錯誤消息
                    if "error" in data:
                        error_code = data.get("error", {}).get("code")
                        error_msg = data.get("error", {}).get("message")
                        self.logger.error(f"WebSocket錯誤: 代碼={error_code}, 消息={error_msg}, 完整消息: {data}")
                        continue
                    
                    # 處理訂單更新
                    if "stream" in data and "data" in data:
                        stream = data["stream"]
                        event_data = data["data"]
                        
                        # 訂單更新數據流
                        if stream.startswith("account.orderUpdate"):
                            self.logger.info(f"收到訂單更新: {event_data}")
                            
                            # 調用回調函數
                            if "account.orderUpdate" in self.callbacks:
                                await self.callbacks["account.orderUpdate"](event_data)
                    else:
                        self.logger.debug(f"收到未處理的訊息: {data}")
            except websockets.exceptions.ConnectionClosed:
                self.logger.warning("WebSocket連接已關閉，嘗試重連")
                await self._reconnect()
                break
            except Exception as e:
                self.logger.error(f"處理訊息時出錯: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def subscribe_account_updates(self):
        """訂閱賬戶更新（專門方法）"""
        try:
            # 檢查WebSocket連接狀態
            if not self.ws:
                self.logger.warning("WebSocket未連接，嘗試連接")
                connected = await self.connect()
                if not connected:
                    self.logger.error("WebSocket連接失敗，無法訂閱")
                    return False
            
            # 生成簽名
            timestamp = str(int(time.time() * 1000))
            window = "5000"  # 使用字符串
            
            # 構建簽名消息 - 使用與成功代碼相同的格式
            message_to_sign = f"instruction=subscribe&timestamp={timestamp}&window={window}"
            
            # 使用ED25519簽名
            private_key_bytes = base64.b64decode(self.secret_key)
            signing_key = nacl.signing.SigningKey(private_key_bytes)
            
            # 簽名
            signed = signing_key.sign(message_to_sign.encode('ascii'))
            signature = base64.b64encode(signed.signature).decode()
            
            # 使用正確的訂閱格式
            subscription_data = {
                "method": "SUBSCRIBE",
                "params": ["account.orderUpdate"],  # 不包含交易對
                "signature": [
                    self.api_key,
                    signature,
                    timestamp,
                    window
                ]
            }
            
            self.logger.debug(f"訂閱數據: {json.dumps({**subscription_data, 'signature': [self.api_key, 'SIGNATURE', timestamp, window]})}")
            
            if self.ws:
                await self.ws.send(json.dumps(subscription_data))
                self.subscriptions.append({"channel": "account.orderUpdate", "symbols": [self.symbol]})
                self.logger.info(f"已訂閱: account.orderUpdate")
                return True
            else:
                self.logger.error("WebSocket未連接，無法發送訂閱請求")
                return False
        except Exception as e:
            self.logger.error(f"訂閱賬戶更新失敗: {e}", exc_info=True)
            return False
    
    def on(self, channel, callback):
        """註冊頻道數據的回調函數"""
        self.callbacks[channel] = callback
        self.logger.info(f"已註冊 {channel} 頻道的回調函數")
        
    def is_connected(self):
        """檢查WebSocket是否已連接"""
        return self.connected and self.ws and self.ws.open
