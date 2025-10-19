"""
API認證和簽名相關模塊
"""
import base64
import nacl.signing
import time  # 添加這行
import sys
from typing import Optional
from logger import setup_logger

logger = setup_logger("api.auth")

def create_signature(secret_key: str, params: dict, instruction: str = "orderExecute", window: int = 5000) -> Optional[str]:
    """生成API簽名"""
    try:
        timestamp = int(time.time() * 1000)
        
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
        private_key_bytes = base64.b64decode(secret_key)
        signing_key = nacl.signing.SigningKey(private_key_bytes)
        
        # 簽名
        signed = signing_key.sign(message.encode('ascii'))
        signature = base64.b64encode(signed.signature).decode()
        
        # 返回簽名、時間戳和窗口
        return {
            "signature": signature,
            "timestamp": str(timestamp),
            "window": str(window)
        }
    except Exception as e:
        logger.error(f"簽名生成失敗: {str(e)}")
        return None
