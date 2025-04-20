"""
API認證和簽名相關模塊
"""

import base64
import hmac
import hashlib
import nacl.signing
from typing import Optional
from logger import setup_logger
import os

logger = setup_logger("api.auth")

# ========================
# 🔒 Backpack - Ed25519 簽章 (Base64)
# ========================
def create_ed25519_signature(secret_key: str, message: str) -> Optional[str]:
    """
    創建 Ed25519 類型的簽名（用於 WebSocket 認證或特定 API）
    Args:
        secret_key: Base64 編碼的 Ed25519 金鑰
        message: 要簽名的原始訊息
    Returns:
        Base64 簽名字串，若失敗則回傳 None
    """
    try:
        decoded_key = base64.b64decode(secret_key)
        signing_key = nacl.signing.SigningKey(decoded_key)
        signature = signing_key.sign(message.encode('utf-8')).signature
        return base64.b64encode(signature).decode('utf-8')
    except Exception as e:
        logger.error(f"Ed25519 簽名創建失敗: {e}")
        return None

# ========================
# 🔐 Backpack - HMAC SHA256 簽章（REST API使用）
# ========================
def create_hmac_signature(secret_key: str, timestamp: str, method: str, request_path: str, body: str = "") -> str:
    """
    創建 Backpack REST API 所需的 HMAC-SHA256 簽名
    Args:
        timestamp: 當前毫秒時間戳
        method: HTTP 請求方法（GET/POST 等）
        request_path: API 路徑（如 /api/v1/orders）
        body: 請求內容，預設為空字串
    Returns:
        簽名字串（16進制）
    """
    message = f"{timestamp}{method.upper()}{request_path}{body}"
    signature = hmac.new(secret_key.encode(), message.encode(), hashlib.sha256).hexdigest()
    return signature
