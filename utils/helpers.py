"""
輔助函數模塊
"""
import math
import numpy as np
from typing import List, Union, Optional

def round_to_precision(value: float, precision: int) -> float:
    """
    根據精度四捨五入數字
    
    Args:
        value: 要四捨五入的數值
        precision: 小數點精度
        
    Returns:
        四捨五入後的數值
    """
    factor = 10 ** precision
    return math.floor(value * factor) / factor

def round_to_tick_size(price: float, tick_size: float) -> float:
    """
    根據tick_size四捨五入價格
    
    Args:
        price: 原始價格
        tick_size: 價格步長
        
    Returns:
        調整後的價格
    """
    tick_size_float = float(tick_size)
    rounded_price = round(price / tick_size_float) * tick_size_float
    precision = len(str(tick_size_float).split('.')[-1]) if '.' in str(tick_size_float) else 0
    return round(rounded_price, precision)

def calculate_volatility(prices: List[float], window: int = 20) -> float:
    """
    計算波動率
    
    Args:
        prices: 價格列表
        window: 計算窗口大小
        
    Returns:
        波動率百分比
    """
    if len(prices) < window:
        return 0
    
    # 使用最近N個價格計算標準差
    recent_prices = prices[-window:]
    returns = np.diff(recent_prices) / recent_prices[:-1]
    return np.std(returns) * 100  # 轉換為百分比

def calculate_volatility(
    prices: List[float], 
    period: int = 20,
    timeframe: str = "1d"
) -> float:
    """強化版波動率計算"""
    if len(prices) < period:
        return 0.0
    
    # 根據時間週期調整年化因子
    annualization_factors = {
        "1m": 252*24*60, "5m": 252*24*12, "15m": 252*24*4,
        "1h": 252*24, "4h": 252*6, "1d": 252, "1w": 52
    }
    factor = annualization_factors.get(timeframe, 252)
    
    log_returns = np.log(prices[1:]/prices[:-1])
    volatility = np.std(log_returns) * np.sqrt(factor)
    return float(volatility * 100)  # 返回百分比形式