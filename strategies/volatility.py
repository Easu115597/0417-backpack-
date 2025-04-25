# strategies/volatility.py
import requests
from utils.helpers import get_headers

def calculate_historical_volatility(symbol, period=24):
    """
    根據過去 24 小時的價格資料計算歷史波動率（標準差）
    """
    try:
        url = f"https://api.backpack.exchange/api/v1/klines?symbol={symbol}&interval=1h&limit={period}"
        headers = {"Content-Type": "application/json"}
        
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"無法取得K線資料: {response.status_code}")
            return 0.01  # fallback 波動率

        klines = response.json()
        closes = [float(kline[4]) for kline in klines]  # 收盤價在第5個欄位

        if len(closes) < 2:
            return 0.01

        avg_price = sum(closes) / len(closes)
        squared_diffs = [(p - avg_price) ** 2 for p in closes]
        variance = sum(squared_diffs) / (len(closes) - 1)
        volatility = variance ** 0.5
        return round(volatility / avg_price, 4)

    except Exception as e:
        print(f"計算波動率錯誤: {e}")
        return 0.01
