# Backpack Exchange 馬丁交易程序

這是一個針對 Backpack Exchange 的加密貨幣做市交易程序。該程序提供自動化做市功能，通過維持買賣價差賺取利潤。



## 功能特點

- 自動化做市策略
- 基礎價差設置
- 自動重平衡倉位
- 詳細的交易統計
- WebSocket 實時數據連接
- 命令行界面

## 項目結構

```
lemon_trader/
│
├── api/                  # API相關模塊
│   ├── __init__.py
│   ├── auth.py           # API認證和簽名相關
│   └── client.py         # API請求客戶端
│
├── websocket/            # WebSocket模塊
│   ├── __init__.py
│   └── client.py         # WebSocket客戶端
│
├── database/             # 數據庫模塊
│   ├── __init__.py
│   └── db.py             # 數據庫操作
│
├── strategies/           # 策略模塊
│   ├── __init__.py
│   └── market_maker.py   # 做市策略
│
├── utils/                # 工具模塊
│   ├── __init__.py
│   └── helpers.py        # 輔助函數
│
├── cli/                  # 命令行界面
│   ├── __init__.py
│   └── commands.py       # 命令行命令
│
├── panel/                # 交互式面板
│   ├── __init__.py
│   └── interactive_panel.py  # 交互式面板實現
│
├── config.py             # 配置文件
├── logger.py             # 日誌配置
├── main.py               # 主執行文件
├── run.py                # 統一入口文件
└── README.md             # 說明文檔


馬丁程式的主要模組及其關係的簡要流程圖
run.py
├── 初始化策略參數
├── 建立 WebSocket 連線
├── 進行時間同步
├── 初始化資料庫
├── 啟動策略執行
│   └── MartingaleStrategy.run()
│       ├── cancel_existing_orders()
│       ├── get_market_price()
│       ├── place_martingale_orders()
│       │   ├──資金分配 + 計算 size
│       │   ├── submit_order()             ← 呼叫 API 下單
│       │   └── execute_order()
│       ├── 每隔 interval:
│       │   ├──check_order_fills()
│       │   ├──check_take_profit() / check_stop_loss()
│       │   └── 如果觸發 close_all_positions()

在 run.py 中，程式初始化各項設定後，呼叫 MartingaleStrategy 類別的 run() 方法。
​在 run() 方法中，依序執行取消現有訂單、取得市場價格、下馬丁格爾訂單、檢查訂單成交情況，最後在需要時關閉所有持倉。

模組資料流圖：下單流程、API、WebSocket、動態止損邏輯
以下是根據 README.md 整理的模組資料流圖，展示了整個下單流程、API 通訊、WebSocket 資料流，以及動態止損邏輯的互動關係：
+----------------+
|    run.py      |
+----------------+
        |
        v
+----------------+
|  參數解析與初始化  |
+----------------+
        |
        v
+----------------+
|  martingale_mode_long.py  |
+----------------+
        |
        v
+----------------+
|  資金分配與下單邏輯  |
+----------------+
        |
        v
+----------------+
|    client.py   |
+----------------+
        |
        v
+----------------+
|    auth.py     |
+----------------+
        |
        v
+----------------+
|  API 請求與回應處理  |
+----------------+

同時，WebSocket 資料流如下：

+----------------+
|  ws_client.py  |
+----------------+
        |
        v
+----------------+
|  即時市場資料處理  |
+----------------+
        |
        v
+----------------+
|  動態止損判斷邏輯  |
+----------------+
        |
        v
+----------------+
|  client.py (市價平倉) |
+----------------+

此資料流圖展示了從程式啟動、參數解析、策略執行、API 認證與請求、即時資料處理，到動態止損的整個流程。

```


martingale_mode_long.py 主要邏輯檢視
在 martingale_mode_long.py 中，MartingaleStrategy 類別的主要方法如下：​
GitHub

place_martingale_orders()：​計算每一層的訂單價格與數量，然後呼叫 execute_order() 來下單。​

execute_order()：​執行實際的下單操作，與交易所 API 互動。​

cancel_existing_orders()：​取消所有未成交的訂單，避免重複下單或訂單堆積。​

check_order_fills()：​檢查已下訂單的成交情況，更新策略狀態。​

close_all_positions()：​在策略終止時，平倉所有持倉，確保資金安全。


馬丁策略的主循環流程，會呼叫：

_ensure_data_streams() ✅ 初始化 WebSocket 和訂閱

cancel_existing_orders() ✅ 取消所有現有掛單

get_current_price() ✅ 透過 WebSocket 或 REST 拿價格

_check_risk() ✅ 判斷是否觸發止盈/止損

place_martingale_orders() ✅ 執行實際下單

裡面會用到：generate_martingale_orders() ➜ calculate_prices()、allocate_funds()





專案中，API 認證主要透過 auth.py 處理，並在 client.py 中應用。​

auth.py
功能：​生成 API 請求所需的簽名。​

主要流程：

組合請求的各個部分（如方法、路徑、查詢參數、請求體等）。​

使用 HMAC-SHA256 演算法，搭配您的 API Secret，對組合後的字串進行簽名。​

將生成的簽名與其他必要的認證資訊（如 API Key、時間戳）一起，添加到請求的標頭中。​

client.py
功能：​處理與 API 的實際通訊，並應用 auth.py 提供的簽名功能。​

主要流程：

在發送請求前，調用 auth.py 中的簽名函數，生成簽名。​

將簽名與其他認證資訊添加到請求的標頭中。​

發送請求至 API，並處理回應。

## 環境要求

- Python 3.8 或更高版本
- 所需第三方庫：
  - nacl (用於API簽名)
  - requests
  - websocket-client
  - numpy
  - python-dotenv

## 安裝

1. 克隆或下載此代碼庫:

```bash
git clone https://github.com/yanowo/Backpack-MM-Simple.git
cd Backpack-MM-Simple
```

2. 安裝依賴:

```bash
pip install -r requirements.txt
```

3. 設置環境變數:

創建 `.env` 文件並添加:

```
API_KEY=your_api_key
SECRET_KEY=your_secret_key
```

## 使用方法

### 統一入口 (推薦)

```bash
# 啟動交互式面板
python run.py --panel

# 啟動命令行界面
python run.py --cli  

# 直接運行做市策略
python run.py --symbol SOL_USDC --spread 0.1
```

### 命令行界面

啟動命令行界面:

```bash
python main.py --cli
```

### 直接執行做市策略

```bash
python main.py --symbol SOL_USDC --spread 0.5 --max-orders 3 --duration 3600 --interval 60
```

### 命令行參數

- `--api-key`: API 密鑰 (可選，默認使用環境變數)
- `--secret-key`: API 密鑰 (可選，默認使用環境變數)
- `--cli`: 啟動命令行界面
- `--panel`: 啟動交互式面板

### 做市參數

- `--symbol`: 交易對 (例如: SOL_USDC)
- `--spread`: 價差百分比 (例如: 0.5)
- `--quantity`: 訂單數量 (可選)
- `--max-orders`: 每側最大訂單數量 (默認: 3)
- `--duration`: 運行時間（秒）(默認: 3600)
- `--interval`: 更新間隔（秒）(默認: 60)

## 設定保存

通過面板模式修改的設定會自動保存到 `settings/panel_settings.json` 文件中，下次啟動時會自動加載。

## 運行示例

### 基本做市示例

```bash
python run.py --symbol SOL_USDC --spread 0.2 --max-orders 5
```

### 長時間運行示例

```bash
python run.py --symbol SOL_USDC --spread 0.1 --duration 86400 --interval 120
```

### 完整參數示例

```bash
python run.py --symbol SOL_USDC --spread 0.3 --quantity 0.5 --max-orders 3 --duration 7200 --interval 60
``` 

## 注意事項

- 交易涉及風險，請謹慎使用
- 建議先在小資金上測試策略效果
- 定期檢查交易統計以評估策略表現
