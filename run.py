#!/usr/bin/env python
"""
Backpack Exchange 做市交易程序統一入口
支持命令行模式和面板模式
"""
import argparse
import sys
import os
import logging
try:
    from logger import setup_logger
    from config import API_KEY, SECRET_KEY, WS_PROXY
except ImportError:
    API_KEY = os.getenv('API_KEY')
    SECRET_KEY = os.getenv('SECRET_KEY')
    WS_PROXY = os.getenv('PROXY_WEBSOCKET')
    
    def setup_logger(name):
        import logging
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        return logger
from dotenv import load_dotenv
load_dotenv()# ✅ 這行讓你可以從 .env 讀入 API_KEY / SECRET_KEY




# 創建記錄器
logger = setup_logger("main")
logging.basicConfig(level=logging.DEBUG)

def parse_arguments():
    """解析命令行參數"""
    parser = argparse.ArgumentParser(description='Backpack Exchange 馬丁交易Bot', conflict_handler='resolve')
    
    # 主模式選擇參數組
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--panel', action='store_true', help='啟動圖形界面面板')
    mode_group.add_argument('--cli', action='store_true', help='啟動命令行界面')
    mode_group.add_argument('--martingale', action='store_true', help='啟動馬丁現貨多單策略')
    
    # 基本參數
    parser.add_argument('--api-key', type=str, help='API Key (可選，默認使用環境變數或配置文件)')
    parser.add_argument('--secret-key', type=str, help='Secret Key (可選，默認使用環境變數或配置文件)')
    parser.add_argument('--ws-proxy', type=str, help='WebSocket Proxy (可選，默認使用環境變數或配置文件)')
    
    # 馬丁策略專用參數組

    martingale_group = parser.add_argument_group('馬丁策略參數')
    martingale_group.add_argument("--entry-type", type=str, default="offset", choices=["manual", "market", "offset"],
                        help="首單下單方式: manual (指定價格), market (市價), offset (低於現價)")
    martingale_group.add_argument("--entry-price", type=float, default=None,
                        help="entry-type 為 manual 時必填：首筆入場價格")  


    martingale_group.add_argument('--martingale', action='store_true', help='啟用馬丁策略')
    martingale_group.add_argument('--symbol',dest='symbol', type=str, default='SOL_USDC', help='馬丁策略交易對')
    martingale_group.add_argument('--total-capital', dest='total_capital', type=float, default=70, help='馬丁策略總投入 USDT')
    martingale_group.add_argument('--price_step_down',dest='price_step_down', type=float, default=0.005, help='加倉下跌百分比')
    martingale_group.add_argument('--take_profit_pct', type=float, default=0.013, help='止盈百分比')
    martingale_group.add_argument('--stop_loss_pct', type=float, default=-0.33, help='停損百分比')
    martingale_group.add_argument('--max-layers', dest='max_layers', type=int, default=3, help='最大加倉層數')
    martingale_group.add_argument('--multiplier', type=float, default=1.3, help='馬丁加碼倍率')
    martingale_group.add_argument('--use_market_order', action='store_true', help='是否使用市價初始下單')
    martingale_group.add_argument('--target_price', type=float,default=None, help='指定首次入場目標價格')
    martingale_group.add_argument('--runtime', type=int, default=-1, help='馬丁策略運行時間（秒），-1 表示無限運行直到止盈/止損')
    parser.add_argument("--poll-interval", type=int, default=60, help="每層下單的輪詢間隔（秒）")
    return parser.parse_args()

def main():
    """主函數"""
    args = parse_arguments()
    
    # 優先使用命令行參數中的API密鑰
    api_key = args.api_key or API_KEY
    secret_key = args.secret_key or SECRET_KEY

    # 读取wss代理
    ws_proxy = args.ws_proxy or WS_PROXY
    
    # 檢查API密鑰
    if not api_key or not secret_key:
        logger.error("缺少API密鑰，請通過命令行參數或環境變量提供")
        sys.exit(1)

        
    # 決定執行模式
    if args.panel:
        # 啟動圖形界面面板
        try:
            from panel.panel_main import run_panel
            run_panel(api_key=api_key, secret_key=secret_key, default_symbol=args.symbol)
        except ImportError as e:
            logger.error(f"啟動面板時出錯，缺少必要的庫: {str(e)}")
            logger.error("請執行 pip install rich 安裝所需庫")
            sys.exit(1)
    elif args.cli:
        # 啟動命令行界面
        try:
            from cli.commands import main_cli
            main_cli(api_key, secret_key)
        except ImportError as e:
            logger.error(f"啟動命令行界面時出錯: {str(e)}")
            sys.exit(1)

    elif args.martingale:
        
        base_asset, quote_asset = args.symbol.split("_") 
        
        
        try:
            from strategies.martingale_mode_long import MartingaleLongTrader

            



            # 初始化馬丁程式
            trader = MartingaleLongTrader(                
                api_key=api_key,
                secret_key=secret_key,
                symbol=args.symbol,
                base_asset=base_asset,
                quote_asset=quote_asset,
                total_capital_usdt=args.total_capital,
                price_step_down=args.price_step_down,
                take_profit_pct=args.take_profit_pct,
                stop_loss_pct=args.stop_loss_pct,
                max_layers=args.max_layers,
                martingale_multiplier=args.multiplier,
                use_market_order=args.use_market_order,
                target_price=args.target_price,
                entry_type=args.entry_type
                
            
            )
            trader.run()

        except Exception as e:
            logger.error(f"馬丁策略執行錯誤: {e}")
            import traceback
            traceback.print_exc()
    
    else:
        # 沒有指定執行模式時顯示幫助
        print("請指定執行模式：")
        print("  --panel   啟動圖形界面面板")
        print("  --cli     啟動命令行界面")
        print("  --martingale  啟動馬丁多單策略")
        print("  直接指定  --symbol 和 --spread 參數運行做市策略")
        print("\n使用 --help 查看完整幫助")

if __name__ == "__main__":
    main() 
