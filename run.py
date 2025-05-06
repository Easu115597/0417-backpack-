#!/usr/bin/env python
"""
Backpack Exchange 做市交易程序統一入口
支持命令行模式和面板模式
"""
import argparse
import sys
import os
import logging
import asyncio

from strategies.martingale_mode_long import MartingaleLongTrader 

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
load_dotenv()

logger = setup_logger("main")
logging.basicConfig(level=logging.DEBUG)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Backpack Exchange 馬丁交易Bot', conflict_handler='resolve')

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--panel', action='store_true', help='啟動圖形界面面板')
    mode_group.add_argument('--cli', action='store_true', help='啟動命令行界面')
    mode_group.add_argument('--martingale', action='store_true', help='啟動馬丁現貨多單策略')

    parser.add_argument('--api-key', type=str, help='API Key')
    parser.add_argument('--secret-key', type=str, help='Secret Key')
    parser.add_argument('--ws-proxy', type=str, help='WebSocket Proxy')

    martingale_group = parser.add_argument_group('馬丁策略參數')
    martingale_group.add_argument('--symbol', type=str, default='SOL_USDC', help='馬丁策略交易對')
    martingale_group.add_argument('--total-capital', type=float, default=70, help='總投入 USDT')
    martingale_group.add_argument('--price_step_down', type=float, default=0.008, help='每層下跌百分比')
    martingale_group.add_argument('--take_profit_pct', type=float, default=0.013, help='止盈百分比')
    martingale_group.add_argument('--stop_loss_pct', type=float, default=-0.33, help='停損百分比')
    martingale_group.add_argument('--max-layers', type=int, default=3, help='最大層數')
    martingale_group.add_argument('--multiplier', type=float, default=1.3, help='加碼倍率')
    martingale_group.add_argument('--use_market_order', action='store_true', help='是否使用市價首單')
    martingale_group.add_argument('--target_price', type=float, default=None, help='指定首次入場目標價格')
    martingale_group.add_argument('--entry-type', type=str, default='offset', choices=['manual', 'market', 'offset'], help='首單下單方式')
    martingale_group.add_argument('--entry-price', type=float, default=None, help='entry-type 為 manual 時需提供價格')
    martingale_group.add_argument('--runtime', type=int, default=-1, help='策略最大運行時間（秒）')
    parser.add_argument('--poll-interval', type=int, default=60, help='每層下單輪詢間隔（秒）')

    return parser.parse_args()


async def main():
    args = parse_arguments()

    api_key = args.api_key or API_KEY
    secret_key = args.secret_key or SECRET_KEY
    ws_proxy = args.ws_proxy or WS_PROXY

    if not api_key or not secret_key:
        logger.error("缺少API密鑰，請提供")
        sys.exit(1)

    if args.panel:
        try:
            from panel.panel_main import run_panel
            run_panel(api_key=api_key, secret_key=secret_key, default_symbol=args.symbol)
        except ImportError as e:
            logger.error(f"面板啟動錯誤: {str(e)}")
            logger.error("請 pip install rich")
            sys.exit(1)
    elif args.cli:
        try:
            from cli.commands import main_cli
            main_cli(api_key, secret_key)
        except ImportError as e:
            logger.error(f"CLI 啟動錯誤: {str(e)}")
            sys.exit(1)
    elif args.martingale:
        base_asset, quote_asset = args.symbol.split("_")
        try:
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
                entry_type=args.entry_type,
                entry_price=args.entry_price
            )
            await trader.run()
        except Exception as e:
            logger.error(f"馬丁策略執行錯誤: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("請指定模式：--panel 或 --cli 或 --martingale")


if __name__ == "__main__":
    asyncio.run(main())
