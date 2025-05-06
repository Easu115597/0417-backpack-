"""
日誌配置模塊
"""
import logging
import sys
import os

DEFAULT_LOG_FILE = "bot.log"

def setup_logger(name: str, log_file: str = None, level=logging.INFO) -> logging.Logger:
    """
    設置並返回一個 logger 實例，同時輸出到檔案和控制台。
    
    :param name: logger 名稱
    :param log_file: 日誌檔案路徑，預設為 DEFAULT_LOG_FILE
    :param level: 日誌等級，預設為 INFO
    :return: 配置完成的 logger 實例
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 避免重複加入 handler（可能多次 import）
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # ✅ File Handler
        if not log_file:
            log_file = DEFAULT_LOG_FILE
        os.makedirs(os.path.dirname(log_file), exist_ok=True) if os.path.dirname(log_file) else None
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # ✅ Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
