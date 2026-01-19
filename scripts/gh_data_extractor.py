import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
import argparse
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, AsyncGenerator, Callable, Set
from functools import wraps

# -----------------------------------------------------------------------------
# 依赖检查与导入
# -----------------------------------------------------------------------------
try:
    import aiohttp
    import yaml
    from tqdm.asyncio import tqdm
except ImportError as e:
    print(f"CRITICAL ERROR: 缺少必要依赖库: {e.name}")
    print("请运行: pip install aiohttp tqdm PyYAML")
    sys.exit(1)

# Windows 平台下的 asyncio 兼容性设置
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# -----------------------------------------------------------------------------
# 配置与常量 (Configuration)
# -----------------------------------------------------------------------------
@dataclass
class AppConfig:
    """应用程序配置容器"""
    github_token: str
    db_path: str = "data/github_insight.db"
    report_path: str = "reports/insight_report.html"
    lookback_days: int = 30
    concurrency: int = 5
    log_level: str = "INFO"
    
    # 常量定义
    GITHUB_API_BASE: str = "https://api.github.com"
    NOMINATIM_API: str = "https://nominatim.openstreetmap.org/search"
    USER_AGENT: str = "GitHub-Insight-Bot/2.1 (research-purpose)"

# -----------------------------------------------------------------------------
# 日志系统 (Logging)
# -----------------------------------------------------------------------------
def setup_logging(level_name: str) -> logging.Logger:
    logger = logging.getLogger("GHInsight")
    level = getattr(logging, level_name.upper(), logging.INFO)
    logger.setLevel(level)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - [%(levelname)s] - %(message)s',
            datefmt='%H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

logger = setup_logging("INFO")

# -----------------------------------------------------------------------------
# 工具函数 (Utilities)
# -----------------------------------------------------------------------------
def async_retry(retries: int = 3, delay: int = 1, backoff: int = 2):
    """
    [关键逻辑] 异步重试装饰器
    用于网络请求不稳定的情况，实现指数退避策略（失败等待时间 1s -> 2s -> 4s）。
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_delay = delay
            for i in range(retries + 1):
                try:
                    return await func(*args, **kwargs)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if i == retries:
                        logger.error(f"函数 {func.__name__} 重试耗尽: {e}")
                        raise
                    # 仅在非最后一次尝试时等待
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

# -----------------------------------------------------------------------------
# 数据模型 (Data Models)
# -----------------------------------------------------------------------------
@dataclass
class CommitRecord:
    sha: str
    repo_name: str
    author_login: str
    timestamp: int
    raw_location: Optional[str] = None
    country_code: str = "UNKNOWN"
    city: str = ""
    lat: float = 0.0
    lon: float = 0.0
