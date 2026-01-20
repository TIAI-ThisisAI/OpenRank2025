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

# =============================================================================
# 模块 0: 基础设施与环境 (Infrastructure & Environment)
# 职责: 处理依赖导入、系统兼容性设置、全局常量与配置定义
# =============================================================================

# --- 依赖检查 ---
try:
    import aiohttp
    import yaml
    from tqdm.asyncio import tqdm
except ImportError as e:
    print(f"CRITICAL ERROR: 缺少必要依赖库: {e.name}")
    print("请运行: pip install aiohttp tqdm PyYAML")
    sys.exit(1)

# --- Windows 兼容性 ---
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# --- 配置定义 ---
@dataclass
class AppConfig:
    """
    [配置模块] 应用程序配置容器
    集中管理所有可变参数，便于从 CLI 或配置文件注入。
    """
    github_token: str
    db_path: str = "data/github_insight.db"
    report_path: str = "reports/insight_report.html"
    lookback_days: int = 30
    concurrency: int = 5
    log_level: str = "INFO"
    
    # API 常量 (通常不通过外部配置修改)
    GITHUB_API_BASE: str = "https://api.github.com"
    NOMINATIM_API: str = "https://nominatim.openstreetmap.org/search"
    USER_AGENT: str = "GitHub-Insight-Bot/2.1 (research-purpose)"

# =============================================================================
# 模块 1: 日志与工具 (Logging & Utilities)
# 职责: 提供通用的日志记录能力和异步重试机制装饰器
# =============================================================================

def setup_logging(level_name: str) -> logging.Logger:
    """初始化全局日志记录器，配置标准输出格式。"""
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

# 初始化默认日志实例
logger = setup_logging("INFO")

def async_retry(retries: int = 3, delay: int = 1, backoff: int = 2):
    """
    [工具模块] 异步指数退避重试装饰器
    
    用途:
        用于修饰不稳定的网络请求函数。
    逻辑:
        当捕获到网络异常时，不立即报错，而是等待 (delay * backoff^n) 秒后重试。
        直到重试次数用尽才抛出异常。
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
                        logger.error(f"函数 {func.__name__} 重试耗尽，最终错误: {e}")
                        raise
                    logger.debug(f"函数 {func.__name__} 失败，{current_delay}s 后重试 ({i+1}/{retries})")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

# =============================================================================
# 模块 2: 数据模型 (Data Models)
# 职责: 定义核心业务对象的结构，保证数据流转的一致性
# =============================================================================

@dataclass
class CommitRecord:
    """
    [数据模块] 单条 Commit 记录的标准结构
    包含从 GitHub 获取的元数据以及后期解析出的地理信息。
    """
    sha: str
    repo_name: str
    author_login: str
    timestamp: int
    raw_location: Optional[str] = None
    country_code: str = "UNKNOWN"
    city: str = ""
    lat: float = 0.0
    lon: float = 0.0
