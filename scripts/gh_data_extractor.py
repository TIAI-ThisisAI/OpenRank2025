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
