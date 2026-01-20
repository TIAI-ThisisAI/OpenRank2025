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

# =============================================================================
# 模块 3: 持久化层 (Persistence Layer)
# 职责: 封装所有 SQLite 数据库操作，隐藏 SQL 细节
# =============================================================================

class StorageManager:
    """
    [持久化模块] 数据库管理器
    
    特性:
        1. Context Manager: 支持 `with StorageManager(...)` 语法，自动管理连接生命周期。
        2. 缓存管理: 维护 geo_cache 表，避免重复查询地理编码 API。
        3. 批量优化: 提供批量去重 (filter_existing_shas) 和批量写入 (save_commits) 接口。
    """
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.conn: Optional[sqlite3.Connection] = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        if self.conn is None:
            try:
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
                self.conn = sqlite3.connect(self.db_path)
                self.conn.row_factory = sqlite3.Row
                self._init_schema()
            except OSError as e:
                logger.error(f"无法初始化数据库: {e}")
                sys.exit(1)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def _init_schema(self):
        """初始化数据库表结构：包含地理信息缓存表和提交记录表"""
        if not self.conn: return
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS geo_cache (
                    raw_text TEXT PRIMARY KEY,
                    country_code TEXT, city TEXT, lat REAL, lon REAL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS commits (
                    sha TEXT PRIMARY KEY,
                    repo_name TEXT, author_login TEXT, timestamp INTEGER,
                    country_code TEXT, lat REAL, lon REAL
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_commits_country ON commits(country_code)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_commits_author ON commits(author_login)")

    def get_geo_cache(self, raw_text: str) -> Optional[Dict[str, Any]]:
        """读取地理位置缓存"""
        cursor = self.conn.execute(
            "SELECT country_code, city, lat, lon FROM geo_cache WHERE raw_text = ?", 
            (raw_text,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def save_geo_cache(self, raw_text: str, data: Dict[str, Any]):
        """写入地理位置缓存"""
        with self.conn:
            self.conn.execute(
                """INSERT OR REPLACE INTO geo_cache (raw_text, country_code, city, lat, lon) 
                   VALUES (?, ?, ?, ?, ?)""",
                (raw_text, data.get('country_code'), data.get('city'), data.get('lat'), data.get('lon'))
            )

    def filter_existing_shas(self, shas: List[str]) -> Set[str]:
        """
        [性能关键] 批量检查 SHA 是否已存在
        防止对已处理过的 Commit 重复进行 API 请求和处理。
        """
        if not shas: return set()
        # 注意: SQLite 默认限制参数数量，若 batch 过大需分片，此处假设 batch < 999
        placeholders = ','.join(['?'] * len(shas))
        cursor = self.conn.execute(f"SELECT sha FROM commits WHERE sha IN ({placeholders})", shas)
        return {row['sha'] for row in cursor.fetchall()}

    def save_commits(self, commits: List[CommitRecord]):
        """批量保存 Commit 记录"""
        if not commits: return
        data = [
            (c.sha, c.repo_name, c.author_login, c.timestamp, c.country_code, c.lat, c.lon)
            for c in commits
        ]
        with self.conn:
            self.conn.executemany(
                "INSERT OR IGNORE INTO commits VALUES (?, ?, ?, ?, ?, ?, ?)", 
                data
            )

    def get_statistics(self) -> Dict[str, Any]:
        """生成用于报告的统计聚合数据"""
        stats = {}
        # 1. 国家分布
        cur = self.conn.execute("""
            SELECT country_code, COUNT(*) as cnt 
            FROM commits 
            WHERE country_code NOT IN ('UNKNOWN', '') 
            GROUP BY country_code ORDER BY cnt DESC LIMIT 20
        """)
        stats['countries'] = {row['country_code']: row['cnt'] for row in cur.fetchall()}

        # 2. 活跃时间 (UTC)
        cur = self.conn.execute("""
            SELECT strftime('%H', datetime(timestamp, 'unixepoch')) as hour, COUNT(*) as cnt
            FROM commits GROUP BY hour ORDER BY hour
        """)
        stats['hourly'] = {row['hour']: row['cnt'] for row in cur.fetchall()}

        # 3. 顶级贡献者
        cur = self.conn.execute("""
            SELECT author_login, COUNT(*) as cnt 
            FROM commits GROUP BY author_login ORDER BY cnt DESC LIMIT 10
        """)
        stats['top_devs'] = {row['author_login']: row['cnt'] for row in cur.fetchall()}
        
        # 4. 总览数据
        stats['total_commits'] = self.conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
        stats['total_devs'] = self.conn.execute("SELECT COUNT(DISTINCT author_login) FROM commits").fetchone()[0]
        return stats
