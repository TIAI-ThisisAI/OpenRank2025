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

# -----------------------------------------------------------------------------
# 持久化层 (Storage Layer - Optimized)
# -----------------------------------------------------------------------------
class StorageManager:
    """
    负责 SQLite 操作。
    优化点：使用上下文管理器保持连接，避免频繁打开/关闭文件的 IO 开销。
    """
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.conn: Optional[sqlite3.Connection] = None

    # [关键逻辑] 上下文管理器协议
    # 确保使用 with 语句时数据库连接自动建立，退出时自动关闭，防止资源泄漏。
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
        if not self.conn: return
        with self.conn:
            # [关键逻辑] 缓存表设计
            # 使用 raw_text (API返回的原始位置字符串) 作为主键，避免重复查询相同的地址字符串。
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
        cursor = self.conn.execute(
            "SELECT country_code, city, lat, lon FROM geo_cache WHERE raw_text = ?", 
            (raw_text,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def save_geo_cache(self, raw_text: str, data: Dict[str, Any]):
        with self.conn:
            self.conn.execute(
                """INSERT OR REPLACE INTO geo_cache (raw_text, country_code, city, lat, lon) 
                   VALUES (?, ?, ?, ?, ?)""",
                (raw_text, data.get('country_code'), data.get('city'), data.get('lat'), data.get('lon'))
            )

    def filter_existing_shas(self, shas: List[str]) -> Set[str]:
        """
        [关键逻辑] 批量去重优化
        在处理一批 commit 之前，一次性查询数据库中已存在的 SHA。
        避免了 N+1 问题（即避免对每个 commit 都查询一次 DB），显著提升处理速度。
        """
        if not shas: return set()
        # SQLite 限制参数数量，大批量需分块，这里简化处理，假设 batch size 较小(100)
        placeholders = ','.join(['?'] * len(shas))
        cursor = self.conn.execute(f"SELECT sha FROM commits WHERE sha IN ({placeholders})", shas)
        return {row['sha'] for row in cursor.fetchall()}

    def save_commits(self, commits: List[CommitRecord]):
        if not commits: return
        data = [
            (c.sha, c.repo_name, c.author_login, c.timestamp, c.country_code, c.lat, c.lon)
            for c in commits
        ]
        with self.conn:
            # [关键逻辑] 批量写入
            # 使用 executemany 进行事务性批量插入，比逐条 insert 快得多。
            self.conn.executemany(
                "INSERT OR IGNORE INTO commits VALUES (?, ?, ?, ?, ?, ?, ?)", 
                data
            )

    def get_statistics(self) -> Dict[str, Any]:
        stats = {}
        # 国家分布
        cur = self.conn.execute("""
            SELECT country_code, COUNT(*) as cnt 
            FROM commits 
            WHERE country_code NOT IN ('UNKNOWN', '') 
            GROUP BY country_code ORDER BY cnt DESC LIMIT 20
        """)
        stats['countries'] = {row['country_code']: row['cnt'] for row in cur.fetchall()}

        # 活跃时间 (UTC)
        cur = self.conn.execute("""
            SELECT strftime('%H', datetime(timestamp, 'unixepoch')) as hour, COUNT(*) as cnt
            FROM commits GROUP BY hour ORDER BY hour
        """)
        stats['hourly'] = {row['hour']: row['cnt'] for row in cur.fetchall()}

        # 顶级贡献者
        cur = self.conn.execute("""
            SELECT author_login, COUNT(*) as cnt 
            FROM commits GROUP BY author_login ORDER BY cnt DESC LIMIT 10
        """)
        stats['top_devs'] = {row['author_login']: row['cnt'] for row in cur.fetchall()}
        
        # 总览
        stats['total_commits'] = self.conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
        stats['total_devs'] = self.conn.execute("SELECT COUNT(DISTINCT author_login) FROM commits").fetchone()[0]
        return stats
