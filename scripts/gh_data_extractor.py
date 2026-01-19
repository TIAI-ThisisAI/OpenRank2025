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

# -----------------------------------------------------------------------------
# 服务层 (Service Layer)
# -----------------------------------------------------------------------------
class GeoService:
    def __init__(self, session: aiohttp.ClientSession, storage: StorageManager, config: AppConfig):
        self.session = session
        self.storage = storage
        self.config = config
        # [关键逻辑] 并发控制
        # Nominatim API 有严格的使用策略，使用 Semaphore 限制并发数为 1。
        self._rate_limiter = asyncio.Semaphore(1)
        self._mem_cache = {} 

    async def resolve(self, location_str: str) -> Dict[str, Any]:
        """
        [关键逻辑] 三级缓存策略
        1. L1 内存缓存: 速度最快，进程级复用。
        2. L2 数据库缓存 (geo_cache): 持久化存储，避免重启后重新请求 API。
        3. L3 外部 API: 仅在前两层未命中时调用，且调用后会回写缓存。
        """
        if not location_str or not location_str.strip():
            return self._empty_result()

        # 1. 内存缓存
        if location_str in self._mem_cache:
            return self._mem_cache[location_str]

        # 2. 数据库缓存
        cached = self.storage.get_geo_cache(location_str)
        if cached:
            self._mem_cache[location_str] = cached
            return cached

        # 3. API 请求
        result = await self._fetch_from_api(location_str)
        
        # 4. 更新缓存
        self.storage.save_geo_cache(location_str, result)
        self._mem_cache[location_str] = result
        return result

    def _empty_result(self):
        return {"country_code": "UNKNOWN", "city": "", "lat": 0.0, "lon": 0.0}

    @async_retry(retries=2, delay=2)
    async def _fetch_from_api(self, query: str) -> Dict[str, Any]:
        async with self._rate_limiter:
            params = {"q": query, "format": "json", "limit": 1, "accept-language": "en"}
            headers = {"User-Agent": self.config.USER_AGENT}
            
            async with self.session.get(self.config.NOMINATIM_API, params=params, headers=headers) as resp:
                if resp.status != 200:
                    return self._empty_result()
                
                # [关键逻辑] 强制限流
                # 即使有 Semaphore，仍强制 sleep 1.1秒，严格遵守 Nominatim 1秒/次 的协议要求。
                await asyncio.sleep(1.1) 
                data = await resp.json()

                if data and isinstance(data, list) and len(data) > 0:
                    item = data[0]
                    # 更健壮的国家代码提取
                    display_name = item.get("display_name", "")
                    country_code = "UNKNOWN"
                    if display_name:
                        parts = [p.strip() for p in display_name.split(",")]
                        if parts:
                            # 尝试取最后一段作为国家
                            country_code = parts[-1].upper()[:3] 
                    
                    return {
                        "country_code": country_code,
                        "city": item.get("type", "unknown"),
                        "lat": float(item.get("lat", 0)),
                        "lon": float(item.get("lon", 0))
                    }
                return self._empty_result()

class GitHubService:
    def __init__(self, session: aiohttp.ClientSession, token: str, config: AppConfig):
        self.session = session
        self.config = config
        self.base_headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": config.USER_AGENT
        }
        self._user_info_cache: Dict[str, str] = {}

    async def _handle_rate_limit(self, resp: aiohttp.ClientResponse):
        """
        [关键逻辑] API 速率限制处理
        检测 GitHub 返回的 403 及 RateLimit header。
        如果耗尽，计算重置时间并挂起当前协程，而不是直接抛出错误。
        """
        if resp.status == 403 and 'X-RateLimit-Remaining' in resp.headers:
            remaining = int(resp.headers.get('X-RateLimit-Remaining', 1))
            if remaining == 0:
                reset_ts = int(resp.headers.get('X-RateLimit-Reset', 0))
                wait_time = max(reset_ts - time.time(), 0) + 1
                logger.warning(f"Rate Limit 触发，等待 {wait_time:.0f}s")
                await asyncio.sleep(wait_time)
                return True
        return False

    @async_retry()
    async def get_user_location(self, username: str) -> str:
        if not username: return ""
        if username in self._user_info_cache:
            return self._user_info_cache[username]

        url = f"{self.config.GITHUB_API_BASE}/users/{username}"
        async with self.session.get(url, headers=self.base_headers) as resp:
            if await self._handle_rate_limit(resp):
                return await self.get_user_location(username)
            
            location = ""
            if resp.status == 200:
                data = await resp.json()
                location = data.get("location") or ""
            elif resp.status != 404:
                logger.debug(f"用户 {username} 获取失败: {resp.status}")

            self._user_info_cache[username] = location
            return location

    async def fetch_commits(self, repo: str, since: datetime) -> AsyncGenerator[List[Dict], None]:
        url = f"{self.config.GITHUB_API_BASE}/repos/{repo}/commits"
        params = {"since": since.isoformat(), "per_page": 100, "page": 1}
        
        while True:
            try:
                async with self.session.get(url, headers=self.base_headers, params=params) as resp:
                    # 调用限流检查，如果触发了等待，则跳过本次循环重新请求
                    if await self._handle_rate_limit(resp):
                        continue
                    if resp.status != 200:
                        if resp.status == 404:
                            logger.error(f"仓库不可见: {repo}")
                        break
                        
                    batch = await resp.json()
                    if not batch or not isinstance(batch, list):
                        break
                    
                    # [关键逻辑] 异步生成器
                    # 每次 yield 一页数据（100条），调用方可以流式处理，避免一次性加载所有 commit 导致内存爆炸。
                    yield batch
                    
                    if len(batch) < 100: break # 如果不满一页，说明是最后一页
                    params["page"] += 1
            except Exception as e:
                logger.error(f"Fetch loop error for {repo}: {e}")
                break
