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

# =============================================================================
# 模块 4: 外部服务层 (External Services)
# 职责: 封装与外部 API (GitHub, Nominatim) 的交互逻辑
# =============================================================================

class GeoService:
    """
    [服务模块] 地理编码服务
    
    职责: 将位置字符串 (如 "San Francisco") 转换为经纬度和国家代码。
    策略: 采用三级缓存策略 (内存 -> 数据库 -> API) 以最小化外部请求。
    """
    def __init__(self, session: aiohttp.ClientSession, storage: StorageManager, config: AppConfig):
        self.session = session
        self.storage = storage
        self.config = config
        # 并发控制: Nominatim 限制 1秒1次请求，使用 Semaphore 严格控制
        self._rate_limiter = asyncio.Semaphore(1)
        self._mem_cache = {} 

    async def resolve(self, location_str: str) -> Dict[str, Any]:
        """解析位置字符串，依次检查 L1(内存), L2(DB), L3(API)"""
        if not location_str or not location_str.strip():
            return self._empty_result()

        # L1: 内存缓存
        if location_str in self._mem_cache:
            return self._mem_cache[location_str]

        # L2: 数据库缓存
        cached = self.storage.get_geo_cache(location_str)
        if cached:
            self._mem_cache[location_str] = cached
            return cached

        # L3: 外部 API
        result = await self._fetch_from_api(location_str)
        
        # 回写缓存
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
                
                # 强制遵守 Nominatim 使用协议 (1秒/请求)
                await asyncio.sleep(1.1) 
                data = await resp.json()

                if data and isinstance(data, list) and len(data) > 0:
                    item = data[0]
                    # 从 display_name 中提取国家代码 (简易逻辑：取最后一段)
                    display_name = item.get("display_name", "")
                    country_code = "UNKNOWN"
                    if display_name:
                        parts = [p.strip() for p in display_name.split(",")]
                        if parts:
                            country_code = parts[-1].upper()[:3] 
                    
                    return {
                        "country_code": country_code,
                        "city": item.get("type", "unknown"),
                        "lat": float(item.get("lat", 0)),
                        "lon": float(item.get("lon", 0))
                    }
                return self._empty_result()

class GitHubService:
    """
    [服务模块] GitHub API 客户端
    
    职责: 获取仓库 Commits 和用户 Profile。
    特性: 自动处理 Rate Limit (速率限制)，支持分页流式获取。
    """
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
        """检查 API 速率限制，若耗尽则自动挂起等待"""
        if resp.status == 403 and 'X-RateLimit-Remaining' in resp.headers:
            remaining = int(resp.headers.get('X-RateLimit-Remaining', 1))
            if remaining == 0:
                reset_ts = int(resp.headers.get('X-RateLimit-Reset', 0))
                wait_time = max(reset_ts - time.time(), 0) + 1
                logger.warning(f"GitHub API 限流触发，自动等待 {wait_time:.0f}s")
                await asyncio.sleep(wait_time)
                return True
        return False

    @async_retry()
    async def get_user_location(self, username: str) -> str:
        """获取用户的 location 字段"""
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
                logger.debug(f"用户 {username} 信息获取失败: {resp.status}")

            self._user_info_cache[username] = location
            return location

    async def fetch_commits(self, repo: str, since: datetime) -> AsyncGenerator[List[Dict], None]:
        """
        [核心逻辑] 异步生成器获取 Commit
        
        返回:
            AsyncGenerator: 每次 yield 一页 (100条) commits 数据。
            允许调用方在获取数据的同时处理数据，避免等待所有页下载完成。
        """
        url = f"{self.config.GITHUB_API_BASE}/repos/{repo}/commits"
        params = {"since": since.isoformat(), "per_page": 100, "page": 1}
        
        while True:
            try:
                async with self.session.get(url, headers=self.base_headers, params=params) as resp:
                    if await self._handle_rate_limit(resp):
                        continue
                    if resp.status != 200:
                        if resp.status == 404:
                            logger.error(f"仓库不可见或不存在: {repo}")
                        break
                        
                    batch = await resp.json()
                    if not batch or not isinstance(batch, list):
                        break
                    
                    yield batch
                    
                    if len(batch) < 100: break
                    params["page"] += 1
            except Exception as e:
                logger.error(f"Fetch loop error for {repo}: {e}")
                break

# =============================================================================
# 模块 5: 业务逻辑控制层 (Controller / Orchestrator)
# 职责: 协调数据库和服务，调度并发任务，执行核心 ETL 流程
# =============================================================================

class InsightEngine:
    """
    [控制器模块] 核心引擎
    
    职责:
        1. 初始化资源 (DB, Http Session)。
        2. 调度并发任务处理多个仓库。
        3. 协调 '获取数据 -> 过滤去重 -> 补全地理信息 -> 持久化' 的流水线。
    """
    def __init__(self, config: AppConfig):
        self.config = config
    
    async def run(self, projects: List[str]):
        # 初始化数据库上下文
        with StorageManager(self.config.db_path) as storage:
            # 初始化 HTTP 连接池 (限制并发数)
            conn = aiohttp.TCPConnector(limit=self.config.concurrency)
            async with aiohttp.ClientSession(connector=conn) as session:
                # 依赖注入
                self.gh_service = GitHubService(session, self.config.github_token, self.config)
                self.geo_service = GeoService(session, storage, self.config)
                self.storage = storage 
                
                since_date = datetime.now(timezone.utc) - timedelta(days=self.config.lookback_days)
                logger.info(f"开始分析任务 | 项目数: {len(projects)} | 周期: {self.config.lookback_days}天")

                # 创建并发任务列表
                tasks = [self._process_single_repo(p, since_date) for p in projects]
                # 使用 tqdm 显示总体进度
                await tqdm.gather(*tasks, desc="Total Progress", unit="repo")
                
                logger.info("数据采集完成，正在生成报告...")
                self._generate_report()

    async def _process_single_repo(self, repo: str, since: datetime):
        """处理单个仓库的完整流程"""
        new_commits_buffer = []
        commit_count = 0
        
        try:
            # 1. 异步流式获取 Commits
            async for batch in self.gh_service.fetch_commits(repo, since):
                # 2. 增量检测: 过滤掉数据库中已存在的 SHA
                shas_in_batch = [item['sha'] for item in batch if item.get('sha')]
                existing_shas = self.storage.filter_existing_shas(shas_in_batch)
                
                to_process = [
                    item for item in batch 
                    if item.get('sha') not in existing_shas and item.get('author')
                ]

                if not to_process:
                    continue

                # 3. 数据处理与补全
                for item in to_process:
                    author_login = item['author']['login']
                    sha = item['sha']
                    
                    # 获取并解析地理位置 (串行 await，保证顺序和限流)
                    raw_loc = await self.gh_service.get_user_location(author_login)
                    geo_info = await self.geo_service.resolve(raw_loc)
                    
                    # 时间格式标准化
                    ts_str = item['commit']['author']['date'].replace("Z", "+00:00")
                    commit_ts = datetime.fromisoformat(ts_str).timestamp()

                    new_commits_buffer.append(CommitRecord(
                        sha=sha, repo_name=repo, author_login=author_login,
                        timestamp=int(commit_ts), raw_location=raw_loc, **geo_info
                    ))
                
                # 4. 分批持久化 (防止内存溢出)
                if new_commits_buffer:
                    self.storage.save_commits(new_commits_buffer)
                    commit_count += len(new_commits_buffer)
                    new_commits_buffer.clear()

        except Exception as e:
            logger.error(f"处理仓库 {repo} 时发生意外错误: {e}")
        
        if commit_count > 0:
            logger.info(f"[{repo}] 完成，新增记录: {commit_count}")

    def _generate_report(self):
        """调用视图层生成最终报告"""
        stats = self.storage.get_statistics()
        if not stats.get('total_commits'):
            logger.warning("数据库中无数据，跳过报告生成")
            return
        
        ReportGenerator(self.config.report_path).render(stats)
        
        abs_path = os.path.abspath(self.config.report_path)
        try:
            webbrowser.open(f"file://{abs_path}")
        except Exception:
            logger.info(f"报告已保存至: {abs_path}")
