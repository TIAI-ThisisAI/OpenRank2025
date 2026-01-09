# -*- coding: utf-8 -*-
"""
GitHub 高性能数据采集系统 (关键代码注释版)
-----------------------------------------------------------
核心架构解析：
1. 并发模型：使用 asyncio 实现完全异步 I/O。
2. 调度策略：TokenPool 实现多 Token 轮询与智能冷却。
3. 数据流：Producer(API) -> Queue -> Consumer(DB) 管道模式。
"""

import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# 第三方依赖检查
try:
    import aiohttp
    import aiosqlite
    from tqdm.asyncio import tqdm
except ImportError:
    print("错误: 缺少必要依赖。请执行: pip install aiohttp aiosqlite tqdm")
    sys.exit(1)

# ======================== 核心异常体系 ========================
# (此处省略基础异常类定义，与原版一致)
class GitHubCollectorError(Exception): pass
class AuthError(GitHubCollectorError): pass
class RateLimitError(GitHubCollectorError): pass

# ======================== 数据模型与配置 ========================

@dataclass(frozen=True)
class CommitRecord:
    # ... (省略字段定义)
    repo_name: str
    commit_sha: str
    timestamp_unix: int
    author_login: str
    author_name: str
    author_email: str
    location: str
    message: str
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_db_row(self) -> Tuple:
        # 将对象转换为元组，方便 SQLite 批量插入
        return (
            self.commit_sha,
            self.repo_name,
            self.author_login,
            self.timestamp_unix,
            json.dumps(asdict(self))
        )

class AppConfig:
    # ... (常规配置省略)
    API_URL = "https://api.github.com/graphql"
    DB_PATH = "gh_enterprise_v3.db"
    LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    
    # [关键配置] 网络并发与重试参数
    CONCURRENT_REPOS = 5          # 信号量控制：同时并发采集的仓库数
    PAGE_SIZE = 100               # GraphQL 单次请求最大条数
    MAX_RETRIES = 5               # 指数退避的最大重试次数
    TIMEOUT = aiohttp.ClientTimeout(total=120, connect=10)
    
    # [关键配置] 数据库写入优化
    WRITE_BATCH_SIZE = 200        # 批量写入阈值：攒够 200 条再写库，减少 IO 次数
    
    # GraphQL 查询模板 (省略具体内容)
    GRAPHQL_TEMPLATE = """...""" % PAGE_SIZE

# ======================== 基础设施层 (关键) ========================

class TokenPool:
    """
    [核心模块] Token 调度中心
    作用：管理多个 GitHub Token，自动轮询并在触发限流时挂起特定 Token
    """
    def __init__(self, tokens: List[str]):
        # 初始化所有 Token 的冷却结束时间为 0 (即刻可用)
        self._tokens = {t.strip(): 0.0 for t in tokens if t.strip()}
        if not self._tokens:
            raise AuthError("未配置任何有效的 GitHub Personal Access Token")
        self._lock = asyncio.Lock() # 协程锁，防止多任务同时修改 Token 状态
        self._logger = logging.getLogger("TokenPool")

    async def get_best_token(self) -> str:
        """选择冷却时间最短的可用 Token"""
        async with self._lock:
            while True:
                now = time.time()
                # 筛选出当前时间大于冷却时间的 Token
                available = [t for t, cooldown in self._tokens.items() if now >= cooldown]
                if available:
                    # 简单轮询：取第一个，用完后弹出并插到字典末尾 (模拟 Round-Robin)
                    token = available[0]
                    self._tokens.pop(token)
                    self._tokens[token] = 0.0
                    return token
                
                # 若无可用 Token，计算最小等待时间并挂起所有协程
                wait_time = min(self._tokens.values()) - now + 0.5
                self._logger.warning(f"所有 Token 已限流，自动挂起 {wait_time:.1f}s")
                await asyncio.sleep(max(wait_time, 1))

    def penalize(self, token: str, duration: int = 600):
        """
        [关键逻辑] 熔断机制
        当某个 Token 触发 403/429 错误时，将其标记为不可用状态持续 duration 秒
        """
        self._tokens[token] = time.time() + duration
        self._logger.error(f"Token [{token[:8]}...] 触发限流，封锁 {duration}s")

class AsyncDatabase:
    """异步 SQLite 管理器 (针对高并发写入优化)"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.db_path)
        # [关键优化] 开启 WAL (Write-Ahead Logging) 模式
        # 允许读写并发，极大提升 SQLite 在异步环境下的写入性能
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL") # 降低 fsync 频率，牺牲极小安全性换取性能
        await self._conn.execute("PRAGMA cache_size=-64000") # 分配 64MB 内存缓存
        
        # 建表逻辑 (省略)
        await self._conn.execute("""CREATE TABLE IF NOT EXISTS commits ...""")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_repo_ts ON commits(repo, ts_unix)")
        await self._conn.commit()

    async def get_known_shas(self, repo: str) -> Set[str]:
        """读取已存在的 SHA，用于内存级去重"""
        async with self._conn.execute("SELECT sha FROM commits WHERE repo = ?", (repo,)) as cursor:
            rows = await cursor.fetchall()
            return {row[0] for row in rows}

    async def save_batch(self, records: List[CommitRecord]):
        """[关键逻辑] 批量事务写入"""
        if not records: return
        data = [r.to_db_row() for r in records]
        # 使用 executemany 在单个事务中插入多条数据，速度比单条 insert 快数倍
        await self._conn.executemany("INSERT OR IGNORE INTO commits VALUES (?,?,?,?,?)", data)
        await self._conn.commit()

    async def close(self):
        if self._conn: await self._conn.close()

# ======================== 核心逻辑层 (生产者-消费者) ========================

class CollectionEngine:
    def __init__(self, token_pool: TokenPool, db: AsyncDatabase):
        self.token_pool = token_pool
        self.db = db
        # [关键组件] 异步队列：解耦抓取(生产者)和存储(消费者)
        # maxsize 防止生产速度过快导致内存溢出
        self.data_queue = asyncio.Queue(maxsize=1000)
        self.is_running = True
        self.stats = {"total_saved": 0, "errors": 0}
        self._logger = logging.getLogger("Engine")

    async def _api_request(self, session: aiohttp.ClientSession, variables: dict) -> Optional[dict]:
        """封装了重试机制和限流处理的原子请求"""
        for attempt in range(AppConfig.MAX_RETRIES):
            if not self.is_running: return None
            
            token = await self.token_pool.get_best_token() # 获取最佳 Token
            headers = {"Authorization": f"bearer {token}", "User-Agent": "GH-Enterprise-Collector-v3"}
            
            try:
                async with session.post(AppConfig.API_URL, json={...}, headers=headers, timeout=AppConfig.TIMEOUT) as resp:
                    
                    if resp.status == 200:
                        res_json = await resp.json()
                        # 检查 GraphQL 业务层面的限流错误
                        if "errors" in res_json and "rate limit" in str(res_json["errors"]).lower():
                            self.token_pool.penalize(token, 300) # 惩罚该 Token
                            continue
                        return res_json
                    
                    # [关键逻辑] 处理 API 层面限流 (403 Forbidden / 429 Too Many Requests)
                    if resp.status in (403, 429):
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        self.token_pool.penalize(token, retry_after) # 自动冷却该 Token
                        continue
                        
                    self._logger.warning(f"HTTP {resp.status} 重试中 ({attempt+1})")
            except Exception as e:
                # 网络级异常捕获，触发指数退避等待 (2^attempt)
                await asyncio.sleep(2 ** attempt)
        return None

    async def repository_worker(self, repo_name: str, since: datetime, until: datetime, pbar: tqdm):
        """[生产者] 负责从 GitHub 抓取数据并推入队列"""
        # ... (参数解析省略)
        
        # 1. 预加载去重集合 (Memory Cache)
        known_shas = await self.db.get_known_shas(repo_name)
        
        variables = {"owner": owner, "name": name, "since": since.isoformat(), "until": until.isoformat(), "cursor": None}

        async with aiohttp.ClientSession() as session:
            while self.is_running:
                data = await self._api_request(session, variables) # 发起请求
                if not data: break
                
                # ... (JSON 解析逻辑省略)
                
                batch = []
                for edge in edges:
                    sha = node["oid"]
                    if sha in known_shas: continue # [关键优化] 内存去重，避免重复处理
                    
                    # ... (构建 CommitRecord 对象)
                    batch.append(record)
                    known_shas.add(sha)
                
                # 2. 将数据块推入队列，如果队列满则自动阻塞等待消费者消费
                if batch:
                    await self.data_queue.put(batch)
                    pbar.update(len(batch)) # 更新进度条

                # 3. 处理分页 Cursor
                page_info = history.get("pageInfo", {})
                if page_info.get("hasNextPage") and self.is_running:
                    variables["cursor"] = page_info.get("endCursor")
                else:
                    break

    async def storage_worker(self):
        """[消费者] 负责将队列中的数据批量刷入磁盘"""
        buffer = []
        # 只要系统在运行，或者队列里还有剩余数据，就持续工作
        while self.is_running or not self.data_queue.empty():
            try:
                # [关键逻辑] 带超时的获取
                # 即使队列为空，每 2 秒也会唤醒一次，检查是否需要将 buffer 中的残余数据写入
                batch = await asyncio.wait_for(self.data_queue.get(), timeout=2.0)
                buffer.extend(batch)
                
                # 缓冲区达到阈值 (200条) -> 触发写盘
                if len(buffer) >= AppConfig.WRITE_BATCH_SIZE:
                    await self.db.save_batch(buffer)
                    self.stats["total_saved"] += len(buffer)
                    buffer = []
                
                self.data_queue.task_done()
            except asyncio.TimeoutError:
                # 超时处理：如果 buffer 有数据但不够 200 条，也强制写入，防止数据滞留
                if buffer:
                    await self.db.save_batch(buffer)
                    self.stats["total_saved"] += len(buffer)
                    buffer = []
                continue

# ======================== 任务管理 ========================

class Application:
    # ... (初始化省略)

    async def run(self):
        # ... (DB连接与时间计算省略)
        
        # 信号处理：优雅停机，确保数据不丢失
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: setattr(self.engine, 'is_running', False))

        # 启动唯一的消费者任务 (Storage Worker)
        storage_task = asyncio.create_task(self.engine.storage_worker())
        
        # [关键逻辑] 使用 Semaphore 控制最大并发仓库数量
        # 防止同时发起过多连接导致本机 fd 耗尽或被 GitHub 判定为滥用
        semaphore = asyncio.Semaphore(AppConfig.CONCURRENT_REPOS)
        
        async def sem_worker(repo, pbar):
            async with semaphore:
                await self.engine.repository_worker(repo, since, until, pbar)

        # 启动所有生产者任务
        with tqdm(desc="数据抓取进度", unit="条") as pbar:
            tasks = [sem_worker(repo, pbar) for repo in self.repos]
            await asyncio.gather(*tasks)

        # 任务结束清理流程
        self.engine.is_running = False
        await storage_task # 等待消费者处理完剩余数据
        await self.db.close()
        
        # ... (打印统计信息)

# ... (Main 入口省略)
