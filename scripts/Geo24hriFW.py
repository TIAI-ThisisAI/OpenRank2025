# -*- coding: utf-8 -*-
"""
GitHub 高性能数据采集系统 
-----------------------------------------------------------
核心特性：
1. 异步 DB 驱动：使用 aiosqlite 消除数据库 I/O 阻塞。
2. 管道模式：基于 asyncio.Queue 的生产者-消费者架构，提升海量数据吞吐。
3. 智能容错：多级重试与 Token 调度算法，自动规避 Secondary Rate Limit。
4. 内存索引：基于集合的秒级去重，显著降低磁盘开销。
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

class GitHubCollectorError(Exception):
    """基础异常类"""
    pass

class AuthError(GitHubCollectorError):
    """身份验证失败"""
    pass

class RateLimitError(GitHubCollectorError):
    """触发限流"""
    pass

# ======================== 数据模型与配置 ========================

@dataclass(frozen=True)
class CommitRecord:
    """不可变提交记录模型"""
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
        return (
            self.commit_sha,
            self.repo_name,
            self.author_login,
            self.timestamp_unix,
            json.dumps(asdict(self))
        )

class AppConfig:
    """系统全局配置"""
    API_URL = "https://api.github.com/graphql"
    DB_PATH = "gh_enterprise_v3.db"
    LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    
    # 网络配置
    CONCURRENT_REPOS = 5      # 同时采集的仓库数量
    PAGE_SIZE = 100           # 每一页获取的记录数
    MAX_RETRIES = 5           # 最大重试次数
    TIMEOUT = aiohttp.ClientTimeout(total=120, connect=10)
    
    # 数据库配置
    WRITE_BATCH_SIZE = 200    # 缓冲区达到此数值后触发批量写入
    
    GRAPHQL_TEMPLATE = """
    query($owner: String!, $name: String!, $since: GitTimestamp, $until: GitTimestamp, $cursor: String) {
      repository(owner: $owner, name: $name) {
        defaultBranchRef {
          target {
            ... on Commit {
              history(first: %d, since: $since, until: $until, after: $cursor) {
                pageInfo { endCursor hasNextPage }
                edges {
                  node {
                    oid message committedDate
                    author {
                      name email
                      user { login location }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    """ % PAGE_SIZE

# ======================== 基础设施层 ========================

class TokenPool:
    """高性能 Token 调度中心"""
    def __init__(self, tokens: List[str]):
        self._tokens = {t.strip(): 0.0 for t in tokens if t.strip()}
        if not self._tokens:
            raise AuthError("未配置任何有效的 GitHub Personal Access Token")
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("TokenPool")

    async def get_best_token(self) -> str:
        """选择冷却时间最短的可用 Token"""
        async with self._lock:
            while True:
                now = time.time()
                available = [t for t, cooldown in self._tokens.items() if now >= cooldown]
                if available:
                    # 轮询策略
                    token = available[0]
                    # 将其排到末尾以平衡负载
                    self._tokens.pop(token)
                    self._tokens[token] = 0.0
                    return token
                
                wait_time = min(self._tokens.values()) - now + 0.5
                self._logger.warning(f"所有 Token 已限流，自动挂起 {wait_time:.1f}s")
                await asyncio.sleep(max(wait_time, 1))

    def penalize(self, token: str, duration: int = 600):
        """对触发错误的 Token 进行惩罚（进入冷却）"""
        self._tokens[token] = time.time() + duration
        self._logger.error(f"Token [{token[:8]}...] 触发限流，封锁 {duration}s")

class AsyncDatabase:
    """异步 SQLite 管理器"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.db_path)
        # 高级性能优化
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA cache_size=-64000") # 64MB 缓存
        
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS commits (
                sha TEXT PRIMARY KEY,
                repo TEXT,
                author_login TEXT,
                ts_unix INTEGER,
                raw_json TEXT
            )
        """)
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_repo_ts ON commits(repo, ts_unix)")
        await self._conn.commit()

    async def get_known_shas(self, repo: str) -> Set[str]:
        """获取指定仓库已存在的 SHA 缓存"""
        async with self._conn.execute("SELECT sha FROM commits WHERE repo = ?", (repo,)) as cursor:
            rows = await cursor.fetchall()
            return {row[0] for row in rows}

    async def save_batch(self, records: List[CommitRecord]):
        if not records: return
        data = [r.to_db_row() for r in records]
        await self._conn.executemany("INSERT OR IGNORE INTO commits VALUES (?,?,?,?,?)", data)
        await self._conn.commit()

    async def close(self):
        if self._conn:
            await self._conn.close()

# ======================== 核心逻辑层 ========================

class CollectionEngine:
    """数据采集引擎 (生产者-消费者)"""
    def __init__(self, token_pool: TokenPool, db: AsyncDatabase):
        self.token_pool = token_pool
        self.db = db
        self.data_queue = asyncio.Queue(maxsize=1000)
        self.is_running = True
        self.stats = {"total_saved": 0, "errors": 0}
        self._logger = logging.getLogger("Engine")

    async def _api_request(self, session: aiohttp.ClientSession, variables: dict) -> Optional[dict]:
        """封装重试与限流逻辑的原子请求"""
        for attempt in range(AppConfig.MAX_RETRIES):
            if not self.is_running: return None
            
            token = await self.token_pool.get_best_token()
            headers = {
                "Authorization": f"bearer {token}",
                "User-Agent": "GH-Enterprise-Collector-v3"
            }
            
            try:
                async with session.post(AppConfig.API_URL, json={
                    "query": AppConfig.GRAPHQL_TEMPLATE, 
                    "variables": variables
                }, headers=headers, timeout=AppConfig.TIMEOUT) as resp:
                    
                    if resp.status == 200:
                        res_json = await resp.json()
                        if "errors" in res_json:
                            err_msg = str(res_json["errors"])
                            if "rate limit" in err_msg.lower():
                                self.token_pool.penalize(token, 300)
                                continue
                            self._logger.error(f"GraphQL 逻辑错误: {err_msg[:200]}")
                            return None
                        return res_json
                    
                    if resp.status in (403, 429):
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        self.token_pool.penalize(token, retry_after)
                        continue
                        
                    self._logger.warning(f"HTTP {resp.status} 重试中 ({attempt+1})")
            except Exception as e:
                self._logger.debug(f"连接异常: {type(e).__name__}")
            
            await asyncio.sleep(2 ** attempt)
        return None

    async def repository_worker(self, repo_name: str, since: datetime, until: datetime, pbar: tqdm):
        """生产者：负责从 GitHub 抓取数据并推入队列"""
        if "/" not in repo_name: return
        owner, name = repo_name.split("/")
        
        # 加载内存去重缓存
        known_shas = await self.db.get_known_shas(repo_name)
        
        variables = {
            "owner": owner, "name": name,
            "since": since.isoformat(), "until": until.isoformat(),
            "cursor": None
        }

        async with aiohttp.ClientSession() as session:
            while self.is_running:
                data = await self._api_request(session, variables)
                if not data: break
                
                try:
                    target = data.get("data", {}).get("repository", {}).get("defaultBranchRef", {}).get("target")
                    if not target: break
                    
                    history = target.get("history", {})
                    edges = history.get("edges", [])
                    
                    batch = []
                    for edge in edges:
                        node = edge["node"]
                        sha = node["oid"]
                        
                        if sha in known_shas: continue # 内存级去重
                        
                        author_info = node.get("author", {})
                        user_node = author_info.get("user") or {}
                        
                        record = CommitRecord(
                            repo_name=repo_name,
                            commit_sha=sha,
                            timestamp_unix=int(datetime.fromisoformat(node["committedDate"].replace("Z", "+00:00")).timestamp()),
                            author_login=user_node.get("login") or "ghost",
                            author_name=author_info.get("name") or "Unknown",
                            author_email=author_info.get("email") or "",
                            location=user_node.get("location") or "",
                            message=node["message"][:500]
                        )
                        batch.append(record)
                        known_shas.add(sha)
                    
                    if batch:
                        await self.data_queue.put(batch)
                        pbar.update(len(batch))

                    page_info = history.get("pageInfo", {})
                    if page_info.get("hasNextPage") and self.is_running:
                        variables["cursor"] = page_info.get("endCursor")
                    else:
                        break
                except Exception as e:
                    self._logger.error(f"解析 {repo_name} 时发生异常: {e}")
                    break

    async def storage_worker(self):
        """消费者：负责将队列中的数据批量刷入磁盘"""
        buffer = []
        while self.is_running or not self.data_queue.empty():
            try:
                # 带有超时的等待，确保在停止任务时能及时响应
                batch = await asyncio.wait_for(self.data_queue.get(), timeout=2.0)
                buffer.extend(batch)
                
                if len(buffer) >= AppConfig.WRITE_BATCH_SIZE:
                    await self.db.save_batch(buffer)
                    self.stats["total_saved"] += len(buffer)
                    buffer = []
                
                self.data_queue.task_done()
            except asyncio.TimeoutError:
                if buffer:
                    await self.db.save_batch(buffer)
                    self.stats["total_saved"] += len(buffer)
                    buffer = []
                continue

# ======================== 任务管理与入口 ========================

class Application:
    def __init__(self, repos: List[str], tokens: List[str], days: int):
        self.repos = repos
        self.tokens = tokens
        self.days = days
        self.db = AsyncDatabase(AppConfig.DB_PATH)
        self.tp = TokenPool(tokens)
        self.engine = CollectionEngine(self.tp, self.db)

    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO, format=AppConfig.LOG_FORMAT)

    async def run(self):
        self._setup_logging()
        await self.db.connect()
        
        # 时间窗口
        until = datetime.now(timezone.utc)
        since = until - timedelta(days=self.days)
        
        # 信号处理
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: setattr(self.engine, 'is_running', False))

        print(f"🚀 任务启动 | 目标仓库: {len(self.repos)} | 追溯时长: {self.days}天")
        
        # 启动持久化消费者
        storage_task = asyncio.create_task(self.engine.storage_worker())
        
        # 启动并发生产者
        start_time = time.time()
        semaphore = asyncio.Semaphore(AppConfig.CONCURRENT_REPOS)
        
        async def sem_worker(repo, pbar):
            async with semaphore:
                await self.engine.repository_worker(repo, since, until, pbar)

        with tqdm(desc="数据抓取进度", unit="条") as pbar:
            tasks = [sem_worker(repo, pbar) for repo in self.repos]
            await asyncio.gather(*tasks)

        # 等待数据全部落盘
        self.engine.is_running = False
        await storage_task
        await self.db.close()
        
        elapsed = time.time() - start_time
        print(f"\n✅ 采集完成! ")
        print(f"总计持久化: {self.engine.stats['total_saved']} 条记录")
        print(f"有效耗时: {elapsed:.2f} 秒")
        print(f"平均吞吐: {self.engine.stats['total_saved']/elapsed:.1f} 条/秒")

# ======================== 启动逻辑 ========================

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="GitHub Enterprise Collector v3")
    parser.add_argument("--repos", nargs="+", required=True)
    parser.add_argument("--tokens", nargs="+", required=True)
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()

    app = Application(args.repos, args.tokens, args.days)
    await app.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
