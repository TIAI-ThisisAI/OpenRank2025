"""
GitHub 高性能数据采集系统 (Refactored)
核心逻辑注释版
"""
import asyncio
import json
import logging
import os
import signal
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Any

# 第三方依赖检查
try:
    import aiohttp
    import aiosqlite
    from tqdm.asyncio import tqdm
except ImportError:
    print("错误: 缺少必要依赖。请执行: pip install aiohttp aiosqlite tqdm")
    sys.exit(1)

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("GH-Collector")

# ======================== 配置与模型 ========================
class AppConfig:
    API_URL = "https://api.github.com/graphql"
    DB_PATH = "gh_data_optimized.db"
    
    # 环境变量读取，处理逗号分隔的多个 Token
    GITHUB_TOKENS = [t.strip() for t in os.getenv("GITHUB_TOKENS", "").split(",") if t.strip()]
    
    CONCURRENT_REPOS = 5  # 限制同时并发采集的仓库数量
    PAGE_SIZE = 100       # GraphQL 单页最大条数
    MAX_RETRIES = 5       # API 请求最大重试次数
    TIMEOUT = aiohttp.ClientTimeout(total=60, connect=10)
    WRITE_BATCH_SIZE = 50 # 数据库批量写入的阈值，减少 IO 次数

    # GraphQL 查询模板：只获取必要的字段以减少载荷
    GRAPHQL_TEMPLATE = """
    query($owner: String!, $name: String!, $since: GitTimestamp!, $until: GitTimestamp!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        defaultBranchRef {
          target {
            ... on Commit {
              history(since: $since, until: $until, first: %d, after: $cursor) {
                pageInfo { hasNextPage endCursor }
                edges {
                  node {
                    oid
                    messageHeadline
                    committedDate
                    author {
                      name
                      email
                      user { login }
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

@dataclass(frozen=True)
class CommitRecord:
    repo_name: str
    commit_sha: str
    timestamp_unix: int
    author_login: str
    author_name: str
    author_email: str
    message: str

    def to_db_row(self) -> tuple:
        return (
            self.commit_sha,
            self.repo_name,
            self.author_login,
            self.timestamp_unix,
            self.message,
            json.dumps(asdict(self))
        )

# ======================== 基础设施层 ========================

class TokenPool:
    """Token 资源池，负责负载均衡和速率限制管理"""
    def __init__(self, tokens: List[str]):
        # 记录每个 Token 的冷却结束时间戳 (0.0 表示立即可用)
        self._tokens = {t: 0.0 for t in tokens}
        self._lock = asyncio.Lock() # 保证并发环境下 Token 选取的原子性
        if not self._tokens:
            logger.warning("未检测到 Token，将尝试匿名访问 (极易受限)")

    async def get_token(self) -> Optional[str]:
        if not self._tokens: return None
        
        async with self._lock:
            while True:
                now = time.time()
                # 筛选出当前时间已经冷却完毕的 Token
                available = [t for t, cd in self._tokens.items() if now >= cd]
                
                if available:
                    token = available[0]
                    # 轮询策略：取出一个后，将其从字典中删除并重新插入到末尾
                    # 这实现了简单的 Round-Robin 调度，均匀使用 Token
                    del self._tokens[token]
                    self._tokens[token] = 0.0
                    return token
                
                # 若无可用 Token，计算最小等待时间并挂起协程
                # 避免忙轮询 (Busy Waiting) 占用 CPU
                wait_time = min(self._tokens.values()) - now + 0.5
                logger.warning(f"所有 Token 冷却中，等待 {wait_time:.1f}s")
                await asyncio.sleep(max(wait_time, 1))

    def penalize(self, token: str, duration: int = 600):
        """对触发限流的 Token 进行惩罚，暂时移出可用池"""
        if token in self._tokens:
            self._tokens[token] = time.time() + duration
            logger.warning(f"Token [...{token[-4:]}] 冷却 {duration}s")

class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.db_path)
        # 关键性能优化：开启 WAL (Write-Ahead Logging) 模式
        # 允许读写并发，极大提高写入吞吐量
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS commits (
                sha TEXT PRIMARY KEY,
                repo TEXT,
                author_login TEXT,
                ts_unix INTEGER,
                message TEXT,
                raw_json TEXT
            )
        """)
        # 创建索引加速后续查询
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_repo_ts ON commits(repo, ts_unix)")
        await self._conn.commit()

    async def save_batch(self, records: List[CommitRecord]):
        if not records: return
        try:
            # 使用 INSERT OR IGNORE 自动处理主键冲突 (去重)
            # 比在 Python 代码中检查存在性更高效且原子化
            await self._conn.executemany(
                "INSERT OR IGNORE INTO commits VALUES (?,?,?,?,?,?)", 
                [r.to_db_row() for r in records]
            )
            await self._conn.commit()
        except Exception as e:
            logger.error(f"DB 写入异常: {e}")

    async def close(self):
        if self._conn: await self._conn.close()
