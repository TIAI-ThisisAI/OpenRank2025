"""
GitHub 高性能数据采集系统 (Single File Modularized Edition)

设计理念:
    1. [Config] 配置模块: 管理常量、环境变量和 API 模板
    2. [Models] 模型模块: 定义数据结构和序列化逻辑
    3. [Infrastructure] 基础设施模块: 处理数据库和 Token 资源池
    4. [Core] 核心逻辑模块: 实现生产者-消费者采集引擎
    5. [Main] 入口模块: 依赖检查、任务编排与生命周期管理
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

# ==============================================================================
# 0. 全局初始化与依赖检查
# ==============================================================================

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("GH-Collector")

# 第三方依赖检查
try:
    import aiohttp
    import aiosqlite
    from tqdm.asyncio import tqdm
except ImportError:
    print("错误: 缺少必要依赖。请执行: pip install aiohttp aiosqlite tqdm")
    sys.exit(1)

# ==============================================================================
# 模块 1: [Config] 配置层
# 功能: 集中管理所有硬编码参数、SQL模板和环境变量
# ==============================================================================

class AppConfig:
    """应用程序配置容器"""
    
    # 基础配置
    API_URL = "https://api.github.com/graphql"
    DB_PATH = "gh_data_optimized.db"
    
    # 身份认证：支持从环境变量读取逗号分隔的多个 Token
    GITHUB_TOKENS = [t.strip() for t in os.getenv("GITHUB_TOKENS", "").split(",") if t.strip()]
    
    # 性能参数
    CONCURRENT_REPOS = 5    # 并发采集仓库数 (Semaphore)
    PAGE_SIZE = 100         # 单次请求获取的 Commit 数
    MAX_RETRIES = 5         # API 请求重试次数
    WRITE_BATCH_SIZE = 50   # 数据库批量写入阈值
    
    # 网络超时 (总计60秒, 连接10秒)
    TIMEOUT = aiohttp.ClientTimeout(total=60, connect=10)

    # GraphQL 查询模板 (优化载荷，仅查询必要字段)
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

# ==============================================================================
# 模块 2: [Models] 数据模型层
# 功能: 定义核心数据结构，解耦业务逻辑与数据存储格式
# ==============================================================================

@dataclass(frozen=True)
class CommitRecord:
    """
    Commit 业务实体
    使用 frozen=True 确保数据不可变，线程安全
    """
    repo_name: str
    commit_sha: str
    timestamp_unix: int
    author_login: str
    author_name: str
    author_email: str
    message: str

    def to_db_row(self) -> tuple:
        """转换逻辑：将对象序列化为数据库行格式"""
        return (
            self.commit_sha,
            self.repo_name,
            self.author_login,
            self.timestamp_unix,
            self.message,
            # 保留原始数据的 JSON 备份，便于后续扩展字段
            json.dumps(asdict(self))
        )

# ==============================================================================
# 模块 3: [Infrastructure] 基础设施层
# 功能: 提供底层通用服务 (Token池、数据库连接)，不包含具体业务逻辑
# ==============================================================================

class TokenPool:
    """
    Token 资源管理器
    职责：负载均衡 (Round-Robin) 与 速率限制 (Rate Limiting)
    """
    def __init__(self, tokens: List[str]):
        # 记录 Token 的冷却结束时间戳 (0.0 表示可用)
        self._tokens = {t: 0.0 for t in tokens}
        self._lock = asyncio.Lock() # 保证并发安全
        
        if not self._tokens:
            logger.warning("未检测到 Token，将尝试匿名访问 (极易受限)")

    async def get_token(self) -> Optional[str]:
        """获取可用 Token，若全部冷却则阻塞等待"""
        if not self._tokens: return None
        
        async with self._lock:
            while True:
                now = time.time()
                # 筛选当前可用的 Token
                available = [t for t, cd in self._tokens.items() if now >= cd]
                
                if available:
                    token = available[0]
                    # 轮询策略：取出并放回队尾
                    del self._tokens[token]
                    self._tokens[token] = 0.0
                    return token
                
                # 若无可用，计算最小等待时间
                wait_time = min(self._tokens.values()) - now + 0.5
                logger.warning(f"所有 Token 冷却中，等待 {wait_time:.1f}s")
                await asyncio.sleep(max(wait_time, 1))

    def penalize(self, token: str, duration: int = 600):
        """惩罚机制：将触发限流的 Token 暂时移出可用池"""
        if token in self._tokens:
            self._tokens[token] = time.time() + duration
            logger.warning(f"Token [...{token[-4:]}] 冷却 {duration}s")

class AsyncDatabase:
    """
    异步数据库包装器
    职责：连接管理、Schema 初始化、高性能批量写入
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        """建立连接并开启 WAL 模式优化性能"""
        self._conn = await aiosqlite.connect(self.db_path)
        # WAL 模式允许读写并发，极大提升吞吐量
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        
        # 初始化表结构
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
        # 索引优化
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_repo_ts ON commits(repo, ts_unix)")
        await self._conn.commit()

    async def save_batch(self, records: List[CommitRecord]):
        """批量写入，使用 INSERT OR IGNORE 自动去重"""
        if not records: return
        try:
            await self._conn.executemany(
                "INSERT OR IGNORE INTO commits VALUES (?,?,?,?,?,?)", 
                [r.to_db_row() for r in records]
            )
            await self._conn.commit()
        except Exception as e:
            logger.error(f"DB 写入异常: {e}")

    async def close(self):
        if self._conn: await self._conn.close()
