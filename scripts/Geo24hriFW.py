"""
GitHub 高性能数据采集系统 (Refactored)
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
    
    # 环境变量读取
    GITHUB_TOKENS = [t.strip() for t in os.getenv("GITHUB_TOKENS", "").split(",") if t.strip()]
    
    CONCURRENT_REPOS = 5
    PAGE_SIZE = 100
    MAX_RETRIES = 5
    TIMEOUT = aiohttp.ClientTimeout(total=60, connect=10)
    WRITE_BATCH_SIZE = 50

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
    def __init__(self, tokens: List[str]):
        self._tokens = {t: 0.0 for t in tokens}
        self._lock = asyncio.Lock()
        if not self._tokens:
            logger.warning("未检测到 Token，将尝试匿名访问 (极易受限)")

    async def get_token(self) -> Optional[str]:
        if not self._tokens: return None
        
        async with self._lock:
            while True:
                now = time.time()
                # 筛选可用 Token
                available = [t for t, cd in self._tokens.items() if now >= cd]
                
                if available:
                    token = available[0]
                    # 轮询策略: 移到末尾
                    del self._tokens[token]
                    self._tokens[token] = 0.0
                    return token
                
                # 若无可用，计算最小等待时间
                wait_time = min(self._tokens.values()) - now + 0.5
                logger.warning(f"所有 Token 冷却中，等待 {wait_time:.1f}s")
                await asyncio.sleep(max(wait_time, 1))

    def penalize(self, token: str, duration: int = 600):
        if token in self._tokens:
            self._tokens[token] = time.time() + duration
            logger.warning(f"Token [...{token[-4:]}] 冷却 {duration}s")

class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.db_path)
        # 开启 WAL 模式提高并发写入性能
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
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_repo_ts ON commits(repo, ts_unix)")
        await self._conn.commit()

    async def save_batch(self, records: List[CommitRecord]):
        if not records: return
        try:
            # 依靠 IGNORE 自动去重，无需 Python 层面判断
            await self._conn.executemany(
                "INSERT OR IGNORE INTO commits VALUES (?,?,?,?,?,?)", 
                [r.to_db_row() for r in records]
            )
            await self._conn.commit()
        except Exception as e:
            logger.error(f"DB 写入异常: {e}")

    async def close(self):
        if self._conn: await self._conn.close()

# ======================== 核心逻辑层 ========================

class CollectionEngine:
    def __init__(self, token_pool: TokenPool, db: AsyncDatabase):
        self.token_pool = token_pool
        self.db = db
        self.data_queue = asyncio.Queue(maxsize=1000)
        self.stats = {"saved": 0}

    async def _fetch_page(self, session: aiohttp.ClientSession, variables: dict) -> Optional[dict]:
        """封装单次 API 请求与重试逻辑"""
        for attempt in range(AppConfig.MAX_RETRIES):
            token = await self.token_pool.get_token()
            headers = {"User-Agent": "GH-Col-v4", "Authorization": f"Bearer {token}"} if token else {}
            
            try:
                async with session.post(
                    AppConfig.API_URL, 
                    json={"query": AppConfig.GRAPHQL_TEMPLATE, "variables": variables}, 
                    headers=headers, 
                    timeout=AppConfig.TIMEOUT
                ) as resp:
                    if resp.status == 200:
                        res = await resp.json()
                        if "errors" in res:
                            logger.error(f"GraphQL Error: {res['errors'][0].get('message')}")
                            # 这里简单化：如果是 RateLimit 则惩罚，否则忽略本页
                            if "rate limit" in str(res).lower() and token:
                                self.token_pool.penalize(token, 300)
                                continue
                            return None
                        return res
                    
                    if resp.status in (403, 429):
                        wait = int(resp.headers.get("Retry-After", 60))
                        if token: self.token_pool.penalize(token, wait)
                    
                    await asyncio.sleep(1 * (2 ** attempt)) # 指数退避
            except Exception as e:
                logger.debug(f"Req Error: {e}")
                await asyncio.sleep(1)
        return None

    async def producer(self, repo: str, since: str, until: str, pbar: tqdm):
        """生产者：采集数据并推入队列"""
        owner, name = repo.split("/")
        vars = {"owner": owner, "name": name, "since": since, "until": until, "cursor": None}
        
        async with aiohttp.ClientSession() as session:
            while True:
                data = await self._fetch_page(session, vars)
                if not data: break
                
                # 安全导航: data -> repository -> defaultBranchRef -> target -> history
                try:
                    history = data["data"]["repository"]["defaultBranchRef"]["target"]["history"]
                except (TypeError, KeyError):
                    break # 数据结构不符合预期或无权限

                batch = []
                for edge in history.get("edges", []):
                    node = edge["node"]
                    batch.append(CommitRecord(
                        repo_name=repo,
                        commit_sha=node["oid"],
                        timestamp_unix=int(datetime.fromisoformat(node["committedDate"].replace("Z", "+00:00")).timestamp()),
                        author_login=node.get("author", {}).get("user", {}).get("login", "Unknown") or "Unknown",
                        author_name=node.get("author", {}).get("name", "Unknown"),
                        author_email=node.get("author", {}).get("email", ""),
                        message=node["messageHeadline"]
                    ))
                
                if batch:
                    await self.data_queue.put(batch)
                    pbar.update(len(batch))

                if history["pageInfo"]["hasNextPage"]:
                    vars["cursor"] = history["pageInfo"]["endCursor"]
                else:
                    break

    async def consumer(self):
        """消费者：从队列读取并批量写入 (使用哨兵模式)"""
        buffer = []
        while True:
            batch = await self.data_queue.get()
            
            # 哨兵检查: None 代表生产结束
            if batch is None:
                # 也可以在这里做最后的 flush，但通常放在循环外更安全
                self.data_queue.task_done()
                break
            
            buffer.extend(batch)
            if len(buffer) >= AppConfig.WRITE_BATCH_SIZE:
                await self.db.save_batch(buffer)
                self.stats["saved"] += len(buffer)
                buffer.clear()
            
            self.data_queue.task_done()
        
        # 循环结束，写入剩余数据
        if buffer:
            await self.db.save_batch(buffer)
            self.stats["saved"] += len(buffer)

# ======================== 主程序 ========================

async def main(repos: List[str]):
    # 1. 检查
    if not AppConfig.GITHUB_TOKENS:
        logger.warning("无 Token 模式，速率受限。建议设置 GITHUB_TOKENS 环境变量。")

    # 2. 初始化
    db = AsyncDatabase(AppConfig.DB_PATH)
    await db.connect()
    
    token_pool = TokenPool(AppConfig.GITHUB_TOKENS)
    engine = CollectionEngine(token_pool, db)
    
    # 3. 准备任务
    until_ts = datetime.now(timezone.utc)
    since_ts = until_ts - timedelta(days=30)
    
    pbar = tqdm(desc="Fetching", unit=" commits")
    
    # 启动消费者
    consumer_task = asyncio.create_task(engine.consumer())
    
    # 启动生产者 (限制并发数)
    sem = asyncio.Semaphore(AppConfig.CONCURRENT_REPOS)
    
    async def run_repo(r):
        async with sem:
            await engine.producer(r, since_ts.isoformat(), until_ts.isoformat(), pbar)

    await asyncio.gather(*[run_repo(r) for r in repos])
    
    # 4. 优雅停止
    await engine.data_queue.put(None) # 发送哨兵
    await consumer_task # 等待消费完成
    
    pbar.close()
    await db.close()
    logger.info(f"任务完成，共入库 {engine.stats['saved']} 条记录")

if __name__ == "__main__":
    target_repos = [
        "python/cpython", "torvalds/linux", "microsoft/vscode",
        "tensorflow/tensorflow", "django/django"
    ]
    
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main(target_repos))
    except KeyboardInterrupt:
        print("\n用户终止")
