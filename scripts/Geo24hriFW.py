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

# ======================== 核心逻辑层 ========================

class CollectionEngine:
    def __init__(self, token_pool: TokenPool, db: AsyncDatabase):
        self.token_pool = token_pool
        self.db = db
        # 设置有界队列，防止生产者速度过快导致内存溢出
        self.data_queue = asyncio.Queue(maxsize=1000)
        self.stats = {"saved": 0}

    async def _fetch_page(self, session: aiohttp.ClientSession, variables: dict) -> Optional[dict]:
        """封装单次 API 请求，包含重试和限流处理逻辑"""
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
                        # GraphQL 特殊性：错误可能包含在 200 响应体中
                        if "errors" in res:
                            logger.error(f"GraphQL Error: {res['errors'][0].get('message')}")
                            # 针对性处理 Rate Limit 错误，惩罚 Token
                            if "rate limit" in str(res).lower() and token:
                                self.token_pool.penalize(token, 300)
                                continue
                            return None # 其他错误跳过该页
                        return res
                    
                    # 处理 HTTP 层面的限流 (403/429)
                    if resp.status in (403, 429):
                        wait = int(resp.headers.get("Retry-After", 60))
                        if token: self.token_pool.penalize(token, wait)
                    
                    # 指数退避策略：防止在服务不稳定时发起风暴式请求
                    await asyncio.sleep(1 * (2 ** attempt)) 
            except Exception as e:
                logger.debug(f"Req Error: {e}")
                await asyncio.sleep(1)
        return None

    async def producer(self, repo: str, since: str, until: str, pbar: tqdm):
        """生产者：负责 API 翻页采集，解析数据并放入队列"""
        owner, name = repo.split("/")
        vars = {"owner": owner, "name": name, "since": since, "until": until, "cursor": None}
        
        async with aiohttp.ClientSession() as session:
            while True:
                data = await self._fetch_page(session, vars)
                if not data: break
                
                # 安全导航提取嵌套数据，避免 KeyError
                try:
                    history = data["data"]["repository"]["defaultBranchRef"]["target"]["history"]
                except (TypeError, KeyError):
                    break # 数据结构不符合预期或无权限

                batch = []
                for edge in history.get("edges", []):
                    node = edge["node"]
                    # 数据清洗与对象化
                    batch.append(CommitRecord(
                        repo_name=repo,
                        commit_sha=node["oid"],
                        timestamp_unix=int(datetime.fromisoformat(node["committedDate"].replace("Z", "+00:00")).timestamp()),
                        author_login=node.get("author", {}).get("user", {}).get("login", "Unknown") or "Unknown",
                        author_name=node.get("author", {}).get("name", "Unknown"),
                        author_email=node.get("author", {}).get("email", ""),
                        message=node["messageHeadline"]
                    ))
                
                # 放入队列等待消费者处理，如果队列满则此处会阻塞，实现背压 (Backpressure)
                if batch:
                    await self.data_queue.put(batch)
                    pbar.update(len(batch))

                # 翻页逻辑：根据 endCursor 继续请求下一页
                if history["pageInfo"]["hasNextPage"]:
                    vars["cursor"] = history["pageInfo"]["endCursor"]
                else:
                    break

    async def consumer(self):
        """消费者：从队列读取并批量写入数据库 (IO 密集型操作隔离)"""
        buffer = []
        while True:
            batch = await self.data_queue.get()
            
            # 哨兵模式 (Sentinel)：接收到 None 表示生产者全部结束，准备退出
            if batch is None:
                self.data_queue.task_done()
                break
            
            buffer.extend(batch)
            # 缓冲区机制：累积一定数量后再写入 DB，极大减少磁盘 IOPS
            if len(buffer) >= AppConfig.WRITE_BATCH_SIZE:
                await self.db.save_batch(buffer)
                self.stats["saved"] += len(buffer)
                buffer.clear()
            
            self.data_queue.task_done()
        
        # 循环结束，确保缓冲区剩余数据被写入
        if buffer:
            await self.db.save_batch(buffer)
            self.stats["saved"] += len(buffer)


# ======================== 主程序 ========================

async def main(repos: List[str]):
    # 1. 检查配置
    if not AppConfig.GITHUB_TOKENS:
        logger.warning("无 Token 模式，速率受限。建议设置 GITHUB_TOKENS 环境变量。")

    # 2. 初始化资源
    db = AsyncDatabase(AppConfig.DB_PATH)
    await db.connect()
    
    token_pool = TokenPool(AppConfig.GITHUB_TOKENS)
    engine = CollectionEngine(token_pool, db)
    
    # 3. 准备任务
    until_ts = datetime.now(timezone.utc)
    since_ts = until_ts - timedelta(days=30)
    
    pbar = tqdm(desc="Fetching", unit=" commits")
    
    # 启动消费者任务 (后台运行)
    consumer_task = asyncio.create_task(engine.consumer())
    
    # 启动生产者任务 (使用 Semaphore 限制并发仓库数)
    # 防止同时对 GitHub 发起过多连接导致封禁或内存暴涨
    sem = asyncio.Semaphore(AppConfig.CONCURRENT_REPOS)
    
    async def run_repo(r):
        async with sem:
            await engine.producer(r, since_ts.isoformat(), until_ts.isoformat(), pbar)

    # 等待所有生产者完成
    await asyncio.gather(*[run_repo(r) for r in repos])
    
    # 4. 优雅停止 (Graceful Shutdown)
    await engine.data_queue.put(None) # 发送哨兵信号通知消费者退出
    await consumer_task # 等待消费者处理完剩余数据
    
    pbar.close()
    await db.close()
    logger.info(f"任务完成，共入库 {engine.stats['saved']} 条记录")

if __name__ == "__main__":
    target_repos = [
        "python/cpython", "torvalds/linux", "microsoft/vscode",
        "tensorflow/tensorflow", "django/django"
    ]
    
    try:
        # Windows 下 asyncio 需要特定的事件循环策略
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main(target_repos))
    except KeyboardInterrupt:
        print("\n用户终止")
