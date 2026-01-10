"""
GitHub 高性能数据采集系统
-----------------------------------------------------------
前置要求:
pip install aiohttp aiosqlite tqdm
"""
import asyncio
import json
import logging
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Set, Tuple

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

# ======================== 核心异常体系 ========================

class GitHubCollectorError(Exception): pass
class AuthError(GitHubCollectorError): pass

# ======================== 数据模型与配置 ========================

@dataclass(frozen=True)
class CommitRecord:
    repo_name: str
    commit_sha: str
    timestamp_unix: int
    author_login: str
    author_name: str
    author_email: str
    message: str
    # 辅助字段
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_db_row(self) -> Tuple:
        return (
            self.commit_sha,
            self.repo_name,
            self.author_login,
            self.timestamp_unix,
            self.message, # 新增 message 字段对应 SQL
            json.dumps(asdict(self))
        )

class AppConfig:
    API_URL = "https://api.github.com/graphql"
    DB_PATH = "gh_enterprise_v3_fixed.db"
    
    # 这里的 Token 需要替换为真实 Token
    # 格式: ["ghp_xxxx", "ghp_yyyy"]
    GITHUB_TOKENS = os.getenv("GITHUB_TOKENS", "").split(",") 

    CONCURRENT_REPOS = 5
    PAGE_SIZE = 100
    MAX_RETRIES = 5
    TIMEOUT = aiohttp.ClientTimeout(total=60, connect=10)
    WRITE_BATCH_SIZE = 50 # 调小一点方便观察写入

    # 完整的 GraphQL 查询模板
    GRAPHQL_TEMPLATE = """
    query($owner: String!, $name: String!, $since: GitTimestamp!, $until: GitTimestamp!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        defaultBranchRef {
          target {
            ... on Commit {
              history(since: $since, until: $until, first: %d, after: $cursor) {
                pageInfo {
                  hasNextPage
                  endCursor
                }
                edges {
                  node {
                    oid
                    messageHeadline
                    committedDate
                    author {
                      name
                      email
                      user {
                        login
                      }
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

import os # 补充导入

# ======================== 基础设施层 ========================

class TokenPool:
    def __init__(self, tokens: List[str]):
        # 过滤空 Token
        self._tokens = {t.strip(): 0.0 for t in tokens if t.strip()}
        if not self._tokens:
            logging.warning("警告: 未检测到 Token，将尝试匿名访问(极易限流)或请在环境变量设置 GITHUB_TOKENS")
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("TokenPool")

    async def get_best_token(self) -> Optional[str]:
        if not self._tokens:
            return None # 无 Token 模式

        async with self._lock:
            while True:
                now = time.time()
                available = [t for t, cooldown in self._tokens.items() if now >= cooldown]
                if available:
                    token = available[0]
                    # Round-Robin: 移出并重新插入到末尾
                    self._tokens.pop(token)
                    self._tokens[token] = 0.0
                    return token
                
                wait_time = min(self._tokens.values()) - now + 0.5
                self._logger.warning(f"所有 Token 已限流，挂起 {wait_time:.1f}s")
                await asyncio.sleep(max(wait_time, 1))

    def penalize(self, token: str, duration: int = 600):
        if not token: return
        self._tokens[token] = time.time() + duration
        self._logger.warning(f"Token [...{token[-4:]}] 冷却 {duration}s")

class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        
        # 修复建表语句，确保列数和 insert 匹配
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

    async def get_known_shas(self, repo: str) -> Set[str]:
        async with self._conn.execute("SELECT sha FROM commits WHERE repo = ?", (repo,)) as cursor:
            rows = await cursor.fetchall()
            return {row[0] for row in rows}

    async def save_batch(self, records: List[CommitRecord]):
        if not records: return
        data = [r.to_db_row() for r in records]
        # 修复占位符数量：6个字段 = 6个问号
        await self._conn.executemany("INSERT OR IGNORE INTO commits VALUES (?,?,?,?,?,?)", data)
        await self._conn.commit()

    async def close(self):
        if self._conn: await self._conn.close()

# ======================== 核心逻辑层 ========================

class CollectionEngine:
    def __init__(self, token_pool: TokenPool, db: AsyncDatabase):
        self.token_pool = token_pool
        self.db = db
        self.data_queue = asyncio.Queue(maxsize=1000)
        self.is_running = True
        self.stats = {"total_saved": 0}
        self._logger = logging.getLogger("Engine")

    async def _api_request(self, session: aiohttp.ClientSession, variables: dict) -> Optional[dict]:
        for attempt in range(AppConfig.MAX_RETRIES):
            if not self.is_running: return None
            
            token = await self.token_pool.get_best_token()
            headers = {"User-Agent": "GH-Collector-v3"}
            if token:
                headers["Authorization"] = f"bearer {token}"
            
            payload = {"query": AppConfig.GRAPHQL_TEMPLATE, "variables": variables}

            try:
                async with session.post(AppConfig.API_URL, json=payload, headers=headers, timeout=AppConfig.TIMEOUT) as resp:
                    if resp.status == 200:
                        res_json = await resp.json()
                        if "errors" in res_json:
                             # 简单处理 GraphQL 错误
                            err_msg = str(res_json["errors"])
                            if "rate limit" in err_msg.lower():
                                if token: self.token_pool.penalize(token, 300)
                                continue
                            if "Could not resolve to a Repository" in err_msg:
                                self._logger.error(f"仓库不存在: {variables['owner']}/{variables['name']}")
                                return None
                            # 其他错误打印并返回
                            self._logger.error(f"GraphQL Error: {err_msg}")
                            return None
                        return res_json
                    
                    if resp.status in (403, 429):
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        if token: self.token_pool.penalize(token, retry_after)
                        await asyncio.sleep(retry_after)
                        continue
                    
                    self._logger.warning(f"HTTP {resp.status} - 重试中...")
                    await asyncio.sleep(2 ** attempt)

            except Exception as e:
                self._logger.error(f"网络异常: {e}")
                await asyncio.sleep(2 ** attempt)
        return None

    async def repository_worker(self, repo_full_name: str, since: datetime, until: datetime, pbar: tqdm):
        # 1. 变量解析
        try:
            owner, name = repo_full_name.split("/")
        except ValueError:
            self._logger.error(f"仓库名格式错误: {repo_full_name}")
            return

        known_shas = await self.db.get_known_shas(repo_full_name)
        variables = {"owner": owner, "name": name, "since": since.isoformat(), "until": until.isoformat(), "cursor": None}

        async with aiohttp.ClientSession() as session:
            while self.is_running:
                data = await self._api_request(session, variables)
                if not data: break
                
                # 安全获取嵌套数据
                repo_data = data.get("data", {}).get("repository")
                if not repo_data: break # 可能无权限或仓库为空

                default_branch = repo_data.get("defaultBranchRef")
                if not default_branch: break # 空仓库

                history = default_branch["target"]["history"]
                edges = history.get("edges", [])
                
                batch = []
                for edge in edges:
                    node = edge["node"]
                    sha = node["oid"]
                    if sha in known_shas: continue
                    
                    author = node.get("author") or {}
                    user = author.get("user") or {}

                    record = CommitRecord(
                        repo_name=repo_full_name,
                        commit_sha=sha,
                        timestamp_unix=int(datetime.fromisoformat(node["committedDate"].replace("Z", "+00:00")).timestamp()),
                        author_login=user.get("login", "Unknown"),
                        author_name=author.get("name", "Unknown"),
                        author_email=author.get("email", ""),
                        message=node["messageHeadline"]
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

    async def storage_worker(self):
        buffer = []
        while self.is_running or not self.data_queue.empty():
            try:
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
            except Exception as e:
                self._logger.error(f"存储线程异常: {e}")

# ======================== 任务管理 ========================

class Application:
    def __init__(self, repos: List[str]):
        self.repos = repos
        # 尝试从环境变量读取 Token
        tokens = AppConfig.GITHUB_TOKENS
        self.token_pool = TokenPool(tokens)
        self.db = AsyncDatabase(AppConfig.DB_PATH)
        self.engine = CollectionEngine(self.token_pool, self.db)

    async def run(self):
        print(f"初始化数据库: {AppConfig.DB_PATH}")
        await self.db.connect()
        
        loop = asyncio.get_running_loop()
        # Windows 下 signal 支持有限，此处做简单兼容
        if sys.platform != 'win32':
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: setattr(self.engine, 'is_running', False))

        storage_task = asyncio.create_task(self.engine.storage_worker())
        semaphore = asyncio.Semaphore(AppConfig.CONCURRENT_REPOS)
        
        # 定义采集时间范围 (最近 30 天)
        until = datetime.now(timezone.utc)
        since = until - timedelta(days=30)

        async def sem_worker(repo, pbar):
            async with semaphore:
                await self.engine.repository_worker(repo, since, until, pbar)

        print(f"开始采集 {len(self.repos)} 个仓库...")
        # 进度条
        pbar = tqdm(total=0, desc="采集 Commit", unit="条") # total=0 因为不知道总数，动态更新
        
        tasks = [sem_worker(repo, pbar) for repo in self.repos]
        await asyncio.gather(*tasks)
        
        # 结束处理
        self.engine.is_running = False
        print("\n正在等待数据写入完成...")
        await storage_task
        await self.db.close()
        pbar.close()
        print(f"采集完成! 共存储 {self.engine.stats['total_saved']} 条记录。")

# ======================== 入口 ========================

if __name__ == "__main__":
    # 示例仓库列表
    target_repos = [
        "python/cpython",
        "torvalds/linux", 
        "microsoft/vscode",
        "tensorflow/tensorflow",
        "django/django"
    ]
    
    # 检查是否有 Token，没有的话提醒用户
    if not any(t.strip() for t in AppConfig.GITHUB_TOKENS):
         print("-" * 60)
         print("警告: 未设置 GITHUB_TOKENS 环境变量。")
         print("公共 API 速率限制非常低 (60次/小时)，程序可能会立即报错或挂起。")
         print("建议: export GITHUB_TOKENS='ghp_xxx,ghp_yyy'")
         print("-" * 60)
         # 稍微等待让用户看到警告
         time.sleep(2)

    app = Application(target_repos)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        print("\n用户强制停止")
