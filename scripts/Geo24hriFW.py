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

# ==============================================================================
# 模块 4: [Core] 核心业务层
# 功能: 具体的采集逻辑，实现 生产者-消费者 模型
# ==============================================================================

class CollectionEngine:
    """采集引擎：协调 API 请求与数据库写入"""
    
    def __init__(self, token_pool: TokenPool, db: AsyncDatabase):
        self.token_pool = token_pool
        self.db = db
        # 有界队列：提供背压 (Backpressure) 防止内存溢出
        self.data_queue = asyncio.Queue(maxsize=1000)
        self.stats = {"saved": 0}

    async def _fetch_page(self, session: aiohttp.ClientSession, variables: dict) -> Optional[dict]:
        """封装单次 API 请求，包含重试与限流处理"""
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
                        # GraphQL 错误处理
                        if "errors" in res:
                            logger.error(f"GraphQL Error: {res['errors'][0].get('message')}")
                            # 识别 API 限流
                            if "rate limit" in str(res).lower() and token:
                                self.token_pool.penalize(token, 300)
                                continue
                            return None
                        return res
                    
                    # HTTP 限流处理 (403/429)
                    if resp.status in (403, 429):
                        wait = int(resp.headers.get("Retry-After", 60))
                        if token: self.token_pool.penalize(token, wait)
                    
                    # 指数退避
                    await asyncio.sleep(1 * (2 ** attempt)) 
            except Exception as e:
                logger.debug(f"Req Error: {e}")
                await asyncio.sleep(1)
        return None

    async def producer(self, repo: str, since: str, until: str, pbar: tqdm):
        """生产者：翻页抓取 -> 解析数据 -> 推送队列"""
        owner, name = repo.split("/")
        vars = {"owner": owner, "name": name, "since": since, "until": until, "cursor": None}
        
        async with aiohttp.ClientSession() as session:
            while True:
                data = await self._fetch_page(session, vars)
                if not data: break
                
                try:
                    history = data["data"]["repository"]["defaultBranchRef"]["target"]["history"]
                except (TypeError, KeyError):
                    break # 数据结构异常或无权限

                batch = []
                for edge in history.get("edges", []):
                    node = edge["node"]
                    # 数据转换
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
                    await self.data_queue.put(batch) # 队列满时阻塞
                    pbar.update(len(batch))

                if history["pageInfo"]["hasNextPage"]:
                    vars["cursor"] = history["pageInfo"]["endCursor"]
                else:
                    break

    async def consumer(self):
        """消费者：读取队列 -> 缓冲 -> 批量写库"""
        buffer = []
        while True:
            batch = await self.data_queue.get()
            
            # 哨兵模式：接收 None 退出
            if batch is None:
                self.data_queue.task_done()
                break
            
            buffer.extend(batch)
            # 批量写入优化 IO
            if len(buffer) >= AppConfig.WRITE_BATCH_SIZE:
                await self.db.save_batch(buffer)
                self.stats["saved"] += len(buffer)
                buffer.clear()
            
            self.data_queue.task_done()
        
        # 清理残余
        if buffer:
            await self.db.save_batch(buffer)
            self.stats["saved"] += len(buffer)

# ==============================================================================
# 模块 5: [Main] 入口模块 (专业优化版)
# 功能: 依赖检查、参数解析、任务编排与优雅的生命周期管理
# ==============================================================================

import argparse
from pathlib import Path

def parse_arguments():
    """解析命令行参数，提供更灵活的运行配置"""
    parser = argparse.ArgumentParser(description="GitHub 高性能数据采集系统 (Single File)")
    
    # 互斥组：允许直接输入仓库名，或指定文件路径
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("-r", "--repos", type=str, help="目标仓库列表，逗号分隔 (例: owner/repo1,owner/repo2)")
    group.add_argument("-f", "--file", type=str, help="仓库列表文件路径 (每行一个 owner/repo)")
    
    parser.add_argument("-d", "--days", type=int, default=30, help="采集过去多少天的数据 (默认: 30)")
    parser.add_argument("--db", type=str, default=AppConfig.DB_PATH, help=f"数据库输出路径 (默认: {AppConfig.DB_PATH})")
    
    return parser.parse_args()

def load_repositories(args) -> List[str]:
    """加载并清洗仓库列表"""
    repos = []
    
    # 1. 优先读取文件
    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            logger.error(f"文件未找到: {args.file}")
            sys.exit(1)
        with open(file_path, "r", encoding="utf-8") as f:
            repos = [line.strip() for line in f if line.strip() and not line.startswith("#")]
            
    # 2. 读取命令行列表
    elif args.repos:
        repos = [r.strip() for r in args.repos.split(",") if r.strip()]
        
    # 3. 默认回退 (用于测试)
    else:
        logger.info("未指定参数，使用默认测试仓库列表")
        repos = [
            "python/cpython", "torvalds/linux", "microsoft/vscode",
            "tensorflow/tensorflow", "django/django"
        ]
    
    logger.info(f"已加载 {len(repos)} 个目标仓库")
    return repos

async def main():
    # 1. 参数解析与配置
    args = parse_arguments()
    target_repos = load_repositories(args)
    
    if not AppConfig.GITHUB_TOKENS:
        logger.warning("警告: 未检测到 Token，运行于匿名限流模式 (极易触发 403)")

    # 2. 资源初始化 (Infrastructure)
    # 使用 args.db 允许用户自定义输出位置
    db = AsyncDatabase(args.db)
    token_pool = TokenPool(AppConfig.GITHUB_TOKENS)
    engine = CollectionEngine(token_pool, db)
    
    # 3. 计算时间窗口
    until_ts = datetime.now(timezone.utc)
    since_ts = until_ts - timedelta(days=args.days)
    logger.info(f"采集时间窗口: {since_ts.date()} -> {until_ts.date()} ({args.days} days)")

    # 进度条初始化
    pbar = tqdm(total=0, desc="Fetching", unit=" commits", dynamic_ncols=True)

    try:
        await db.connect()
        
        # 4. 启动异步任务
        # 消费者：后台运行 (fire and forget pattern, but monitored)
        consumer_task = asyncio.create_task(engine.consumer())
        
        # 生产者：创建任务列表
        sem = asyncio.Semaphore(AppConfig.CONCURRENT_REPOS)
        
        async def protected_producer(repo):
            """带信号量保护的生产者包装器"""
            async with sem:
                try:
                    await engine.producer(repo, since_ts.isoformat(), until_ts.isoformat(), pbar)
                except Exception as e:
                    logger.error(f"[{repo}] 采集失败: {e}")

        # 创建所有生产者任务
        producer_tasks = [asyncio.create_task(protected_producer(repo)) for repo in target_repos]
        
        # 等待所有生产者完成
        # return_exceptions=True 确保个别任务崩溃不影响整体流程
        await asyncio.gather(*producer_tasks, return_exceptions=True)
        
        # 5. 正常结束流程
        await engine.data_queue.put(None) # 发送哨兵信号
        await consumer_task               # 等待消费者落库完毕

    except asyncio.CancelledError:
        logger.warning("\n任务被取消，正在停止...")
    except Exception as e:
        logger.error(f"\n运行时发生未捕获异常: {e}")
    finally:
        # 6. 资源清理与优雅关闭 (Context Cleanup)
        # 确保即使在报错或 Ctrl+C 时也能保存队列中剩余的数据
        if 'consumer_task' in locals() and not consumer_task.done():
            logger.info("正在等待剩余数据写入数据库...")
            await engine.data_queue.put(None)
            try:
                # 给予消费者 10秒 宽限期将缓冲区写入磁盘
                await asyncio.wait_for(consumer_task, timeout=10.0)
            except asyncio.TimeoutError:
                logger.error("写入超时，部分数据可能丢失")
        
        pbar.close()
        await db.close()
        
        final_count = engine.stats['saved']
        duration = args.days
        logger.info(f"任务结束 | 累计入库: {final_count} 条记录 | 目标: {len(target_repos)} 仓库")

