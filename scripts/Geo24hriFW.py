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
