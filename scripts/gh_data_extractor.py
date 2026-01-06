# -*- coding: utf-8 -*-
import asyncio
import csv
import json
import logging
import os
import random
import sys
import time
import sqlite3
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple, Union
from dataclasses import dataclass, asdict
from contextlib import asynccontextmanager

# 检查运行环境是否安装了必要的三方库
try:
    import aiohttp
    from tqdm.asyncio import tqdm
except ImportError:
    print("错误: 缺少必要依赖库。请运行: pip install aiohttp tqdm")
    sys.exit(1)

# ======================== 全局配置 ========================
GITHUB_API_BASE = "https://api.github.com"
DEFAULT_PER_PAGE = 100  # API 单页最大记录数
CACHE_DB_NAME = "github_data_cache.db" # 本地 SQLite 缓存文件名

@dataclass(frozen=True)
class CommitData:
    """存储 Commit 核心信息的数据类"""
    timestamp_unix: int
    raw_location: str
    contributor_id: str
    commit_sha: str
    repo_name: str
    location_iso3: str = "PENDING" # 预留字段，用于后期地理编码转换

    @classmethod
    def from_api_response(cls, commit_node: Dict, location: str, repo_name: str) -> 'CommitData':
        """将 GitHub API 返回的 JSON 节点解析为 Python 对象"""
        date_str = commit_node['commit']['author']['date']
        # 处理 ISO 时间格式，确保兼容性（将 Z 替换为标准偏移量）
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return cls(
            timestamp_unix=int(dt.timestamp()),
            raw_location=location,
            contributor_id=commit_node['author']['login'],
            commit_sha=commit_node['sha'],
            repo_name=repo_name
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典，方便写入 CSV 或 JSON"""
        return asdict(self)

# ======================== 数据库操作 ========================
class StorageEngine:
    """封装 SQLite 操作，用于缓存用户信息和记录已处理的 Commit"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        """初始化数据库表结构"""
        with self.conn:
            # 用户地理位置缓存表：避免重复查询同一个用户的 Profile
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS user_locations (
                    username TEXT PRIMARY KEY,
                    location TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 已处理 Commit 记录表：用于实现增量采集，避免重复下载
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_commits (
                    sha TEXT PRIMARY KEY,
                    repo_name TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def get_user_location(self, username: str) -> Optional[str]:
        """从本地缓存读取用户位置"""
        cursor = self.conn.execute("SELECT location FROM user_locations WHERE username = ?", (username,))
        row = cursor.fetchone()
        return row[0] if row else None

    def upsert_user_locations(self, user_data: List[Tuple[str, str]]):
        """批量更新/插入用户位置信息"""
        if not user_data: return
        with self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO user_locations (username, location) VALUES (?, ?)", 
                user_data
            )

    def filter_new_shas(self, shas: List[str]) -> Set[str]:
        """
        输入一组 SHA，返回其中尚未处理过的 SHA。
        用于在 API 分页过程中判断是否已经同步到了旧数据。
        """
        if not shas: return set()
        placeholders = ','.join(['?'] * len(shas))
        cursor = self.conn.execute(f"SELECT sha FROM processed_commits WHERE sha IN ({placeholders})", shas)
        processed = {row[0] for row in cursor.fetchall()}
        return set(shas) - processed

    def mark_processed(self, sha_repo_list: List[Tuple[str, str]]):
        """将采集完成的 Commit SHA 存入数据库"""
        if not sha_repo_list: return
        with self.conn:
            self.conn.executemany("INSERT OR IGNORE INTO processed_commits (sha, repo_name) VALUES (?, ?)", sha_repo_list)

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()

# ======================== 采集逻辑 ========================
class GitHubCollector:
    """GitHub 数据采集器，支持异步并发、速率控制和自动重试"""
    def __init__(self, token: str, db_path: str = CACHE_DB_NAME, concurrency: int = 10):
        self.token = token
        self.concurrency = concurrency
        self.storage = StorageEngine(db_path)
        self.session: Optional[aiohttp.ClientSession] = None
        # 使用信号量控制最大并发请求数，防止被 GitHub 封禁
        self.semaphore = asyncio.Semaphore(concurrency)
        
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )
        self.logger = logging.getLogger("GitHubCollector")

    async def __aenter__(self):
        """异步上下文入口，初始化 HTTP 会话"""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Geo-Collector-Project"
        }
        self.session = aiohttp.ClientSession(headers=headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文出口，清理连接资源"""
        if self.session:
            await self.session.close()
        self.storage.close()

    async def _fetch(self, url: str, params: Optional[Dict] = None) -> Any:
        """封装底层的 GET 请求，包含指数退避重试和速率限制处理"""
        for attempt in range(5):
            try:
                async with self.session.get(url, params=params, timeout=30) as resp:
                    # 处理 403 频率限制
                    if resp.status == 403:
                        reset_time = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                        wait_sec = max(reset_time - time.time(), 5) + 2
                        self.logger.warning(f"触发 API 速率限制，休眠 {wait_sec:.0f}秒后重试...")
                        await asyncio.sleep(wait_sec)
                        continue
                    
                    if resp.status == 404: return None
                    resp.raise_for_status()
                    return await resp.json()
            except Exception as e:
                if attempt == 4:
                    self.logger.error(f"请求失败 {url}: {e}")
                    return None
                # 指数退避：2, 4, 8, 16 秒加随机抖动
                await asyncio.sleep(2 ** attempt + random.random())
        return None

    async def get_user_location(self, username: str) -> str:
        """获取用户的地理位置，优先读取本地缓存，否则请求 API"""
        if not username: return ""
        
        cached = self.storage.get_user_location(username)
        if cached is not None: return cached

        async with self.semaphore:
            data = await self._fetch(f"{GITHUB_API_BASE}/users/{username}")
            # 清洗字符串，去除换行符
            loc = (data.get("location") or "").strip().replace("\n", " ") if data else ""
            self.storage.upsert_user_locations([(username, loc)])
            return loc

    async def collect_repo(self, repo_path: str, days: int) -> List[CommitData]:
        """采集指定仓库在过去 X 天内的增量提交记录"""
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        url = f"{GITHUB_API_BASE}/repos/{repo_path}/commits"
        all_commits = []
        page = 1

        self.logger.info(f"开始同步: {repo_path} (最近 {days} 天)")

        while True:
            params = {"per_page": DEFAULT_PER_PAGE, "page": page, "since": since}
            batch = await self._fetch(url, params)
            if not batch: break

            # 提取当前页的所有 SHA，并对比数据库过滤掉已采集过的
            shas_in_batch = [c['sha'] for c in batch]
            new_shas = self.storage.filter_new_shas(shas_in_batch)
            
            # 如果这一页所有提交都在数据库里，说明遇到了上次采集的断点，可以提前结束
            if not new_shas:
                self.logger.info(f"[{repo_path}] 遇到已同步数据，停止扫描")
                break

            # 过滤掉没有作者信息的提交（如 Web 端的合并操作）
            active_commits = [c for c in batch if c['sha'] in new_shas and c.get('author')]
            
            # 批量并行获取这些 Commit 作者的 Location 信息
            unique_logins = {c['author']['login'] for c in active_commits}
            user_loc_tasks = {login: self.get_user_location(login) for login in unique_logins}
            user_map = dict(zip(user_loc_tasks.keys(), await asyncio.gather(*user_loc_tasks.values())))

            # 组装最终结果
            current_batch_data = []
            db_marks = []
            for c in active_commits:
                login = c['author']['login']
                loc = user_map.get(login, "")
                cd = CommitData.from_api_response(c, loc, repo_path)
                current_batch_data.append(cd)
                db_marks.append((c['sha'], repo_path))

            all_commits.extend(current_batch_data)
            # 及时更新本地数据库状态
            self.storage.mark_processed(db_marks)

            # 如果当前页不满，说明已经没有更多数据了
            if len(batch) < DEFAULT_PER_PAGE: break
            page += 1

        self.logger.info(f"[{repo_path}] 采集完成，新增 {len(all_commits)} 条记录")
        return all_commits

# ======================== 入口函数 ========================
async def main_async(args):
    # 优先从命令行参数取 Token，其次取环境变量
    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not token:
        print("错误: 缺少 GitHub Token。请通过 -t 参数或环境变量 GITHUB_TOKEN 设置。")
        return

    async with GitHubCollector(token, concurrency=args.concurrency) as collector:
        # 启动多个仓库的并发采集任务
        tasks = [collector.collect_repo(repo, args.days) for repo in args.projects]
        results = await asyncio.gather(*tasks)
        
        # 将嵌套的列表结果拍平
        flattened_results = [cd.to_dict() for repo_res in results for cd in repo_res]
        
        if not flattened_results:
            print("\n未发现符合条件的增量数据。")
            return

        # 确保输出目录存在
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # 根据后缀名自动选择保存格式
        if out_path.suffix.lower() == '.csv':
            with open(out_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=list(flattened_results[0].keys()))
                writer.writeheader()
                writer.writerows(flattened_results)
        else:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(flattened_results, f, ensure_ascii=False, indent=2)

        print(f"\n采集成功! 数据已存至: {out_path} (共 {len(flattened_results)} 条记录)")

def main():
    parser = argparse.ArgumentParser(description="GitHub 开发者地理分布数据采集工具")
    parser.add_argument("--projects", "-p", nargs='+', required=True, help="项目列表，格式：owner/repo")
    parser.add_argument("--token", "-t", help="GitHub 访问令牌")
    parser.add_argument("--days", "-d", type=int, default=30, help="向前追溯的天数")
    parser.add_argument("--output", "-o", default="github_geo_data.json", help="输出路径 (.json 或 .csv)")
    parser.add_argument("--concurrency", "-c", type=int, default=10, help="API 请求并发数")

    args = parser.parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\n用户手动停止。")

if __name__ == "__main__":
    main()
