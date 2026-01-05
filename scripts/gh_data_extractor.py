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

# 依赖检查
try:
    import aiohttp
    from tqdm.asyncio import tqdm
except ImportError:
    print("错误: 缺少必要依赖库。请运行: pip install aiohttp tqdm")
    sys.exit(1)

# ======================== 配置与常量 ========================
GITHUB_API_BASE = "https://api.github.com"
DEFAULT_PER_PAGE = 100
CACHE_DB_NAME = "github_data_cache.db"

@dataclass(frozen=True)
class CommitData:
    """标准化的提交数据结构"""
    timestamp_unix: int
    raw_location: str
    contributor_id: str
    commit_sha: str
    repo_name: str
    location_iso3: str = "PENDING"

    @classmethod
    def from_api_response(cls, commit_node: Dict, location: str, repo_name: str) -> 'CommitData':
        """从 GitHub API 响应解析数据"""
        date_str = commit_node['commit']['author']['date']
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return cls(
            timestamp_unix=int(dt.timestamp()),
            raw_location=location,
            contributor_id=commit_node['author']['login'],
            commit_sha=commit_node['sha'],
            repo_name=repo_name
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# ======================== 存储引擎 ========================
class StorageEngine:
    """持久化层，管理 SQLite 连接与缓存"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS user_locations (
                    username TEXT PRIMARY KEY,
                    location TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_commits (
                    sha TEXT PRIMARY KEY,
                    repo_name TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def get_user_location(self, username: str) -> Optional[str]:
        cursor = self.conn.execute("SELECT location FROM user_locations WHERE username = ?", (username,))
        row = cursor.fetchone()
        return row[0] if row else None

    def upsert_user_locations(self, user_data: List[Tuple[str, str]]):
        if not user_data: return
        with self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO user_locations (username, location) VALUES (?, ?)", 
                user_data
            )

    def filter_new_shas(self, shas: List[str]) -> Set[str]:
        """批量检查哪些 SHA 是未处理过的"""
        if not shas: return set()
        placeholders = ','.join(['?'] * len(shas))
        cursor = self.conn.execute(f"SELECT sha FROM processed_commits WHERE sha IN ({placeholders})", shas)
        processed = {row[0] for row in cursor.fetchall()}
        return set(shas) - processed

    def mark_processed(self, sha_repo_list: List[Tuple[str, str]]):
        if not sha_repo_list: return
        with self.conn:
            self.conn.executemany("INSERT OR IGNORE INTO processed_commits (sha, repo_name) VALUES (?, ?)", sha_repo_list)

    def close(self):
        if self.conn:
            self.conn.close()

# ======================== 核心采集器 ========================
class GitHubCollector:
    def __init__(self, token: str, db_path: str = CACHE_DB_NAME, concurrency: int = 10):
        self.token = token
        self.concurrency = concurrency
        self.storage = StorageEngine(db_path)
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(concurrency)
        
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )
        self.logger = logging.getLogger("GitHubCollector")

    async def __aenter__(self):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Geo-Collector-Pro/4.0"
        }
        self.session = aiohttp.ClientSession(headers=headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        self.storage.close()

    async def _fetch(self, url: str, params: Optional[Dict] = None) -> Any:
        """带退避机制的请求核心"""
        for attempt in range(5):
            try:
                async with self.session.get(url, params=params, timeout=30) as resp:
                    if resp.status == 403:
                        reset_time = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                        wait_sec = max(reset_time - time.time(), 5) + 2
                        self.logger.warning(f"频率限制触发，休眠 {wait_sec:.0f}s...")
                        await asyncio.sleep(wait_sec)
                        continue
                    
                    if resp.status == 404: return None
                    resp.raise_for_status()
                    return await resp.json()
            except Exception as e:
                if attempt == 4:
                    self.logger.error(f"无法获取数据 {url}: {e}")
                    return None
                await asyncio.sleep(2 ** attempt + random.random())
        return None

    async def get_user_location(self, username: str) -> str:
        """获取单个用户位置，带二级缓存"""
        if not username: return ""
        
        cached = self.storage.get_user_location(username)
        if cached is not None: return cached

        async with self.semaphore:
            data = await self._fetch(f"{GITHUB_API_BASE}/users/{username}")
            loc = (data.get("location") or "").strip().replace("\n", " ") if data else ""
            self.storage.upsert_user_locations([(username, loc)])
            return loc

    async def collect_repo(self, repo_path: str, days: int) -> List[CommitData]:
        """增量采集仓库提交数据"""
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        url = f"{GITHUB_API_BASE}/repos/{repo_path}/commits"
        all_commits = []
        page = 1

        self.logger.info(f"开始同步: {repo_path} (最近 {days} 天)")

        while True:
            params = {"per_page": DEFAULT_PER_PAGE, "page": page, "since": since}
            batch = await self._fetch(url, params)
            if not batch: break

            # 批量过滤已存在的 SHA
            shas_in_batch = [c['sha'] for c in batch]
            new_shas = self.storage.filter_new_shas(shas_in_batch)
            
            if not new_shas:
                self.logger.info(f"[{repo_path}] 该页所有提交已处理过，停止扫描")
                break

            # 仅处理新的提交
            active_commits = [c for c in batch if c['sha'] in new_shas and c.get('author')]
            
            # 提取并并发查询用户信息
            unique_logins = {c['author']['login'] for c in active_commits}
            user_loc_tasks = {login: self.get_user_location(login) for login in unique_logins}
            user_map = dict(zip(user_loc_tasks.keys(), await asyncio.gather(*user_loc_tasks.values())))

            # 构建数据对象
            current_batch_data = []
            db_marks = []
            for c in active_commits:
                login = c['author']['login']
                loc = user_map.get(login, "")
                cd = CommitData.from_api_response(c, loc, repo_path)
                current_batch_data.append(cd)
                db_marks.append((c['sha'], repo_path))

            all_commits.extend(current_batch_data)
            self.storage.mark_processed(db_marks)

            if len(batch) < DEFAULT_PER_PAGE: break
            page += 1

        self.logger.info(f"[{repo_path}] 采集完成，新增 {len(all_commits)} 条记录")
        return all_commits

# ======================== 主逻辑 ========================
async def main_async(args):
    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not token:
        print("错误: 缺少 GitHub Token。请通过 -t 或环境变量 GITHUB_TOKEN 设置。")
        return

    async with GitHubCollector(token, concurrency=args.concurrency) as collector:
        tasks = [collector.collect_repo(repo, args.days) for repo in args.projects]
        results = await asyncio.gather(*tasks)
        
        # 展平结果列表
        flattened_results = [cd.to_dict() for repo_res in results for cd in repo_res]
        
        if not flattened_results:
            print("\n未发现符合条件的新数据。")
            return

        # 保存结果
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if out_path.suffix.lower() == '.csv':
            with open(out_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=list(flattened_results[0].keys()))
                writer.writeheader()
                writer.writerows(flattened_results)
        else:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(flattened_results, f, ensure_ascii=False, indent=2)

        print(f"\n任务成功! 结果已写入: {out_path} (共 {len(flattened_results)} 条记录)")

def main():
    parser = argparse.ArgumentParser(description="GitHub 开源项目地理数据采集器 Pro (Optimized)")
    parser.add_argument("--projects", "-p", nargs='+', required=True, help="仓库路径列表 (例如: owner/repo)")
    parser.add_argument("--token", "-t", help="GitHub Token")
    parser.add_argument("--days", "-d", type=int, default=30, help="回溯天数")
    parser.add_argument("--output", "-o", default="github_geo_data.json", help="输出文件 (.json 或 .csv)")
    parser.add_argument("--concurrency", "-c", type=int, default=10, help="并发请求数")

    args = parser.parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\n用户中止采集。")

if __name__ == "__main__":
    main()
