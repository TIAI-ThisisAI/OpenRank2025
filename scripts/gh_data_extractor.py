# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import os
import sys
import time
import random
import argparse
import sqlite3
import csv
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 第三方库依赖
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

@dataclass
class CommitData:
    """标准化的提交数据结构"""
    timestamp_unix: int
    raw_location: str
    contributor_id: str
    commit_sha: str
    repo_name: str
    location_iso3: str = "PENDING"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# ======================== 存储引擎 (SQLite) ========================
class StorageEngine:
    """使用 SQLite 存储用户地理位置和处理记录，支持断点续传"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            # 用户位置缓存表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_locations (
                    username TEXT PRIMARY KEY,
                    location TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 记录已处理的 Repo 和 Sha，防止重复采集
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_commits (
                    sha TEXT PRIMARY KEY,
                    repo_name TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def get_user_location(self, username: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT location FROM user_locations WHERE username = ?", (username,))
            row = cursor.fetchone()
            return row[0] if row else None

    def upsert_user_location(self, username: str, location: Optional[str]):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO user_locations (username, location) VALUES (?, ?)",
                (username, location or "")
            )
            conn.commit()

    def is_commit_processed(self, sha: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT 1 FROM processed_commits WHERE sha = ?", (sha,))
            return cursor.fetchone() is not None

    def mark_commits_processed(self, sha_repo_list: List[Tuple[str, str]]):
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany("INSERT OR IGNORE INTO processed_commits (sha, repo_name) VALUES (?, ?)", sha_repo_list)
            conn.commit()

# ======================== 核心采集器 ========================
class GitHubCollectorPro:
    def __init__(self, token: str, db_path: str = CACHE_DB_NAME, concurrency: int = 10):
        self.token = token
        self.concurrency = concurrency
        self.storage = StorageEngine(db_path)
        self.session: Optional[aiohttp.ClientSession] = None
        
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger("Collector")

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Geo-Collector-Pro/3.0"
        }

    async def _request(self, url: str, params: Dict = None) -> Any:
        """核心请求逻辑，包含频率限制嗅探"""
        if not self.session:
            self.session = aiohttp.ClientSession(headers=self._get_headers())
        
        for attempt in range(5):
            try:
                async with self.session.get(url, params=params, timeout=30) as resp:
                    # 处理频率限制
                    if resp.status == 403:
                        reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                        wait_time = max(reset - time.time(), 5) + 1
                        self.logger.warning(f"触发频率限制，需等待 {wait_time:.0f}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    if resp.status == 404: return None
                    resp.raise_for_status()
                    return await resp.json()
            except Exception as e:
                if attempt == 4: self.logger.error(f"请求失败 {url}: {e}")
                await asyncio.sleep(2 ** attempt + random.random())
        return None

    async def get_user_location(self, username: str) -> str:
        """获取用户位置（带二级缓存：DB + Memory）"""
        if not username: return ""
        
        # 1. 检查数据库缓存
        cached = self.storage.get_user_location(username)
        if cached is not None: return cached

        # 2. 爬取 API
        data = await self._request(f"{GITHUB_API_BASE}/users/{username}")
        loc = (data.get("location") or "").strip().replace("\n", " ") if data else ""
        
        # 3. 异步存入数据库
        self.storage.upsert_user_location(username, loc)
        return loc

    async def collect_repo(self, repo_path: str, days: int) -> List[CommitData]:
        """采集单个仓库的增量数据"""
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        url = f"{GITHUB_API_BASE}/repos/{repo_path}/commits"
        
        self.logger.info(f"正在同步项目: {repo_path}")
        all_results = []
        page = 1
        
        while True:
            params = {"per_page": DEFAULT_PER_PAGE, "page": page, "since": since}
            batch = await self._request(url, params)
            if not batch: break

            # 过滤掉已处理的 Commit
            new_commits = [c for c in batch if not self.storage.is_commit_processed(c['sha'])]
            if not new_commits and len(batch) > 0:
                self.logger.info(f"[{repo_path}] 到达已处理区域，停止翻页")
                break
            
            # 提取贡献者
            unique_users = {c['author']['login'] for c in new_commits if c.get('author')}
            user_map = {}
            
            # 并发获取用户信息
            semaphore = asyncio.Semaphore(self.concurrency)
            async def _get_loc(u):
                async with semaphore:
                    user_map[u] = await self.get_user_location(u)

            if unique_users:
                await asyncio.gather(*[_get_loc(u) for u in unique_users])

            # 封装数据
            current_batch_data = []
            storage_batch = []
            for c in new_commits:
                login = c['author']['login'] if c.get('author') else None
                if not login: continue
                
                loc = user_map.get(login, "")
                cd = CommitData(
                    timestamp_unix=int(datetime.fromisoformat(c['commit']['author']['date'].replace("Z", "+00:00")).timestamp()),
                    raw_location=loc,
                    contributor_id=login,
                    commit_sha=c['sha'],
                    repo_name=repo_path
                )
                current_batch_data.append(cd)
                storage_batch.append((c['sha'], repo_path))

            all_results.extend(current_batch_data)
            self.storage.mark_commits_processed(storage_batch)
            
            if len(batch) < DEFAULT_PER_PAGE: break
            page += 1
            
        self.logger.info(f"[{repo_path}] 新采集记录: {len(all_results)}")
        return all_results

    async def close(self):
        if self.session:
            await self.session.close()

# ======================== 主程序 ========================
async def run_pipeline(args):
    collector = GitHubCollectorPro(
        token=args.token or os.environ.get("GITHUB_TOKEN", ""),
        concurrency=args.concurrency
    )
    
    final_output = []
    try:
        for repo in args.projects:
            data = await collector.collect_repo(repo, args.days)
            final_output.extend([d.to_dict() for d in data])
            
        if final_output:
            out_path = Path(args.output)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            
            if out_path.suffix == '.csv':
                with open(out_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=CommitData.__dataclass_fields__.keys())
                    writer.writeheader()
                    writer.writerows(final_output)
            else:
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(final_output, f, ensure_ascii=False, indent=2)
            
            print(f"\n采集完成! 结果保存在: {out_path} (共 {len(final_output)} 条新数据)")
        else:
            print("\n没有发现新提交。")
            
    finally:
        await collector.close()

def main():
    parser = argparse.ArgumentParser(description="GitHub 开源项目地理数据采集器 Pro", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--projects", "-p", nargs='+', required=True, help="项目列表 (owner/repo)")
    parser.add_argument("--token", "-t", help="GitHub PAT (建议设置环境变量 GITHUB_TOKEN)")
    parser.add_argument("--days", "-d", type=int, default=30, help="回溯时间范围(天)")
    parser.add_argument("--output", "-o", default="raw_github_data.json", help="输出路径 (.json 或 .csv)")
    parser.add_argument("--concurrency", "-c", type=int, default=15, help="并行采集数")
    
    args = parser.parse_args()
    if not (args.token or os.environ.get("GITHUB_TOKEN")):
        print("错误: 请通过 --token 或环境变量 GITHUB_TOKEN 提供访问令牌。")
        return

    asyncio.run(run_pipeline(args))

if __name__ == "__main__":
    main()
