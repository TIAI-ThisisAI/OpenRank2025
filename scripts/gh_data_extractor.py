# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import math
import os
import sys
import time
import random
import argparse
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass, asdict
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
CACHE_FILE_NAME = "github_user_cache.json"

@dataclass
class CommitData:
    """标准化的提交数据结构"""
    timestamp_unix: int
    raw_location: str
    contributor_id: str
    commit_sha: str
    repo_name: str
    # 预留字段，供下游 location_optimizer.py 填充
    location_iso3: str = "NEED_CLEANING"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class DiskCache:
    """持久化缓存，用于存储不变的数据（如用户位置），节省API配额"""
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.data: Dict[str, str] = {}
        self.loaded = False

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    self.data = json.load(f)
            except Exception as e:
                logging.warning(f"加载缓存失败: {e}")
        self.loaded = True

    def save(self):
        try:
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"保存缓存失败: {e}")

    def get(self, key: str) -> Optional[str]:
        return self.data.get(key)

    def set(self, key: str, value: str):
        self.data[key] = value

class GitHubCollector:
    def __init__(self, token: str, cache_path: str = CACHE_FILE_NAME, concurrency: int = 10):
        self.token = token
        self.concurrency = concurrency
        self.cache = DiskCache(cache_path)
        self.cache.load()
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger("GitHubCollector")

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Commit-Collector-Async/2.0"
        }

    async def _fetch(self, session: aiohttp.ClientSession, url: str, params: Dict = None) -> Any:
        """通用请求函数，处理速率限制和重试"""
        retry_delay = 1.0
        max_retries = 5
        
        for attempt in range(max_retries):
            try:
                async with session.get(url, headers=self._get_headers(), params=params) as response:
                    # 处理速率限制
                    if response.status == 403:
                        rate_limit_remaining = response.headers.get('X-RateLimit-Remaining')
                        if rate_limit_remaining == '0':
                            reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                            wait_time = max(reset_time - time.time(), 1) + 1
                            self.logger.warning(f"触发API速率限制，暂停 {wait_time:.0f} 秒...")
                            await asyncio.sleep(wait_time)
                            continue

                    if response.status != 200:
                        # 404 表示用户或仓库不存在，无需重试
                        if response.status == 404:
                            return None
                        response.raise_for_status()

                    return await response.json()
            
            except Exception as e:
                if attempt < max_retries - 1:
                    sleep_time = retry_delay * (2 ** attempt) + random.uniform(0, 1)
                    await asyncio.sleep(sleep_time)
                else:
                    self.logger.error(f"请求失败 {url}: {e}")
                    return None
        return None

    async def fetch_user_location(self, session: aiohttp.ClientSession, username: str) -> str:
        """获取用户位置（优先查缓存）"""
        if not username:
            return ""
            
        # 1. 查缓存
        cached_loc = self.cache.get(username)
        if cached_loc is not None:
            return cached_loc

        # 2. 查API
        url = f"{GITHUB_API_BASE}/users/{username}"
        user_data = await self._fetch(session, url)
        
        location = ""
        if user_data:
            location = user_data.get("location") or ""
            # 清理换行符等
            location = location.replace("\n", " ").strip()
        
        # 3. 写入缓存（即使为空也缓存，避免重复查询无效用户）
        self.cache.set(username, location)
        return location

    async def collect_repo_commits(self, repo_full_name: str, days: int) -> List[CommitData]:
        """收集单个仓库的提交数据"""
        self.logger.info(f"开始处理项目: {repo_full_name}")
        
        since_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        url = f"{GITHUB_API_BASE}/repos/{repo_full_name}/commits"
        
        all_commits_raw = []
        page = 1
        
        async with aiohttp.ClientSession() as session:
            # 1. 获取所有提交列表 (分页)
            while True:
                params = {"per_page": DEFAULT_PER_PAGE, "page": page, "since": since_date}
                self.logger.info(f"[{repo_full_name}] 获取提交列表第 {page} 页...")
                
                batch = await self._fetch(session, url, params)
                if not batch:
                    break
                
                all_commits_raw.extend(batch)
                if len(batch) < DEFAULT_PER_PAGE:
                    break
                page += 1

            self.logger.info(f"[{repo_full_name}] 共获取 {len(all_commits_raw)} 条提交记录，开始解析用户...")

            # 2. 提取唯一作者并获取位置
            unique_authors = set()
            commits_with_author = []

            for commit in all_commits_raw:
                author = commit.get("author") # GitHub账号信息
                if author and "login" in author:
                    login = author["login"]
                    unique_authors.add(login)
                    commits_with_author.append({
                        "login": login,
                        "sha": commit["sha"],
                        "date": commit["commit"]["author"]["date"]
                    })

            # 3. 并发获取用户位置
            semaphore = asyncio.Semaphore(self.concurrency)
            user_location_map = {}
            
            async def _bounded_fetch_user(username):
                async with semaphore:
                    loc = await self.fetch_user_location(session, username)
                    user_location_map[username] = loc

            # 使用 tqdm 显示用户解析进度
            tasks = [_bounded_fetch_user(u) for u in unique_authors]
            if tasks:
                for f in tqdm.as_completed(tasks, desc=f"解析 {repo_full_name} 贡献者", total=len(tasks)):
                    await f
            
            # 4. 组装最终数据
            results = []
            for item in commits_with_author:
                ts_str = item["date"].replace("Z", "+00:00")
                try:
                    ts = int(datetime.fromisoformat(ts_str).timestamp())
                except:
                    ts = 0
                
                loc = user_location_map.get(item["login"], "")
                
                # 仅保留有位置信息的记录（可选策略）
                if loc:
                    results.append(CommitData(
                        timestamp_unix=ts,
                        raw_location=loc,
                        contributor_id=item["login"],
                        commit_sha=item["sha"],
                        repo_name=repo_full_name
                    ))

            # 每一轮大任务结束保存一次缓存
            self.cache.save()
            return results

    async def run_batch(self, repos: List[str], days: int) -> Dict[str, List[Dict]]:
        final_data = {}
        for repo in repos:
            try:
                commits = await self.collect_repo_commits(repo, days)
                final_data[repo] = [c.to_dict() for c in commits]
                self.logger.info(f"[{repo}] 处理完成，有效含位置提交数: {len(commits)}")
            except Exception as e:
                self.logger.error(f"[{repo}] 处理出错: {e}")
        return final_data

# ======================== 模拟数据生成器 ========================
def generate_mock_data(repos: List[str], count: int = 200) -> Dict[str, List[Dict]]:
    """生成用于测试的模拟数据，无需联网"""
    print(f"--- 正在生成模拟数据 ({count}条/库) ---")
    data = {}
    locations = [
        "Beijing, China", "San Francisco, CA", "Remote", "Earth", 
        "Tokyo, JP", "Berlin", "New York", "", "Not specified"
    ]
    users = [f"user_{i}" for i in range(20)]
    
    for repo in repos:
        repo_commits = []
        for i in range(count):
            repo_commits.append({
                "timestamp_unix": int(time.time()) - random.randint(0, 86400*90),
                "raw_location": random.choice(locations),
                "contributor_id": random.choice(users),
                "commit_sha": f"mock_sha_{random.randint(1000,9999)}",
                "repo_name": repo,
                "location_iso3": "NEED_CLEANING"
            })
        data[repo] = repo_commits
    return data

# ======================== 主程序入口 ========================
def main():
    parser = argparse.ArgumentParser(description="GitHub 开源项目地理位置数据采集器 (Async)")
    parser.add_argument("--projects", nargs='+', required=True, help="项目列表 (格式: owner/repo)")
    parser.add_argument("--token", help="GitHub Personal Access Token (必须提供，或设置 env GITHUB_TOKEN)")
    parser.add_argument("--days", type=int, default=90, help="回溯天数 (默认: 90)")
    parser.add_argument("--output", default="raw_commits_data.json", help="输出文件路径")
    parser.add_argument("--mock", action="store_true", help="使用模拟数据 (无需Token)")
    parser.add_argument("--concurrency", type=int, default=10, help="并发请求数")
    
    args = parser.parse_args()

    # 1. 模式选择
    if args.mock:
        result_data = generate_mock_data(args.projects)
    else:
        token = args.token or os.environ.get("GITHUB_TOKEN")
        if not token:
            print("错误: 必须提供 GitHub Token。使用 --token 参数或设置 GITHUB_TOKEN 环境变量。")
            sys.exit(1)
            
        collector = GitHubCollector(token, concurrency=args.concurrency)
        result_data = asyncio.run(collector.run_batch(args.projects, args.days))

    # 2. 保存结果
    if result_data:
        try:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2)
            
            total_items = sum(len(v) for v in result_data.values())
            print(f"\n成功! 数据已保存至: {output_path}")
            print(f"总计收集: {total_items} 条记录")
            print("提示: 接下来请运行 location_optimizer.py 对 raw_location 进行清洗。")
        except Exception as e:
            print(f"保存文件失败: {e}")
    else:
        print("警告: 未收集到任何数据。")

if __name__ == "__main__":
    main()
