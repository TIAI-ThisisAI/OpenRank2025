# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import os
import sys
import time
import random
import argparse
import csv
import re
from typing import List, Dict, Any, Optional, Set, Iterator
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from itertools import cycle

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
DEFAULT_CACHE_TTL_DAYS = 7

@dataclass
class CommitData:
    """标准化的提交数据结构"""
    timestamp_unix: int
    raw_location: str
    contributor_id: str
    commit_sha: str
    repo_name: str
    commit_message: str
    # 预留字段，供下游 location_optimizer.py 填充
    location_iso3: str = "NEED_CLEANING"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class DiskCache:
    """带TTL（过期时间）的持久化缓存"""
    def __init__(self, filepath: str, ttl_days: int = DEFAULT_CACHE_TTL_DAYS):
        self.filepath = filepath
        self.ttl_seconds = ttl_days * 86400
        self.data: Dict[str, Dict[str, Any]] = {}
        self.loaded = False

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    self.data = json.load(f)
                
                # 清理过期数据
                now = time.time()
                original_len = len(self.data)
                self.data = {
                    k: v for k, v in self.data.items() 
                    if isinstance(v, dict) and v.get("expire_at", 0) > now
                }
                cleaned = original_len - len(self.data)
                if cleaned > 0:
                    logging.info(f"缓存清理: 移除 {cleaned} 条过期记录")
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
        entry = self.data.get(key)
        if entry and entry.get("expire_at", 0) > time.time():
            return entry.get("value")
        return None

    def set(self, key: str, value: str):
        self.data[key] = {
            "value": value,
            "expire_at": time.time() + self.ttl_seconds
        }

class GitHubCollector:
    def __init__(self, tokens: List[str], cache_path: str = CACHE_FILE_NAME, concurrency: int = 10, ttl_days: int = 7):
        self.tokens = [t.strip() for t in tokens if t.strip()]
        if not self.tokens:
            raise ValueError("至少需要提供一个有效的GitHub Token")
        self.token_cycle = cycle(self.tokens) # Token 轮询器
        
        self.concurrency = concurrency
        self.cache = DiskCache(cache_path, ttl_days)
        self.cache.load()
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler("gh_collector.log", encoding="utf-8")
            ]
        )
        self.logger = logging.getLogger("GitHubCollector")

    def _get_headers(self) -> Dict[str, str]:
        # 每次请求轮询使用下一个 Token
        token = next(self.token_cycle)
        return {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Commit-Collector-Async/3.0"
        }

    async def _fetch(self, session: aiohttp.ClientSession, url: str, params: Dict = None) -> Any:
        """通用请求函数，处理速率限制和重试"""
        retry_delay = 1.0
        max_retries = 5
        
        for attempt in range(max_retries):
            try:
                # 动态获取Headers以实现Token轮询
                async with session.get(url, headers=self._get_headers(), params=params) as response:
                    # 处理速率限制
                    if response.status == 403:
                        rate_limit_remaining = response.headers.get('X-RateLimit-Remaining')
                        if rate_limit_remaining == '0':
                            reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                            wait_time = max(reset_time - time.time(), 1) + 1
                            self.logger.warning(f"当前Token触发速率限制，暂停 {wait_time:.0f} 秒...")
                            await asyncio.sleep(wait_time) 
                            # 注意：如果是多Token轮询，其实可以不睡直接重试下一个Token，
                            # 但为了简化逻辑防止所有Token瞬间耗尽，这里选择保守策略。
                            continue

                    if response.status != 200:
                        # 404 表示用户或仓库不存在，无需重试
                        if response.status == 404:
                            return None
                        # 429 Too Many Requests
                        if response.status == 429:
                            retry_after = int(response.headers.get("Retry-After", 60))
                            self.logger.warning(f"触发429限流，等待 {retry_after} 秒")
                            await asyncio.sleep(retry_after)
                            continue
                            
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
            # 初步清洗：去除换行和首尾空格
            location = re.sub(r'\s+', ' ', location).strip()
        
        # 3. 写入缓存
        self.cache.set(username, location)
        return location

    async def collect_repo_commits(
        self, 
        repo_full_name: str, 
        since: datetime,
        until: datetime,
        filter_keyword: Optional[str] = None,
        filter_author: Optional[str] = None
    ) -> List[CommitData]:
        """收集单个仓库的提交数据"""
        self.logger.info(f"开始处理项目: {repo_full_name} ({since.date()} -> {until.date()})")
        
        url = f"{GITHUB_API_BASE}/repos/{repo_full_name}/commits"
        
        # 构建API过滤参数
        api_params = {
            "per_page": DEFAULT_PER_PAGE,
            "since": since.isoformat(),
            "until": until.isoformat()
        }
        if filter_author:
            api_params["author"] = filter_author
        
        all_commits_raw = []
        page = 1
        
        async with aiohttp.ClientSession() as session:
            # 1. 获取所有提交列表 (异步分页)
            # 虽然分页必须串行获取（需要上一页才知道有没有下一页），但使用aiohttp比requests快
            while True:
                params = {**api_params, "page": page}
                self.logger.info(f"[{repo_full_name}] 获取提交列表第 {page} 页...")
                
                batch = await self._fetch(session, url, params)
                if not batch:
                    break
                
                all_commits_raw.extend(batch)
                if len(batch) < DEFAULT_PER_PAGE:
                    break
                page += 1

            self.logger.info(f"[{repo_full_name}] 共获取 {len(all_commits_raw)} 条原始提交，开始解析与过滤...")

            # 2. 提取唯一作者并预处理
            unique_authors = set()
            commits_to_process = []

            for commit in all_commits_raw:
                # 关键词过滤 (API不支持，需本地过滤)
                message = commit["commit"]["message"]
                if filter_keyword and filter_keyword not in message:
                    continue

                author = commit.get("author") # GitHub账号信息
                if author and "login" in author:
                    login = author["login"]
                    unique_authors.add(login)
                    commits_to_process.append({
                        "login": login,
                        "sha": commit["sha"],
                        "date": commit["commit"]["author"]["date"],
                        "message": message
                    })

            self.logger.info(f"[{repo_full_name}] 过滤后剩余 {len(commits_to_process)} 条提交，涉及 {len(unique_authors)} 位贡献者")

            # 3. 并发获取用户位置
            semaphore = asyncio.Semaphore(self.concurrency)
            user_location_map = {}
            
            async def _bounded_fetch_user(username):
                async with semaphore:
                    loc = await self.fetch_user_location(session, username)
                    user_location_map[username] = loc

            # 使用 tqdm 显示用户解析进度
            if unique_authors:
                tasks = [_bounded_fetch_user(u) for u in unique_authors]
                for f in tqdm.as_completed(tasks, desc=f"解析 {repo_full_name} 贡献者", total=len(tasks)):
                    await f
            
            # 4. 组装最终数据
            results = []
            for item in commits_to_process:
                ts_str = item["date"].replace("Z", "+00:00")
                try:
                    ts = int(datetime.fromisoformat(ts_str).timestamp())
                except:
                    ts = 0
                
                loc = user_location_map.get(item["login"], "")
                
                results.append(CommitData(
                    timestamp_unix=ts,
                    raw_location=loc,
                    contributor_id=item["login"],
                    commit_sha=item["sha"],
                    repo_name=repo_full_name,
                    commit_message=item["message"]
                ))

            # 每一轮大任务结束保存一次缓存
            self.cache.save()
            return results

    async def run_batch(
        self, 
        repos: List[str], 
        since: datetime,
        until: datetime,
        filter_keyword: Optional[str] = None,
        filter_author: Optional[str] = None
    ) -> Dict[str, List[Dict]]:
        
        final_data = {}
        for repo in repos:
            try:
                commits = await self.collect_repo_commits(
                    repo, since, until, filter_keyword, filter_author
                )
                final_data[repo] = [c.to_dict() for c in commits]
                self.logger.info(f"[{repo}] 处理完成，收集到 {len(commits)} 条记录")
            except Exception as e:
                self.logger.error(f"[{repo}] 处理出错: {e}")
        return final_data

# ======================== 统计与输出工具 ========================
def write_csv(data: Dict[str, List[Dict]], filepath: str):
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ["repo_name", "timestamp_unix", "contributor_id", "raw_location", "location_iso3", "commit_sha", "commit_message"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for repo_data in data.values():
                for item in repo_data:
                    # 确保只写入定义的字段
                    row = {k: item.get(k, "") for k in fieldnames}
                    writer.writerow(row)
        logging.info(f"CSV 数据已保存至: {filepath}")
    except Exception as e:
        logging.error(f"写入 CSV 失败: {e}")

def write_stats(data: Dict[str, List[Dict]], filepath: str):
    stats = {}
    report_lines = ["=== GitHub 项目提交统计报告 ==="]
    
    for repo, commits in data.items():
        total = len(commits)
        users = set(c['contributor_id'] for c in commits)
        locs = [c['raw_location'] for c in commits if c['raw_location']]
        
        # 简单位置统计
        loc_counts = {}
        for l in locs:
            loc_counts[l] = loc_counts.get(l, 0) + 1
        top_locs = sorted(loc_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        stats[repo] = {
            "total_commits": total,
            "unique_contributors": len(users),
            "location_filled_rate": f"{len(locs)/total:.1%}" if total > 0 else "0%"
        }
        
        report_lines.append(f"\n【{repo}】")
        report_lines.append(f"  总提交数: {total}")
        report_lines.append(f"  贡献者数: {len(users)}")
        report_lines.append(f"  位置填写率: {len(locs)}/{total}")
        report_lines.append(f"  Top 5 位置: {top_locs}")

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("\n".join(report_lines))
        
        json_path = filepath.replace(".txt", ".json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
            
        logging.info(f"统计报告已保存至: {filepath}")
    except Exception as e:
        logging.error(f"写入统计报告失败: {e}")

# ======================== 模拟数据生成器 ========================
def generate_mock_data(repos: List[str], count: int = 200) -> Dict[str, List[Dict]]:
    print(f"--- 正在生成模拟数据 ({count}条/库) ---")
    data = {}
    locations = ["Beijing, China", "San Francisco, CA", "Remote", "Tokyo, JP", "Berlin", ""]
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
                "commit_message": f"feat: mock commit {i}",
                "location_iso3": "NEED_CLEANING"
            })
        data[repo] = repo_commits
    return data

# ======================== 主程序入口 ========================
def main():
    parser = argparse.ArgumentParser(description="GitHub 高性能数据采集器 (Async Pro)")
    parser.add_argument("--projects", nargs='+', required=True, help="项目列表 (格式: owner/repo)")
    parser.add_argument("--tokens", help="GitHub Token (逗号分隔，支持轮询以突破限流)")
    parser.add_argument("--since", help="起始时间 (ISO格式, 如 2024-01-01)")
    parser.add_argument("--days", type=int, default=90, help="回溯天数 (默认: 90，如果未指定since)")
    parser.add_argument("--output", required=True, help="输出文件路径 (支持 .json 或 .csv)")
    parser.add_argument("--stats", help="统计报告输出路径 (如 stats.txt)")
    
    # 过滤参数
    parser.add_argument("--filter-keyword", help="仅保留Commit Message包含该关键词的提交")
    parser.add_argument("--filter-author", help="仅保留指定作者(login)的提交")
    
    # 高级参数
    parser.add_argument("--mock", action="store_true", help="模拟模式 (无需Token)")
    parser.add_argument("--concurrency", type=int, default=10, help="并发请求数")
    parser.add_argument("--cache-ttl", type=int, default=7, help="缓存有效期(天)")
    
    args = parser.parse_args()

    # 1. 计算时间范围
    until = datetime.now(timezone.utc)
    if args.since:
        try:
            # 处理 ISO 格式
            since = datetime.fromisoformat(args.since.replace("Z", "+00:00"))
            if since.tzinfo is None:
                since = since.replace(tzinfo=timezone.utc)
        except ValueError:
            print("错误: 时间格式无效，请使用 ISO 格式 (如 2024-01-01)")
            sys.exit(1)
    else:
        since = until - timedelta(days=args.days)

    # 2. 运行采集
    if args.mock:
        result_data = generate_mock_data(args.projects)
    else:
        tokens_str = args.tokens or os.environ.get("GITHUB_TOKEN")
        if not tokens_str:
            print("错误: 必须提供 GitHub Token。使用 --tokens 参数或设置 GITHUB_TOKEN 环境变量。")
            sys.exit(1)
        
        tokens = tokens_str.split(",")
        collector = GitHubCollector(
            tokens, 
            concurrency=args.concurrency,
            ttl_days=args.cache_ttl
        )
        
        result_data = asyncio.run(collector.run_batch(
            repos=args.projects, 
            since=since, 
            until=until,
            filter_keyword=args.filter_keyword,
            filter_author=args.filter_author
        ))

    # 3. 输出结果
    if result_data:
        # 确保输出目录存在
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if args.output.lower().endswith(".csv"):
            write_csv(result_data, args.output)
        else:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2)
                
        print(f"\n成功! 数据已保存至: {output_path}")
        
        if args.stats:
            write_stats(result_data, args.stats)
    else:
        print("警告: 未收集到任何数据。")

if __name__ == "__main__":
    main()
