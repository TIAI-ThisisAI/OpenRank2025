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
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple, Union
from dataclasses import dataclass, asdict, field

# 依赖检查
try:
    import aiohttp
    import yaml
    from tqdm.asyncio import tqdm
except ImportError:
    print("错误: 缺少必要依赖库。请运行: pip install aiohttp tqdm PyYAML")
    sys.exit(1)

# ======================== 增强配置与常量 ========================
GITHUB_API_BASE = "https://api.github.com"
NOMINATIM_API = "https://nominatim.openstreetmap.org/search"
DEFAULT_CONCURRENCY = 5
GEO_CACHE_TTL_DAYS = 30  # 地理信息缓存有效期

@dataclass
class CommitData:
    """扩展的 Commit 核心数据结构"""
    sha: str
    repo_name: str
    author_login: str
    timestamp: int
    raw_location: str
    # 地理编码后的字段
    country_code: str = ""
    city: str = ""
    lat: float = 0.0
    lon: float = 0.0

    def to_dict(self) -> Dict:
        return asdict(self)

# ======================== 存储与分析引擎 ========================
class InsightStorage:
    """增强型存储引擎：负责数据持久化、缓存管理及统计分析"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        """初始化多表结构：用户、地理缓存、Commit记录"""
        with self.conn:
            # 1. 地理编码缓存 (避免重复消耗 API)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS geo_cache (
                    raw_text TEXT PRIMARY KEY,
                    country_code TEXT,
                    city TEXT,
                    lat REAL,
                    lon REAL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 2. 用户元数据
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS user_profiles (
                    username TEXT PRIMARY KEY,
                    location TEXT,
                    company TEXT,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 3. Commit 完整记录 (本地数仓化)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS commits (
                    sha TEXT PRIMARY KEY,
                    repo_name TEXT,
                    author_login TEXT,
                    timestamp INTEGER,
                    country_code TEXT,
                    lat REAL,
                    lon REAL
                )
            """)

    # --- 地理缓存操作 ---
    def get_cached_geo(self, raw_text: str) -> Optional[Dict]:
        if not raw_text: return None
        cursor = self.conn.execute(
            "SELECT country_code, city, lat, lon FROM geo_cache WHERE raw_text = ?", (raw_text,)
        )
        row = cursor.fetchone()
        return {"country_code": row[0], "city": row[1], "lat": row[2], "lon": row[3]} if row else None

    def save_geo_cache(self, raw_text: str, geo_data: Dict):
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO geo_cache (raw_text, country_code, city, lat, lon) VALUES (?, ?, ?, ?, ?)",
                (raw_text, geo_data.get('country_code'), geo_data.get('city'), geo_data.get('lat'), geo_data.get('lon'))
            )

    # --- 数据写入 ---
    def save_commits(self, commits: List[CommitData]):
        if not commits: return
        data = [(c.sha, c.repo_name, c.author_login, c.timestamp, c.country_code, c.lat, c.lon) for c in commits]
        with self.conn:
            self.conn.executemany(
                "INSERT OR IGNORE INTO commits VALUES (?, ?, ?, ?, ?, ?, ?)", data
            )

    def is_processed(self, sha: str) -> bool:
        cursor = self.conn.execute("SELECT 1 FROM commits WHERE sha = ?", (sha,))
        return cursor.fetchone() is not None

    # --- 数据分析 ---
    def get_stats(self) -> Dict:
        """从 SQLite 生成多维度统计数据数据"""
        stats = {}
        with self.conn:
            # 国家分布
            cursor = self.conn.execute(
                "SELECT country_code, COUNT(*) as count FROM commits WHERE country_code != '' GROUP BY country_code ORDER BY count DESC"
            )
            stats['countries'] = dict(cursor.fetchall())
            
            # 时段分布 (UTC)
            cursor = self.conn.execute(
                "SELECT strftime('%H', datetime(timestamp, 'unixepoch')) as hour, COUNT(*) FROM commits GROUP BY hour"
            )
            stats['hourly'] = dict(cursor.fetchall())

            # 活跃开发者排行
            cursor = self.conn.execute(
                "SELECT author_login, COUNT(*) as count FROM commits GROUP BY author_login ORDER BY count DESC LIMIT 10"
            )
            stats['top_devs'] = dict(cursor.fetchall())
        return stats

    def close(self):
        self.conn.close()

# ======================== 地理编码引擎 ========================
class GeocodingService:
    """使用 OpenStreetMap Nominatim 进行异步地理编码"""
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.semaphore = asyncio.Semaphore(1) # Nominatim 限制每秒 1 次请求

    async def resolve(self, location_str: str) -> Dict:
        if not location_str:
            return {"country_code": "", "city": "", "lat": 0.0, "lon": 0.0}
        
        try:
            async with self.semaphore:
                params = {"q": location_str, "format": "json", "limit": 1, "accept-language": "en"}
                headers = {"User-Agent": "GitHub-Geo-Insight-Project-v2"}
                async with self.session.get(NOMINATIM_API, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data:
                            res = data[0]
                            # 解析国家代码
                            display_name = res.get("display_name", "")
                            country_code = display_name.split(",")[-1].strip().upper()[:3]
                            return {
                                "country_code": country_code,
                                "city": res.get("type", "unknown"),
                                "lat": float(res.get("lat", 0)),
                                "lon": float(res.get("lon", 0))
                            }
        except Exception:
            pass
        return {"country_code": "UNKNOWN", "city": "", "lat": 0.0, "lon": 0.0}

# ======================== 核心采集器 ========================
class GitHubInsightPro:
    def __init__(self, token: str, db_path: str, concurrency: int = DEFAULT_CONCURRENCY):
        self.token = token
        self.storage = InsightStorage(db_path)
        self.concurrency = concurrency
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger("GH-Insight")
        logger.setLevel(logging.INFO)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(sh)
        return logger

    @asynccontextmanager
    async def session_scope(self):
        headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Insight-Pro-Tool"
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            self.geo_service = GeocodingService(session)
            self.http_session = session
            yield self

    async def _api_call(self, url: str, params: Optional[Dict] = None):
        """包含重试逻辑的 API 调用"""
        for i in range(3):
            async with self.http_session.get(url, params=params) as resp:
                if resp.status == 403: # 速率限制
                    reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                    wait = max(reset - time.time(), 5) + 1
                    self.logger.warning(f"速率限制，等待 {wait:.1f}s...")
                    await asyncio.sleep(wait)
                    continue
                if resp.status != 200: return None
                return await resp.json()
        return None

    async def get_user_loc(self, username: str) -> str:
        data = await self._api_call(f"{GITHUB_API_BASE}/users/{username}")
        return (data.get("location") or "") if data else ""

    async def process_repo(self, repo_path: str, days: int):
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        self.logger.info(f"正在分析仓库: {repo_path}")
        
        page = 1
        all_new_commits = []
        
        while True:
            params = {"per_page": 100, "page": page, "since": since}
            batch = await self._api_call(f"{GITHUB_API_BASE}/repos/{repo_path}/commits", params)
            if not batch: break
            
            # 过滤并去重
            targets = [c for c in batch if not self.storage.is_processed(c['sha']) and c.get('author')]
            if not targets: 
                if page > 1: break # 遇到断点
                else: # 这一页全是旧的
                    page += 1
                    continue

            # 并行获取用户信息和地理位置
            for c in targets:
                username = c['author']['login']
                raw_loc = await self.get_user_loc(username)
                
                # 地理编码逻辑
                geo = self.storage.get_cached_geo(raw_loc)
                if not geo:
                    geo = await self.geo_service.resolve(raw_loc)
                    self.storage.save_geo_cache(raw_loc, geo)
                
                dt = datetime.fromisoformat(c['commit']['author']['date'].replace("Z", "+00:00"))
                cd = CommitData(
                    sha=c['sha'],
                    repo_name=repo_path,
                    author_login=username,
                    timestamp=int(dt.timestamp()),
                    raw_location=raw_loc,
                    **geo
                )
                all_new_commits.append(cd)

            if len(batch) < 100: break
            page += 1
            
        self.storage.save_commits(all_new_commits)
        self.logger.info(f"[{repo_path}] 已存入 {len(all_new_commits)} 条新记录")

# ======================== 可视化面板生成器 ========================
def generate_report(stats: Dict, output_file: str):
    """生成包含交互式图表的 HTML 报告"""
    html_template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>GitHub 开发者洞察报告</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{ font-family: 'Segoe UI', sans-serif; background: #f4f7f6; margin: 40px; }}
            .card {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 20px; }}
            h1 {{ color: #2c3e50; }}
            .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
        </style>
    </head>
    <body>
        <h1>GitHub 项目开发者地理洞察报告</h1>
        <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        
        <div class="grid">
            <div class="card">
                <h3>国家/地区分布 Top 10</h3>
                <canvas id="countryChart"></canvas>
            </div>
            <div class="card">
                <h3>24小时贡献活跃度 (UTC)</h3>
                <canvas id="hourChart"></canvas>
            </div>
        </div>

        <div class="card">
            <h3>核心贡献者榜单 (Commit Count)</h3>
            <table style="width:100%; border-collapse: collapse;">
                <tr style="background:#eee"><th>用户名</th><th>贡献数</th></tr>
                {''.join([f"<tr><td>{u}</td><td>{c}</td></tr>" for u,c in stats['top_devs'].items()])}
            </table>
        </div>

        <script>
            const countryCtx = document.getElementById('countryChart').getContext('2d');
            new Chart(countryCtx, {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(list(stats['countries'].keys())[:10])},
                    datasets: [{{ label: 'Commits', data: {json.dumps(list(stats['countries'].values())[:10])}, backgroundColor: '#3498db' }}]
                }}
            }});

            const hourCtx = document.getElementById('hourChart').getContext('2d');
            new Chart(hourCtx, {{
                type: 'line',
                data: {{
                    labels: {json.dumps(sorted(list(stats['hourly'].keys())))},
                    datasets: [{{ label: 'Activity', data: {json.dumps([stats['hourly'].get(str(i).zfill(2), 0) for i in range(24)])}, borderColor: '#e74c3c', fill: true }}]
                }}
            }});
        </script>
    </body>
    </html>
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_template)

# ======================== 程序入口 ========================
async def main():
    parser = argparse.ArgumentParser(description="GitHub 全球开发者洞察专业版")
    parser.add_argument("-p", "--projects", nargs='+', help="仓库列表 owner/repo")
    parser.add_argument("-f", "--config", help="YAML 配置文件路径")
    parser.add_argument("-d", "--days", type=int, default=30, help="回溯天数")
    parser.add_argument("-o", "--report", default="insight_report.html", help="可视化报告输出路径")
    
    args = parser.parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    
    if not token:
        print("请设置环境变量 GITHUB_TOKEN")
        return

    # 加载项目列表
    projects = args.projects or []
    if args.config:
        with open(args.config, 'r') as cf:
            cfg = yaml.safe_load(cf)
            projects.extend(cfg.get('projects', []))
    
    if not projects:
        print("没有指定项目，请使用 -p 或 -f 参数")
        return

    tool = GitHubInsightPro(token, "github_master.db")
    
    async with tool.session_scope():
        tasks = [tool.process_repo(p, args.days) for p in projects]
        await tqdm.gather(*tasks, desc="数据同步中")

    # 生成分析报告
    stats = tool.storage.get_stats()
    generate_report(stats, args.report)
    print(f"\n[完成] 报告已生成: {args.report}")
    print(f"[数据] 本地 SQLite 数据库: github_master.db (可用 SQL 进行深度分析)")
    
    tool.storage.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
