import asyncio
import csv
import json
import logging
import os
import random
import sqlite3
import sys
import time
import argparse
import webbrowser
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple, Union, AsyncGenerator, Callable
from functools import wraps

# -----------------------------------------------------------------------------
# 依赖检查与导入
# -----------------------------------------------------------------------------
try:
    import aiohttp
    import yaml
    from tqdm.asyncio import tqdm
except ImportError as e:
    print(f"CRITICAL ERROR: 缺少必要依赖库: {e.name}")
    print("请运行: pip install aiohttp tqdm PyYAML")
    sys.exit(1)

# -----------------------------------------------------------------------------
# 配置与常量 (Configuration)
# -----------------------------------------------------------------------------
@dataclass
class AppConfig:
    """应用程序配置容器"""
    github_token: str
    db_path: str = "data/github_insight.db"
    report_path: str = "reports/insight_report.html"
    lookback_days: int = 30
    concurrency: int = 5
    log_level: str = "INFO"
    
    # 常量定义
    GITHUB_API_BASE: str = "https://api.github.com"
    NOMINATIM_API: str = "https://nominatim.openstreetmap.org/search"
    USER_AGENT: str = "GitHub-Insight-Pro/2.0 (Research Purpose)"

# -----------------------------------------------------------------------------
# 日志系统 (Logging)
# -----------------------------------------------------------------------------
def setup_logging(level_name: str) -> logging.Logger:
    """配置全局日志系统"""
    level = getattr(logging, level_name.upper(), logging.INFO)
    logger = logging.getLogger("GHInsight")
    logger.setLevel(level)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
            datefmt='%H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

logger = setup_logging("INFO")

# -----------------------------------------------------------------------------
# 工具函数 (Utilities)
# -----------------------------------------------------------------------------
def async_retry(retries: int = 3, delay: int = 1, backoff: int = 2):
    """异步重试装饰器，支持指数退避"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_delay = delay
            for i in range(retries + 1):
                try:
                    return await func(*args, **kwargs)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if i == retries:
                        logger.error(f"函数 {func.__name__} 重试耗尽: {str(e)}")
                        raise
                    logger.warning(f"请求失败 ({i+1}/{retries})，{current_delay}s 后重试: {str(e)}")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

# -----------------------------------------------------------------------------
# 数据模型 (Data Models)
# -----------------------------------------------------------------------------
@dataclass
class CommitRecord:
    """提交记录实体"""
    sha: str
    repo_name: str
    author_login: str
    timestamp: int
    raw_location: Optional[str] = None
    country_code: str = "UNKNOWN"
    city: str = ""
    lat: float = 0.0
    lon: float = 0.0

    @property
    def commit_date(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)

# -----------------------------------------------------------------------------
# 持久化层 (Storage Layer)
# -----------------------------------------------------------------------------
class StorageManager:
    """负责所有 SQLite 数据库操作"""
    
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self):
        """初始化数据库表结构"""
        with self._get_conn() as conn:
            # 1. 地理编码缓存表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS geo_cache (
                    raw_text TEXT PRIMARY KEY,
                    country_code TEXT,
                    city TEXT,
                    lat REAL,
                    lon REAL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 2. 提交记录表 (核心数仓)
            conn.execute("""
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
            # 索引优化查询速度
            conn.execute("CREATE INDEX IF NOT EXISTS idx_commits_country ON commits(country_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_commits_author ON commits(author_login)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_commits_time ON commits(timestamp)")

    def get_geo_cache(self, raw_text: str) -> Optional[Dict[str, Any]]:
        if not raw_text: return None
        with self._get_conn() as conn:
            cursor = conn.execute(
                "SELECT country_code, city, lat, lon FROM geo_cache WHERE raw_text = ?", 
                (raw_text,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def save_geo_cache(self, raw_text: str, data: Dict[str, Any]):
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO geo_cache (raw_text, country_code, city, lat, lon) 
                   VALUES (?, ?, ?, ?, ?)""",
                (raw_text, data.get('country_code'), data.get('city'), data.get('lat'), data.get('lon'))
            )

    def is_commit_exists(self, sha: str) -> bool:
        with self._get_conn() as conn:
            cursor = conn.execute("SELECT 1 FROM commits WHERE sha = ?", (sha,))
            return cursor.fetchone() is not None

    def save_commits(self, commits: List[CommitRecord]):
        if not commits: return
        data = [
            (c.sha, c.repo_name, c.author_login, c.timestamp, c.country_code, c.lat, c.lon)
            for c in commits
        ]
        with self._get_conn() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO commits VALUES (?, ?, ?, ?, ?, ?, ?)", 
                data
            )

    def get_statistics(self) -> Dict[str, Any]:
        """获取用于报告生成的统计数据"""
        stats = {}
        with self._get_conn() as conn:
            # 国家分布
            cur = conn.execute("""
                SELECT country_code, COUNT(*) as cnt 
                FROM commits 
                WHERE country_code != 'UNKNOWN' AND country_code != '' 
                GROUP BY country_code 
                ORDER BY cnt DESC 
                LIMIT 20
            """)
            stats['countries'] = {row['country_code']: row['cnt'] for row in cur.fetchall()}

            # 活跃时间 (UTC)
            cur = conn.execute("""
                SELECT strftime('%H', datetime(timestamp, 'unixepoch')) as hour, COUNT(*) as cnt
                FROM commits 
                GROUP BY hour
                ORDER BY hour
            """)
            stats['hourly'] = {row['hour']: row['cnt'] for row in cur.fetchall()}

            # 顶级贡献者
            cur = conn.execute("""
                SELECT author_login, COUNT(*) as cnt 
                FROM commits 
                GROUP BY author_login 
                ORDER BY cnt DESC 
                LIMIT 10
            """)
            stats['top_devs'] = {row['author_login']: row['cnt'] for row in cur.fetchall()}
            
            # 总览数据
            stats['total_commits'] = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
            stats['total_devs'] = conn.execute("SELECT COUNT(DISTINCT author_login) FROM commits").fetchone()[0]

        return stats

# -----------------------------------------------------------------------------
# 服务层 (Service Layer)
# -----------------------------------------------------------------------------
class GeoService:
    """处理地理编码逻辑，包含缓存策略"""
    
    def __init__(self, session: aiohttp.ClientSession, storage: StorageManager, config: AppConfig):
        self.session = session
        self.storage = storage
        self.config = config
        # Nominatim 严格限制每秒 1 次请求，这里使用 Semaphore 控制
        self._rate_limiter = asyncio.Semaphore(1)

    async def resolve(self, location_str: str) -> Dict[str, Any]:
        """解析位置字符串，优先读缓存，失败则调用 API"""
        empty_res = {"country_code": "UNKNOWN", "city": "", "lat": 0.0, "lon": 0.0}
        
        if not location_str or not location_str.strip():
            return empty_res

        # 1. 查缓存
        cached = self.storage.get_geo_cache(location_str)
        if cached:
            return cached

        # 2. 调用 API
        return await self._fetch_from_api(location_str)

    @async_retry(retries=2, delay=2)
    async def _fetch_from_api(self, query: str) -> Dict[str, Any]:
        async with self._rate_limiter:
            # 遵守 Nominatim 使用策略：必须包含 User-Agent，限制速率
            params = {"q": query, "format": "json", "limit": 1, "accept-language": "en"}
            headers = {"User-Agent": self.config.USER_AGENT}
            
            async with self.session.get(self.config.NOMINATIM_API, params=params, headers=headers) as resp:
                if resp.status != 200:
                    logger.warning(f"GeoAPI 错误 {resp.status}: {query}")
                    return {"country_code": "UNKNOWN", "city": "", "lat": 0.0, "lon": 0.0}
                
                data = await resp.json()
                await asyncio.sleep(1.1) # 强制冷却，确保不超过 1 RPS

                result = {"country_code": "UNKNOWN", "city": "unknown", "lat": 0.0, "lon": 0.0}
                
                if data:
                    item = data[0]
                    display_name = item.get("display_name", "")
                    # 简单的启发式解析国家代码
                    parts = display_name.split(",")
                    country_code = parts[-1].strip().upper()[:3] if parts else "UNKNOWN"
                    
                    result = {
                        "country_code": country_code,
                        "city": item.get("type", "unknown"),
                        "lat": float(item.get("lat", 0)),
                        "lon": float(item.get("lon", 0))
                    }
                
                # 写入缓存（即使是空结果也缓存，防止重复无效查询）
                self.storage.save_geo_cache(query, result)
                return result

class GitHubService:
    """处理 GitHub API 交互"""
    
    def __init__(self, session: aiohttp.ClientSession, token: str, config: AppConfig):
        self.session = session
        self.token = token
        self.config = config
        self.base_headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": config.USER_AGENT
        }

    async def _handle_rate_limit(self, resp: aiohttp.ClientResponse):
        """处理 GitHub 速率限制"""
        if resp.status == 403 and 'X-RateLimit-Remaining' in resp.headers:
            remaining = int(resp.headers.get('X-RateLimit-Remaining', 1))
            if remaining == 0:
                reset_time = int(resp.headers.get('X-RateLimit-Reset', 0))
                wait_time = max(reset_time - time.time(), 0) + 1
                logger.warning(f"GitHub API 限流触发，休眠 {wait_time:.0f} 秒...")
                await asyncio.sleep(wait_time)
                return True
        return False

    @async_retry()
    async def get_user_location(self, username: str) -> str:
        url = f"{self.config.GITHUB_API_BASE}/users/{username}"
        async with self.session.get(url, headers=self.base_headers) as resp:
            if await self._handle_rate_limit(resp):
                return await self.get_user_location(username) # Retry logic handles recursion depth implicitly via decorator
            if resp.status == 404:
                return ""
            data = await resp.json()
            return data.get("location") or ""

    async def fetch_commits(self, repo: str, since: datetime) -> AsyncGenerator[List[Dict], None]:
        """生成器模式获取 Commit 批次"""
        url = f"{self.config.GITHUB_API_BASE}/repos/{repo}/commits"
        params = {"since": since.isoformat(), "per_page": 100, "page": 1}
        
        while True:
            try:
                async with self.session.get(url, headers=self.base_headers, params=params) as resp:
                    if await self._handle_rate_limit(resp):
                        continue # Retry same page
                    
                    if resp.status != 200:
                        logger.error(f"获取 {repo} 失败: HTTP {resp.status}")
                        break
                        
                    batch = await resp.json()
                    if not batch or not isinstance(batch, list):
                        break
                        
                    yield batch
                    
                    if len(batch) < 100:
                        break
                    params["page"] += 1
            except Exception as e:
                logger.error(f"Fetch loop error: {e}")
                break

# -----------------------------------------------------------------------------
# 核心逻辑控制器 (Controller)
# -----------------------------------------------------------------------------
class InsightEngine:
    """主逻辑编排引擎"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.storage = StorageManager(config.db_path)
    
    async def run(self, projects: List[str]):
        """执行主任务流程"""
        conn = aiohttp.TCPConnector(limit=self.config.concurrency)
        async with aiohttp.ClientSession(connector=conn) as session:
            self.gh_service = GitHubService(session, self.config.github_token, self.config)
            self.geo_service = GeoService(session, self.storage, self.config)
            
            # 计算起始时间
            since_date = datetime.now(timezone.utc) - timedelta(days=self.config.lookback_days)
            logger.info(f"开始分析任务 - 回溯时间: {since_date.date()} - 项目数: {len(projects)}")

            # 创建并发任务
            tasks = [self._process_single_repo(p, since_date) for p in projects]
            
            # 使用 tqdm 显示总体进度
            await tqdm.gather(*tasks, desc="Repositories Analysis", unit="repo")
            
            logger.info("所有仓库分析完成，生成报告中...")
            self._generate_report()

    async def _process_single_repo(self, repo: str, since: datetime):
        """处理单个仓库：获取Commit -> 过滤 -> 补全Geo -> 存储"""
        new_commits_buffer = []
        
        async for batch in self.gh_service.fetch_commits(repo, since):
            for item in batch:
                sha = item['sha']
                
                # 跳过已处理或无作者信息的提交
                if self.storage.is_commit_exists(sha) or not item.get('author'):
                    continue
                
                author_login = item['author']['login']
                commit_ts = datetime.fromisoformat(
                    item['commit']['author']['date'].replace("Z", "+00:00")
                ).timestamp()

                # 1. 获取用户位置 (Raw)
                raw_loc = await self.gh_service.get_user_location(author_login)
                
                # 2. 解析地理位置 (Geo)
                geo_info = await self.geo_service.resolve(raw_loc)
                
                # 3. 构建记录
                record = CommitRecord(
                    sha=sha,
                    repo_name=repo,
                    author_login=author_login,
                    timestamp=int(commit_ts),
                    raw_location=raw_loc,
                    **geo_info
                )
                new_commits_buffer.append(record)
            
            # 批次写入数据库，减少 IO
            if new_commits_buffer:
                self.storage.save_commits(new_commits_buffer)
                new_commits_buffer.clear()

    def _generate_report(self):
        """调用报告生成器"""
        stats = self.storage.get_statistics()
        if not stats.get('total_commits'):
            logger.warning("没有采集到任何数据，跳过报告生成。")
            return
            
        generator = ReportGenerator(self.config.report_path)
        generator.render(stats)
        
        abs_path = os.path.abspath(self.config.report_path)
        logger.info(f"可视化报告已生成: file://{abs_path}")
        # webbrowser.open(f"file://{abs_path}") # 可选：自动打开浏览器

# -----------------------------------------------------------------------------
# 报告生成器 (View Layer)
# -----------------------------------------------------------------------------
class ReportGenerator:
    """生成 HTML 报告"""
    
    def __init__(self, output_path: str):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def render(self, stats: Dict[str, Any]):
        html_content = self._get_template().format(
            gen_time=datetime.now().strftime('%Y-%m-%d %H:%M'),
            total_commits=stats.get('total_commits', 0),
            total_devs=stats.get('total_devs', 0),
            countries_labels=json.dumps(list(stats['countries'].keys())),
            countries_data=json.dumps(list(stats['countries'].values())),
            hourly_labels=json.dumps(list(stats['hourly'].keys())),
            hourly_data=json.dumps(list(stats['hourly'].values())),
            top_devs_rows=self._render_table_rows(stats['top_devs'])
        )
        
        with open(self.output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

    def _render_table_rows(self, dev_dict: Dict[str, int]) -> str:
        rows = []
        for rank, (user, count) in enumerate(dev_dict.items(), 1):
            rows.append(
                f"<tr><td>{rank}</td><td><a href='https://github.com/{user}' target='_blank'>{user}</a></td><td>{count}</td></tr>"
            )
        return "".join(rows)

    def _get_template(self) -> str:
        return """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GitHub Insight Pro Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{ --primary: #2563eb; --bg: #f8fafc; --card: #ffffff; --text: #1e293b; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 20px; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{ text-align: center; margin-bottom: 40px; padding: 20px; background: var(--card); border-radius: 12px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .stat-card {{ background: var(--card); padding: 20px; border-radius: 8px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
        .stat-val {{ font-size: 2em; font-weight: bold; color: var(--primary); }}
        .charts-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 30px; }}
        .chart-box {{ background: var(--card); padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }}
        .table-box {{ background: var(--card); padding: 20px; border-radius: 12px; overflow: hidden; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
        th {{ background: #f1f5f9; font-weight: 600; }}
        a {{ color: var(--primary); text-decoration: none; }}
        @media (max-width: 768px) {{ .charts-grid {{ grid-template-columns: 1fr; }} }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>GitHub 项目地理分布洞察报告</h1>
            <p style="color: #64748b;">生成时间: {gen_time}</p>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-val">{total_commits}</div>
                <div>分析 Commit 总数</div>
            </div>
            <div class="stat-card">
                <div class="stat-val">{total_devs}</div>
                <div>活跃贡献者</div>
            </div>
        </div>

        <div class="charts-grid">
            <div class="chart-box">
                <h3>🌍 贡献者国家/地区分布</h3>
                <canvas id="countryChart"></canvas>
            </div>
            <div class="chart-box">
                <h3>⏰ 全球提交时间分布 (UTC)</h3>
                <canvas id="hourChart"></canvas>
            </div>
        </div>

        <div class="table-box">
            <h3>🏆 核心贡献者榜单 (Top 10)</h3>
            <table>
                <thead><tr><th>排名</th><th>用户 ID</th><th>提交数</th></tr></thead>
                <tbody>{top_devs_rows}</tbody>
            </table>
        </div>
    </div>

    <script>
        const commonOptions = {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }} }};
        
        new Chart(document.getElementById('countryChart'), {{
            type: 'bar',
            data: {{
                labels: {countries_labels},
                datasets: [{{
                    label: 'Commits',
                    data: {countries_data},
                    backgroundColor: '#3b82f6',
                    borderRadius: 4
                }}]
            }},
            options: commonOptions
        }});

        new Chart(document.getElementById('hourChart'), {{
            type: 'line',
            data: {{
                labels: {hourly_labels},
                datasets: [{{
                    label: 'Activity Volume',
                    data: {hourly_data},
                    borderColor: '#ef4444',
                    backgroundColor: 'rgba(239, 68, 68, 0.1)',
                    fill: true,
                    tension: 0.4
                }}]
            }},
            options: commonOptions
        }});
    </script>
</body>
</html>
"""

# -----------------------------------------------------------------------------
# 程序入口 (Entry Point)
# -----------------------------------------------------------------------------
def load_projects_from_config(config_path: str) -> List[str]:
    """从 YAML 加载项目列表"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return data.get('projects', [])
    except Exception as e:
        logger.error(f"无法读取配置文件 {config_path}: {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description="GitHub Insight Pro - Developer Geography Analyzer")
    parser.add_argument("-p", "--projects", nargs='+', help="GitHub 仓库路径 (e.g. facebook/react)")
    parser.add_argument("-f", "--config", help="YAML 配置文件路径包含项目列表")
    parser.add_argument("-d", "--days", type=int, default=30, help="分析过去多少天的数据 (默认: 30)")
    parser.add_argument("-o", "--output", default="reports/insight_report.html", help="报告输出路径")
    parser.add_argument("--db", default="data/github_data.db", help="SQLite 数据库路径")
    
    args = parser.parse_args()
    
    # 1. 获取 Token
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        logger.critical("未检测到环境变量 GITHUB_TOKEN。请设置后重试。")
        logger.info("Example: export GITHUB_TOKEN=ghp_xxxxxxxxxxxx")
        sys.exit(1)

    # 2. 确定项目列表
    projects = []
    if args.projects:
        projects.extend(args.projects)
    if args.config:
        projects.extend(load_projects_from_config(args.config))
    
    # 去重并验证
    projects = list(set(p for p in projects if "/" in p))
    
    if not projects:
        logger.error("未指定有效的 GitHub 项目。请使用 -p 或 -f 参数。")
        parser.print_help()
        sys.exit(1)

    # 3. 初始化配置
    config = AppConfig(
        github_token=token,
        db_path=args.db,
        report_path=args.output,
        lookback_days=args.days
    )

    # 4. 运行引擎
    engine = InsightEngine(config)
    
    try:
        asyncio.run(engine.run(projects))
    except KeyboardInterrupt:
        logger.info("用户中断操作，正在安全退出...")
    except Exception as e:
        logger.exception(f"程序运行发生未捕获异常: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
