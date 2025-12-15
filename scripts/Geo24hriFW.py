# -*- coding: utf-8 -*-
import os
import re
import csv
import json
import time
import sys
import logging
import argparse
from typing import (
    TypedDict, List, Dict, Optional, Union, Tuple, Iterator
)
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======================== 全局配置与日志 ========================
# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("gh_collector.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# 默认配置（可通过环境变量/命令行覆盖）
DEFAULT_CONFIG = {
    "GITHUB_API_BASE": "https://api.github.com",
    "GITHUB_TOKEN": os.getenv("GITHUB_TOKEN", "YOUR_GITHUB_PERSONAL_ACCESS_TOKEN"),
    "CACHE_FILEPATH": os.getenv("GH_CACHE_PATH", "gh_contributor_location_cache.json"),
    "CACHE_TTL_DAYS": 7,  # 缓存有效期7天
    "PER_PAGE": 100,
    "DEFAULT_SINCE_DAYS": 90,
    "REQUEST_TIMEOUT": 10,
    "RETRY_TIMES": 3,
    "RETRY_DELAY": 2,  # 重试基础延迟（秒）
    "RATE_LIMIT_SLEEP": 60  # 速率限制时的休眠时间（秒）
}

# ======================== 类型定义（强类型） ========================
class RawCommit(TypedDict, total=False):
    sha: str
    author: Dict[str, str]
    commit: Dict[str, Any]
    raw_location: str  # 仅模拟数据使用

class OptimizedCommitData(TypedDict):
    timestamp_unix: int
    raw_location: str
    location_iso3: str
    contributor_id: str
    commit_sha: str  # 新增：提交SHA
    commit_message: str  # 新增：提交信息

class ContributorCacheEntry(TypedDict):
    location: str
    expire_at: int  # 过期时间（Unix时间戳）

ContributorCache = Dict[str, ContributorCacheEntry]
ProjectStats = Dict[str, Union[int, Dict[str, int]]]

# ======================== 工具函数 ========================
def get_github_tokens(tokens_str: Optional[str]) -> List[str]:
    """解析多个GitHub Token（逗号分隔）"""
    if not tokens_str:
        return [DEFAULT_CONFIG["GITHUB_TOKEN"]]
    tokens = [t.strip() for t in tokens_str.split(",") if t.strip()]
    return tokens if tokens else [DEFAULT_CONFIG["GITHUB_TOKEN"]]

def validate_datetime_str(dt_str: str) -> Optional[datetime]:
    """验证并解析时间字符串（ISO格式）"""
    try:
        # 处理带Z和不带Z的情况
        if dt_str.endswith("Z"):
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return datetime.fromisoformat(dt_str)
    except ValueError:
        logger.error(f"无效的时间格式：{dt_str}，请使用ISO格式（如 2024-01-01T00:00:00Z）")
        return None

def clean_location(raw_loc: str) -> str:
    """初步清洗位置字符串（去空格、统一格式）"""
    if not raw_loc:
        return ""
    # 去除多余空格、特殊字符
    clean_loc = re.sub(r"\s+", " ", raw_loc.strip()).strip()
    # 统一国家/地区格式（示例）
    clean_loc = clean_loc.replace("CN", "China").replace("US", "United States")
    return clean_loc

def handle_rate_limit(response: requests.Response) -> Tuple[int, Optional[datetime]]:
    """解析速率限制信息，返回剩余请求数和重置时间"""
    remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
    reset_ts = int(response.headers.get("X-RateLimit-Reset", 0))
    reset_time = datetime.fromtimestamp(reset_ts, tz=timezone.utc) if reset_ts else None
    return remaining, reset_time

def rotate_tokens(tokens: List[str]) -> Iterator[str]:
    """Token轮询生成器"""
    idx = 0
    while True:
        yield tokens[idx]
        idx = (idx + 1) % len(tokens)

# ======================== 缓存管理（新增过期机制） ========================
def load_cache(filepath: str, ttl_days: int) -> ContributorCache:
    """加载缓存并清理过期条目"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logger.info(f"缓存文件 {filepath} 不存在或损坏，创建新缓存")
        return {}

    # 清理过期条目
    current_ts = int(time.time())
    valid_cache = {}
    expired_count = 0
    for user_id, entry in cache.items():
        if isinstance(entry, dict) and entry.get("expire_at", 0) > current_ts:
            valid_cache[user_id] = entry
        else:
            expired_count += 1

    if expired_count > 0:
        logger.info(f"清理了 {expired_count} 条过期缓存")
        save_cache(valid_cache, filepath)

    return valid_cache

def save_cache(cache_data: ContributorCache, filepath: str):
    """保存缓存到文件"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
        logger.info(f"缓存已保存到 {filepath}")
    except IOError as e:
        logger.error(f"保存缓存失败：{e}")

def update_cache(
    cache: ContributorCache,
    user_id: str,
    location: str,
    ttl_days: int = DEFAULT_CONFIG["CACHE_TTL_DAYS"]
):
    """更新缓存（带过期时间）"""
    expire_at = int(time.time()) + (ttl_days * 86400)
    cache[user_id] = {
        "location": location,
        "expire_at": expire_at
    }

# ======================== GitHub API 通用工具 ========================
def create_retry_session() -> requests.Session:
    """创建带重试机制的Session"""
    retry_strategy = Retry(
        total=DEFAULT_CONFIG["RETRY_TIMES"],
        backoff_factor=DEFAULT_CONFIG["RETRY_DELAY"],
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.timeout = DEFAULT_CONFIG["REQUEST_TIMEOUT"]
    return session

def fetch_paginated_data(
    session: requests.Session,
    base_url: str,
    params: Dict[str, Any],
    token_iter: Iterator[str]
) -> List[Dict[str, Any]]:
    """通用分页数据获取函数"""
    all_data = []
    current_url = base_url
    page = 1

    while current_url:
        try:
            # 轮询Token
            token = next(token_iter)
            session.headers.update({"Authorization": f"token {token}"})
            
            response = session.get(current_url, params=params if page == 1 else None)
            remaining, reset_time = handle_rate_limit(response)
            
            # 打印速率限制信息
            logger.info(f"API速率限制剩余：{remaining} 次，重置时间：{reset_time.strftime('%Y-%m-%d %H:%M:%S UTC') if reset_time else '未知'}")

            # 处理速率限制
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", DEFAULT_CONFIG["RATE_LIMIT_SLEEP"]))
                logger.warning(f"触发速率限制，将休眠 {retry_after} 秒")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            if not data:
                break

            all_data.extend(data)
            logger.info(f"获取第 {page} 页数据，共 {len(data)} 条，累计 {len(all_data)} 条")

            # 解析下一页URL
            current_url = None
            if "link" in response.headers:
                link_header = response.headers["link"]
                match = re.search(r'<(.*?)>; rel="next"', link_header)
                if match:
                    current_url = match.group(1)

            page += 1
            if current_url:
                time.sleep(0.5)  # 避免高频请求
            params = None  # 后续页面URL已包含参数

        except requests.exceptions.RequestException as e:
            logger.error(f"获取分页数据失败（第{page}页）：{e}")
            break

    return all_data

# ======================== 数据获取与转换 ========================
def fetch_github_commits_mock(
    owner: str,
    repo: str,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    filter_keyword: Optional[str] = None,
    filter_author: Optional[str] = None
) -> List[RawCommit]:
    """增强版模拟数据生成（支持时间/关键词/作者过滤）"""
    num_commits = random.randint(50, 150)
    mock_users = {
        "user_a": {"location": "San Francisco, CA"},
        "user_b": {"location": "Beijing, China"},
        "user_c": {"location": "Frankfurt, Germany"},
        "user_d": {"location": "Mars"},
        "user_e": {"location": "Mumbai, India"}
    }
    user_logins = list(mock_users.keys())

    # 时间范围默认值
    since = since or (datetime.now(timezone.utc) - timedelta(days=90))
    until = until or datetime.now(timezone.utc)

    mock_data = []
    for i in range(num_commits):
        login = random.choice(user_logins)
        raw_location = mock_users[login]["location"]
        
        # 随机时间（在指定范围内）
        ts_start = int(since.timestamp())
        ts_end = int(until.timestamp())
        ts = random.randint(ts_start, ts_end)
        
        commit_msg = f"feat: add feature {i}"
        if filter_keyword and filter_keyword not in commit_msg:
            continue
        if filter_author and filter_author != login:
            continue

        mock_data.append({
            "sha": f"mocksha{i}",
            "author": {"login": login},
            "commit": {
                "author": {"date": datetime.fromtimestamp(ts, timezone.utc).isoformat()},
                "message": commit_msg,
            },
            "raw_location": raw_location
        })
    
    return mock_data

def fetch_contributor_location(
    user_id: str,
    session: requests.Session,
    cache: ContributorCache,
    token_iter: Iterator[str],
    is_mock: bool = False
) -> str:
    """获取贡献者位置（支持模拟/真实，带缓存）"""
    # 优先使用缓存
    if user_id in cache:
        return cache[user_id]["location"]

    if is_mock:
        # 模拟模式随机生成位置
        mock_locs = ["San Francisco, CA", "Beijing, China", "Frankfurt", "Mars", "India"]
        location = random.choice(mock_locs)
        update_cache(cache, user_id, location)
        return location

    # 真实API调用
    url = f"{DEFAULT_CONFIG['GITHUB_API_BASE']}/users/{user_id}"
    try:
        token = next(token_iter)
        session.headers.update({"Authorization": f"token {token}"})
        
        response = session.get(url)
        response.raise_for_status()
        user_data = response.json()
        location = clean_location(user_data.get("location", ""))
        
        update_cache(cache, user_id, location)
        return location
    except requests.exceptions.RequestException as e:
        logger.error(f"获取用户 {user_id} 位置失败：{e}")
        return ""

def transform_commit_data(
    raw_commits: List[RawCommit],
    contributor_locations: Dict[str, str],
    filter_keyword: Optional[str] = None,
    filter_author: Optional[str] = None
) -> List[OptimizedCommitData]:
    """转换提交数据结构（新增过滤逻辑）"""
    optimized_data = []

    for commit in raw_commits:
        author_login = commit.get("author", {}).get("login")
        commit_sha = commit.get("sha", "")
        commit_msg = commit.get("commit", {}).get("message", "")
        timestamp_str = commit.get("commit", {}).get("author", {}).get("date")

        # 过滤逻辑
        if not author_login or not timestamp_str:
            continue
        if filter_keyword and filter_keyword not in commit_msg:
            continue
        if filter_author and filter_author != author_login:
            continue

        # 时间转换
        try:
            dt_obj = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            timestamp_unix = int(dt_obj.timestamp())
        except ValueError:
            logger.warning(f"无效时间格式：{timestamp_str}，跳过该提交")
            continue

        # 构建优化数据
        optimized_data.append({
            "timestamp_unix": timestamp_unix,
            "raw_location": clean_location(contributor_locations.get(author_login, "")),
            "location_iso3": "NEED_LLM_CLEANING",
            "contributor_id": author_login,
            "commit_sha": commit_sha,
            "commit_message": commit_msg.strip()
        })

    return optimized_data

def collect_project_data(
    project_full_name: str,
    session: requests.Session,
    token_iter: Iterator[str],
    is_mock: bool = True,
    contributor_cache: Optional[ContributorCache] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    filter_keyword: Optional[str] = None,
    filter_author: Optional[str] = None
) -> List[OptimizedCommitData]:
    """增强版项目数据收集函数"""
    parts = project_full_name.split("/")
    if len(parts) != 2:
        logger.error(f"项目名称格式错误：{project_full_name}（应为 owner/repo）")
        return []
    owner, repo = parts

    # 1. 设置时间范围默认值
    since = since or (datetime.now(timezone.utc) - timedelta(days=DEFAULT_CONFIG["DEFAULT_SINCE_DAYS"]))
    until = until or datetime.now(timezone.utc)

    # 2. 获取原始提交数据
    all_raw_commits: List[RawCommit] = []
    if is_mock:
        all_raw_commits = fetch_github_commits_mock(
            owner, repo, since=since, until=until,
            filter_keyword=filter_keyword, filter_author=filter_author
        )
        logger.info(f"模拟模式：生成 {len(all_raw_commits)} 条提交数据")
    else:
        base_url = f"{DEFAULT_CONFIG['GITHUB_API_BASE']}/repos/{owner}/{repo}/commits"
        params = {
            "per_page": DEFAULT_CONFIG["PER_PAGE"],
            "since": since.isoformat().replace("+00:00", "Z"),
            "until": until.isoformat().replace("+00:00", "Z")
        }
        logger.info(f"开始获取 {project_full_name} 提交数据（{since} 至 {until}）")
        all_raw_commits = fetch_paginated_data(session, base_url, params, token_iter)

    if not all_raw_commits:
        logger.warning(f"{project_full_name} 未获取到任何提交数据")
        return []

    # 3. 获取贡献者位置
    unique_contributors = {commit.get("author", {}).get("login") for commit in all_raw_commits if commit.get("author", {}).get("login")}
    unique_contributors.discard(None)
    logger.info(f"{project_full_name} 共有 {len(unique_contributors)} 个唯一贡献者")

    contributor_locations = {}
    cache = contributor_cache or {}
    for user_id in unique_contributors:
        location = fetch_contributor_location(
            user_id, session, cache, token_iter, is_mock=is_mock
        )
        contributor_locations[user_id] = location

    # 4. 转换并过滤数据
    optimized_data = transform_commit_data(
        all_raw_commits, contributor_locations,
        filter_keyword=filter_keyword, filter_author=filter_author
    )
    logger.info(f"{project_full_name} 转换后得到 {len(optimized_data)} 条有效数据")

    return optimized_data

# ======================== 输出与统计 ========================
def write_json_output(data: Dict[str, List[OptimizedCommitData]], filepath: str):
    """写入JSON格式输出"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"JSON数据已保存到 {filepath}")
    except IOError as e:
        logger.error(f"写入JSON失败：{e}")
        sys.exit(1)

def write_csv_output(data: Dict[str, List[OptimizedCommitData]], filepath: str):
    """写入CSV格式输出"""
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                "project", "timestamp_unix", "raw_location",
                "location_iso3", "contributor_id", "commit_sha", "commit_message"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for project, commits in data.items():
                for commit in commits:
                    row = {"project": project, **commit}
                    writer.writerow(row)
        logger.info(f"CSV数据已保存到 {filepath}")
    except IOError as e:
        logger.error(f"写入CSV失败：{e}")
        sys.exit(1)

def calculate_project_stats(data: Dict[str, List[OptimizedCommitData]]) -> Dict[str, ProjectStats]:
    """计算项目统计信息"""
    stats = {}
    for project, commits in data.items():
        # 基础统计
        total_commits = len(commits)
        unique_contributors = len({c["contributor_id"] for c in commits})
        
        # 位置分布
        location_dist = {}
        for commit in commits:
            loc = commit["raw_location"] or "未知"
            location_dist[loc] = location_dist.get(loc, 0) + 1
        
        # 时间分布（按天）
        daily_dist = {}
        for commit in commits:
            dt = datetime.fromtimestamp(commit["timestamp_unix"]).strftime("%Y-%m-%d")
            daily_dist[dt] = daily_dist.get(dt, 0) + 1

        stats[project] = {
            "total_commits": total_commits,
            "unique_contributors": unique_contributors,
            "location_distribution": location_dist,
            "daily_commit_distribution": daily_dist
        }
    return stats

def write_stats_report(stats: Dict[str, ProjectStats], filepath: str):
    """写入统计报告"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            # 生成易读的文本报告
            report = []
            report.append("=== GitHub 项目提交统计报告 ===\n")
            for project, project_stats in stats.items():
                report.append(f"\n【{project}】")
                report.append(f"总提交数：{project_stats['total_commits']}")
                report.append(f"唯一贡献者数：{project_stats['unique_contributors']}")
                report.append("\n位置分布：")
                for loc, count in sorted(project_stats['location_distribution'].items(), key=lambda x: x[1], reverse=True):
                    report.append(f"  {loc}: {count} ({count/project_stats['total_commits']*100:.1f}%)")
            f.write("\n".join(report))
        
        # 同时保存JSON格式的统计数据
        json_filepath = filepath.replace(".txt", ".json") if filepath.endswith(".txt") else f"{filepath}.json"
        with open(json_filepath, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"统计报告已保存到 {filepath} 和 {json_filepath}")
    except IOError as e:
        logger.error(f"写入统计报告失败：{e}")

# ======================== 主程序 ========================
def main():
    parser = argparse.ArgumentParser(
        description="增强版 GitHub 项目提交数据收集工具",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # 基础参数
    parser.add_argument("--projects", required=True, nargs='+',
                        help="项目列表（owner/repo 格式，空格分隔），例如：google/gtest tensorflow/tensorflow")
    parser.add_argument("--output", required=True,
                        help="输出文件路径（支持.json/.csv）")
    parser.add_argument("--mock", action='store_true',
                        help="使用模拟数据（跳过真实API调用，用于测试）")
    
    # 拓展参数
    parser.add_argument("--since",
                        help="起始时间（ISO格式，如 2024-01-01T00:00:00Z），默认90天前")
    parser.add_argument("--until",
                        help="结束时间（ISO格式，如 2024-04-01T00:00:00Z），默认当前时间")
    parser.add_argument("--filter-keyword",
                        help="过滤提交信息包含指定关键词的提交")
    parser.add_argument("--filter-author",
                        help="过滤指定作者的提交（login名）")
    parser.add_argument("--tokens",
                        help="多个GitHub Token（逗号分隔），用于轮询突破速率限制")
    parser.add_argument("--cache-ttl", type=int, default=DEFAULT_CONFIG["CACHE_TTL_DAYS"],
                        help="缓存有效期（天），默认7天")
    parser.add_argument("--stats",
                        help="统计报告输出路径（如 stats.txt），不指定则不生成")

    args = parser.parse_args()

    # 1. 初始化
    session = create_retry_session()
    tokens = get_github_tokens(args.tokens)
    token_iter = rotate_tokens(tokens)
    contributor_cache = load_cache(DEFAULT_CONFIG["CACHE_FILEPATH"], args.cache_ttl)

    # 2. 验证Token（真实模式下）
    if not args.mock:
        if tokens[0] == "YOUR_GITHUB_PERSONAL_ACCESS_TOKEN":
            logger.error("真实模式下必须设置有效的GITHUB_TOKEN（环境变量或--tokens参数）")
            sys.exit(1)
        session.headers.update({"Accept": "application/vnd.github.v3+json"})
        logger.info(f"使用 {len(tokens)} 个GitHub Token轮询请求")

    # 3. 解析时间参数
    since = validate_datetime_str(args.since) if args.since else None
    until = validate_datetime_str(args.until) if args.until else None
    if (args.since and not since) or (args.until and not until):
        sys.exit(1)

    # 4. 收集所有项目数据
    all_projects_data: Dict[str, List[OptimizedCommitData]] = {}
    logger.info(f"\n开始收集 {len(args.projects)} 个项目的数据（{'模拟' if args.mock else '真实'}模式）")

    for project in args.projects:
        logger.info(f"\n========== 处理项目：{project} ==========")
        project_data = collect_project_data(
            project, session, token_iter,
            is_mock=args.mock,
            contributor_cache=contributor_cache,
            since=since,
            until=until,
            filter_keyword=args.filter_keyword,
            filter_author=args.filter_author
        )
        if project_data:
            all_projects_data[project] = project_data
        # 多项目时增加间隔
        if not args.mock and len(args.projects) > 1:
            time.sleep(1)

    # 5. 输出数据
    if not all_projects_data:
        logger.error("未收集到任何有效数据")
        sys.exit(1)

    # 选择输出格式
    if args.output.endswith(".csv"):
        write_csv_output(all_projects_data, args.output)
    else:
        write_json_output(all_projects_data, args.output)

    # 6. 保存缓存（真实模式下）
    if not args.mock:
        save_cache(contributor_cache, DEFAULT_CONFIG["CACHE_FILEPATH"])

    # 7. 生成统计报告
    if args.stats:
        stats = calculate_project_stats(all_projects_data)
        write_stats_report(stats, args.stats)

    logger.info("\n========== 数据收集完成 ==========")
    logger.info(f"总处理项目数：{len(all_projects_data)}")
    total_commits = sum(len(commits) for commits in all_projects_data.values())
    logger.info(f"总有效提交数：{total_commits}")
    logger.info("下一步：使用LLM工具清洗raw_location字段并填充location_iso3")

if __name__ == "__main__":
    main()
