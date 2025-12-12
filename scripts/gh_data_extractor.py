# -*- coding: utf-8 -*-
import requests
import json
import time
import sys
import random
import argparse
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple
from pathlib import Path
from contextlib import contextmanager
from functools import lru_cache

# --- 配置类（替代硬编码常量）---
@dataclass(frozen=True)
class Config:
    """配置类，集中管理所有配置项"""
    github_api_base: str = "https://api.github.com"
    github_token: str = ""
    per_page: int = 100
    days_to_fetch: int = 90
    rate_limit_sleep: float = 1.0
    max_retries: int = 3
    retry_delay: float = 2.0

# --- 数据结构定义（使用Dataclass增强类型安全）---
@dataclass
class OptimizedCommitData:
    """优化后的提交数据结构"""
    timestamp_unix: int
    raw_location: str
    location_iso3: str = "NEED_LLM_CLEANING"
    contributor_id: str

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)

# --- 工具函数 ---
@contextmanager
def error_handler(operation: str, default: Any = None):
    """通用错误处理上下文管理器"""
    try:
        yield
    except Exception as e:
        print(f"错误: {operation} 失败 - {str(e)}", file=sys.stderr)
        return default

def setup_session(config: Config) -> requests.Session:
    """创建并配置requests会话"""
    session = requests.Session()
    if config.github_token:
        session.headers.update({
            'Authorization': f'token {config.github_token}',
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'GitHub-Commit-Collector/1.0'
        })
    # 设置超时
    session.timeout = 10
    return session

def exponential_backoff(retry_num: int, base_delay: float = 2.0) -> float:
    """指数退避算法"""
    return base_delay * (2 ** retry_num) + random.uniform(0, 1)

# --- API调用相关函数 ---
def fetch_with_retry(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    config: Config = Config()
) -> Optional[Dict[str, Any]]:
    """带重试机制的API请求"""
    for retry in range(config.max_retries):
        try:
            response = session.get(url, params=params)
            
            # 处理速率限制
            if response.status_code == 403 and 'rate limit' in response.text.lower():
                reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
                sleep_time = max(reset_time - time.time(), 1)
                print(f"警告: 触发GitHub速率限制，将等待 {sleep_time:.1f} 秒", file=sys.stderr)
                time.sleep(sleep_time)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if retry < config.max_retries - 1:
                sleep_time = exponential_backoff(retry, config.retry_delay)
                print(f"警告: 请求失败 (重试 {retry+1}/{config.max_retries}) - {e}，将等待 {sleep_time:.1f} 秒", file=sys.stderr)
                time.sleep(sleep_time)
                continue
            print(f"错误: 请求失败（已重试{config.max_retries}次）- {e}", file=sys.stderr)
            return None

def fetch_paginated_data(
    session: requests.Session,
    base_url: str,
    params: Dict[str, Any],
    config: Config = Config()
) -> List[Dict[str, Any]]:
    """获取分页的GitHub API数据"""
    all_data = []
    page = 1
    
    while True:
        params['page'] = page
        data = fetch_with_retry(session, base_url, params, config)
        
        if not data:
            break
            
        all_data.extend(data)
        
        # 检查是否还有更多页面
        if len(data) < config.per_page:
            break
            
        page += 1
        time.sleep(config.rate_limit_sleep)
        
    return all_data

def fetch_github_commits_mock(owner: str, repo: str) -> List[Dict[str, Any]]:
    """模拟GitHub提交数据获取"""
    num_commits = random.randint(50, 150)
    mock_users = {
        "user_a": {"location": "San Francisco, CA"},
        "user_b": {"location": "Beijing, China"},
        "user_c": {"location": "Frankfurt"},
        "user_d": {"location": "Planet Mars"},
        "user_e": {"location": "India"}
    }
    user_logins = list(mock_users.keys())
    mock_data = []

    for i in range(num_commits):
        login = random.choice(user_logins)
        ts = int(time.time()) - random.randint(86400, 86400 * 30)
        
        mock_data.append({
            "sha": f"mocksha{i}",
            "author": {"login": login},
            "commit": {
                "author": {"date": datetime.fromtimestamp(ts).isoformat() + "Z"},
                "message": f"feat: add feature {i}",
            },
            "raw_location": mock_users[login]["location"]
        })
        
    return mock_data

@lru_cache(maxsize=1000)
def fetch_contributor_location_cached(
    contributor_id: str,
    session: requests.Session,
    config: Config = Config()
) -> str:
    """获取贡献者位置（带缓存）"""
    url = f"{config.github_api_base}/users/{contributor_id}"
    user_data = fetch_with_retry(session, url, config=config)
    return user_data.get('location', '') if user_data else ''

# --- 核心转换逻辑 ---
def parse_github_timestamp(timestamp_str: str) -> int:
    """解析GitHub时间戳为Unix秒数"""
    try:
        dt_obj = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return int(dt_obj.timestamp())
    except (ValueError, AttributeError):
        return 0

def get_unique_contributors(raw_commits: List[Dict[str, Any]]) -> Set[str]:
    """提取唯一贡献者ID"""
    contributors = set()
    for commit in raw_commits:
        login = commit.get('author', {}).get('login')
        if login:
            contributors.add(login)
    return contributors

def collect_project_data(
    project_full_name: str,
    session: requests.Session,
    config: Config,
    is_mock: bool = True
) -> List[OptimizedCommitData]:
    """
    从GitHub API获取项目提交数据并转换为优化结构
    
    Args:
        project_full_name: 项目名称（owner/repo）
        session: requests会话对象
        config: 配置对象
        is_mock: 是否使用模拟数据
        
    Returns:
        优化后的提交数据列表
    """
    # 验证项目名称格式
    parts = project_full_name.split('/')
    if len(parts) != 2:
        print(f"错误: 无效的项目名称格式 '{project_full_name}'", file=sys.stderr)
        return []
    owner, repo = parts

    # 1. 获取提交数据
    if is_mock:
        raw_commits = fetch_github_commits_mock(owner, repo)
    else:
        url = f"{config.github_api_base}/repos/{owner}/{repo}/commits"
        since_date = (datetime.now() - timedelta(days=config.days_to_fetch)).isoformat() + 'Z'
        params = {
            'per_page': config.per_page,
            'since': since_date
        }
        
        print(f"-> 正在从 {project_full_name} 获取提交数据（最近{config.days_to_fetch}天）...")
        raw_commits = fetch_paginated_data(session, url, params, config)
    
    if not raw_commits:
        print(f"-> {project_full_name}: 未获取到提交数据", file=sys.stderr)
        return []

    print(f"-> {project_full_name}: 成功获取 {len(raw_commits)} 条提交记录")

    # 2. 获取唯一贡献者及其位置
    unique_contributors = get_unique_contributors(raw_commits)
    print(f"-> {project_full_name}: 正在查询 {len(unique_contributors)} 个独特贡献者的位置...")
    
    contributor_locations: Dict[str, str] = {}
    for user_id in unique_contributors:
        if is_mock:
            # 从模拟数据中获取位置
            mock_loc = next(
                (c.get('raw_location') for c in raw_commits 
                 if c.get('author', {}).get('login') == user_id), 
                ''
            )
            contributor_locations[user_id] = mock_loc
        else:
            # 真实模式下使用缓存获取位置
            contributor_locations[user_id] = fetch_contributor_location_cached(
                user_id, session, config
            )
            time.sleep(config.rate_limit_sleep / 2)  # 减轻API压力

    # 3. 转换数据结构
    optimized_data: List[OptimizedCommitData] = []
    for commit in raw_commits:
        author_data = commit.get('author', {})
        commit_data = commit.get('commit', {})
        
        contributor_id = author_data.get('login')
        timestamp_str = commit_data.get('author', {}).get('date')
        
        if contributor_id and timestamp_str:
            timestamp_unix = parse_github_timestamp(timestamp_str)
            raw_location = contributor_locations.get(contributor_id, '')
            
            optimized_data.append(OptimizedCommitData(
                timestamp_unix=timestamp_unix,
                raw_location=raw_location,
                contributor_id=contributor_id
            ))
    
    return optimized_data

# --- 主程序 ---
def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description="GitHub开源项目提交数据收集与结构转换工具",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "--projects",
        required=True,
        nargs='+',
        help="要收集的项目列表，格式为 'owner/repo'，例如:\n  --projects google/gtest tensorflow/tensorflow"
    )
    
    parser.add_argument(
        "--output",
        required=True,
        help="输出JSON文件路径"
    )
    
    parser.add_argument(
        "--mock",
        action='store_true',
        help="使用模拟数据模式（测试用）"
    )
    
    parser.add_argument(
        "--token",
        help="GitHub Personal Access Token (优先级高于代码内配置)"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="要获取的提交数据天数范围（默认90天）"
    )
    
    args = parser.parse_args()

    # 初始化配置
    github_token = args.token or "YOUR_GITHUB_PERSONAL_ACCESS_TOKEN"
    config = Config(
        github_token=github_token,
        days_to_fetch=args.days
    )

    # 验证配置
    if not args.mock and config.github_token == "YOUR_GITHUB_PERSONAL_ACCESS_TOKEN":
        print("错误: 请通过 --token 参数或修改代码配置GitHub Token", file=sys.stderr)
        sys.exit(1)

    # 设置会话
    session = setup_session(config)
    
    # 收集数据
    print(f"\n--- 开始收集 {len(args.projects)} 个项目的数据 ({'模拟' if args.mock else '真实'} 模式) ---")
    print(f"--- 时间范围: 最近 {config.days_to_fetch} 天 ---")
    
    all_projects_data: Dict[str, List[Dict[str, Any]]] = {}
    
    for idx, project in enumerate(args.projects, 1):
        print(f"\n[{idx}/{len(args.projects)}] 处理项目: {project}")
        
        with error_handler(f"处理项目 {project}"):
            project_data = collect_project_data(project, session, config, args.mock)
            
            if project_data:
                # 转换为字典列表
                all_projects_data[project] = [item.to_dict() for item in project_data]
                print(f"-> 成功转换 {len(project_data)} 条记录")
        
        # 速率限制控制
        if not args.mock and idx < len(args.projects):
            time.sleep(config.rate_limit_sleep)

    # 保存数据
    if all_projects_data:
        output_path = Path(args.output)
        try:
            # 创建输出目录
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(all_projects_data, f, indent=2, ensure_ascii=False)
            
            print(f"\n--- 收集完成 ---")
            print(f"-> 数据已保存到: {output_path.absolute()}")
            print(f"-> 总计处理 {sum(len(v) for v in all_projects_data.values())} 条记录")
            print("下一步：使用 llm_geo_cleaner.py 清洗 raw_location 字段并填充 location_iso3")
            
        except IOError as e:
            print(f"错误: 无法写入输出文件 - {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("\n警告: 未收集到任何有效数据", file=sys.stderr)
        sys.exit(0)

if __name__ == "__main__":
    main()
