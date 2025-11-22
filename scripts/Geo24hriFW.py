# -*- coding: utf-8 -*-

import requests
import json
import time
import sys
from datetime import datetime, timedelta # 修复：新增 timedelta 导入
import random
import argparse
from typing import List, Dict, Any, Optional

# --- 配置 ---
GITHUB_API_BASE = "https://api.github.com"
# !!! 替换为你的 GitHub Personal Access Token !!!
# 拥有 repo 权限的 Token 可以显著提高 API 速率限制。
GITHUB_TOKEN = "YOUR_GITHUB_PERSONAL_ACCESS_TOKEN" 

# --- 数据结构定义 (优化后的目标结构) ---
OptimizedCommitData = Dict[str, Any]
CACHE_FILEPATH = "gh_contributor_location_cache.json"

# --- 缓存管理 ---

def load_cache(filepath: str) -> Dict[str, str]:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_cache(cache_data: Dict[str, str], filepath: str):
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
    except IOError as e:
        print(f"警告: 无法保存缓存文件 {filepath}. {e}", file=sys.stderr)

# --- 辅助函数：模拟 GitHub API 返回（用于演示） ---
def fetch_github_commits_mock(owner: str, repo: str) -> List[Dict[str, Any]]:
    
    num_commits = random.randint(50, 150)
    mock_data = []
    
    
    mock_users = {
        "user_a": {"location": "San Francisco, CA"},
        "user_b": {"location": "Beijing, China"},
        "user_c": {"location": "Frankfurt"},
        "user_d": {"location": "Planet Mars"}, 
        "user_e": {"location": "India"}
    }
    user_logins = list(mock_users.keys())

    for i in range(num_commits):
        login = random.choice(user_logins)
        raw_location = mock_users[login]["location"]
        
        
        ts = int(time.time()) - random.randint(86400, 86400 * 30)
        
        mock_data.append({
            "sha": f"mocksha{i}",
            "author": {"login": login},
            "commit": {
                "author": {
                    "date": datetime.fromtimestamp(ts).isoformat() + "Z"
                },
                "message": f"feat: add feature {i}",
            },
            
            "raw_location": raw_location 
        })
        
    return mock_data

def fetch_contributor_location_real(contributor_id: str, session: requests.Session, cache: Dict[str, str]) -> str:
    
    if contributor_id in cache:
        return cache[contributor_id] 
    
    url = f"{GITHUB_API_BASE}/users/{contributor_id}"
    try:
        response = session.get(url)
        response.raise_for_status()
        user_data = response.json()
        location = user_data.get('location', '') if user_data.get('location') else ''
        
        cache[contributor_id] = location # 更新缓存
        return location
    except requests.exceptions.RequestException as e:
        print(f"警告: 获取贡献者 {contributor_id} 位置失败: {e}", file=sys.stderr)
        return ''

# --- 核心转换逻辑 ---

def collect_project_data(
    project_full_name: str, 
    session: requests.Session,
    is_mock: bool = True,
    contributor_cache: Optional[Dict[str, str]] = None
) -> List[OptimizedCommitData]:
    
    
    parts = project_full_name.split('/')
    if len(parts) != 2:
        print(f"错误: 项目名称格式错误 '{project_full_name}'", file=sys.stderr)
        return []
        
    owner, repo = parts
    all_raw_commits = []

    # 1. 获取提交数据（支持分页）
    if is_mock:
        all_raw_commits = fetch_github_commits_mock(owner, repo)
    else:
        # --- 真实 API 调用 (实现分页处理) ---
        url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/commits"
        params = {
            'per_page': 100,
            'since': (datetime.now() - timedelta(days=90)).isoformat() + 'Z' 
        }
        
        print(f"-> 正在从 {project_full_name} 获取提交数据（最近 90 天）...")
        
        while url:
            try:
                response = session.get(url, params=params if url == f"{GITHUB_API_BASE}/repos/{owner}/{repo}/commits" else None)
                response.raise_for_status()
                raw_commits = response.json()
                
                if not raw_commits:
                    break
                    
                all_raw_commits.extend(raw_commits)
                
                # 处理分页链接
                if 'link' in response.headers:
                    links = response.headers['link']
                    next_url = None
                    for link in links.split(','):
                        if 'rel="next"' in link:
                            next_url = link.split(';')[0].strip('<> ')
                            break
                    url = next_url
                else:
                    url = None
                
                if url:
                    # 暂停以避免速率限制
                    time.sleep(0.5) 
                    params = None # 下一页的 URL 已包含所有参数，无需重复发送 params
                
            except requests.exceptions.RequestException as e:
                print(f"警告: 无法获取 {project_full_name} 提交数据（分页中断）: {e}", file=sys.stderr)
                url = None
    
    if not all_raw_commits:
        return []

    print(f"-> 成功获取 {len(all_raw_commits)} 条提交记录。")

    # 2. 收集所有独特的贡献者 ID
    contributor_locations = {}
    unique_contributors = set()
    for commit in all_raw_commits:
        # 确保作者信息有效
        author_login = commit.get('author', {}).get('login')
        if author_login:
            unique_contributors.add(author_login)

    # 3. 获取每个独特贡献者的位置 (使用缓存和会话)
    print(f"-> 正在查询 {len(unique_contributors)} 个独特贡献者的位置（使用缓存）...")
    
    cache = contributor_cache if contributor_cache is not None else {}
    
    for user_id in unique_contributors:
        if is_mock:
            
            mock_loc = next((c.get('raw_location') for c in all_raw_commits if c.get('author', {}).get('login') == user_id), '')
            contributor_locations[user_id] = mock_loc if mock_loc else ''
        else:
            # 真实模式：调用带缓存的函数
            location = fetch_contributor_location_real(user_id, session, cache)
            contributor_locations[user_id] = location
    
    # 4. 转换数据结构
    optimized_data: List[OptimizedCommitData] = []
    for commit in all_raw_commits:
        author_data = commit.get('author', {})
        commit_data = commit.get('commit', {})
        
        
        contributor_id = author_data.get('login')
        timestamp_str = commit_data.get('author', {}).get('date')
        
        if contributor_id and timestamp_str:
            try:
                
                dt_obj = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                timestamp_unix = int(dt_obj.timestamp())
            except ValueError:
                timestamp_unix = 0 
                
            raw_location = contributor_locations.get(contributor_id, '')
            
            optimized_data.append({
                "timestamp_unix": timestamp_unix,
                "raw_location": raw_location,
                
                "location_iso3": "NEED_LLM_CLEANING", 
                "contributor_id": contributor_id
            })
            
    return optimized_data

# --- 主程序和命令行界面 ---

def main():
    parser = argparse.ArgumentParser(
        description="GitHub 开源项目提交数据收集与结构转换工具。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "--projects",
        required=True,
        nargs='+',
        help="要收集的项目列表，以空格分隔。格式为 'owner/repo'，例如: 'google/gtest tensorflow/tensorflow'"
    )
    
    parser.add_argument(
        "--output",
        required=True,
        help="输出 JSON 文件路径。转换后的数据将保存到此文件。"
    )
    
    parser.add_argument(
        "--mock",
        action='store_true',
        help="使用模拟数据模式（推荐用于测试），跳过真实的 GitHub API 调用。"
    )
    
    args = parser.parse_args()

    # 1. 设置 Requests Session 和加载缓存
    session = requests.Session()
    contributor_cache = load_cache(CACHE_FILEPATH)
    
    if not args.mock and GITHUB_TOKEN == "YOUR_GITHUB_PERSONAL_ACCESS_TOKEN":
         print("错误: 请设置 GITHUB_TOKEN 以进行真实的 API 调用。", file=sys.stderr)
         sys.exit(1)
         
    if not args.mock:
        session.headers.update({
            'Authorization': f'token {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        })
        print("注意: 正在使用真实 API 模式。请注意 GitHub 的速率限制。")
        
    all_projects_data: Dict[str, List[OptimizedCommitData]] = {}
    
    # 2. 遍历项目并收集数据
    print(f"\n--- 开始收集 {len(args.projects)} 个项目的数据 ({'模拟' if args.mock else '真实'} 模式) ---")

    for project in args.projects:
        print(f"\n[项目] {project}")
        project_data = collect_project_data(project, session, is_mock=args.mock, contributor_cache=contributor_cache)
        
        if project_data:
            all_projects_data[project] = project_data
        
        
        if not args.mock and len(args.projects) > 1:
            time.sleep(1) 

    # 3. 保存到文件并保存缓存
    if all_projects_data:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(all_projects_data, f, indent=2, ensure_ascii=False)
            print(f"\n--- 成功完成收集。数据已保存到 {args.output} ---")
            
            if not args.mock:
                save_cache(contributor_cache, CACHE_FILEPATH)
                print(f"贡献者位置缓存已更新到 {CACHE_FILEPATH}")
                
            print("下一步：使用 llm_geo_cleaner.py 清洗 raw_location 字段并填充 location_iso3。")
        except IOError as e:
            print(f"错误：无法写入输出文件 {args.output}. {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("\n未收集到任何有效数据。", file=sys.stderr)

if __name__ == "__main__":
    main()
