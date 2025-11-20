# -*- coding: utf-8 -*-

import requests
import json
import time
import sys
from datetime import datetime
import random
import argparse
from typing import List, Dict, Any

# --- 配置 ---
GITHUB_API_BASE = "https://api.github.com"
# !!! 替换为你的 GitHub Personal Access Token !!!
# 拥有 repo 权限的 Token 可以显著提高 API 速率限制。
GITHUB_TOKEN = "YOUR_GITHUB_PERSONAL_ACCESS_TOKEN" 

# --- 数据结构定义 (优化后的目标结构) ---
OptimizedCommitData = Dict[str, Any]

# --- 辅助函数：模拟 GitHub API 返回（用于演示） ---
def fetch_github_commits_mock(owner: str, repo: str) -> List[Dict[str, Any]]:
    """
    模拟 GitHub /repos/{owner}/{repo}/commits 接口返回的数据。
    在实际应用中，你需要用真实的 requests.get 调用替换此函数。
    """
    
    # 模拟数据数量
    num_commits = random.randint(50, 150)
    mock_data = []
    
    # 模拟贡献者 ID 及其填写的原始位置
    # 实际上，location 需要单独通过 /users/{username} 接口获取
    mock_users = {
        "user_a": {"location": "San Francisco, CA"},
        "user_b": {"location": "Beijing, China"},
        "user_c": {"location": "Frankfurt"},
        "user_d": {"location": "Planet Mars"}, # 模拟无效位置
        "user_e": {"location": "India"}
    }
    user_logins = list(mock_users.keys())

    for i in range(num_commits):
        login = random.choice(user_logins)
        raw_location = mock_users[login]["location"]
        
        # 模拟 UTC 时间戳
        # 随机在过去 30 天内
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
            # 模拟添加 LLM 清洗后的数据，以便展示最终结构
            "raw_location": raw_location 
        })
        
    return mock_data

def fetch_contributor_location_real(contributor_id: str, session: requests.Session) -> str:
    """
    实际调用 GitHub API 获取单个贡献者的个人位置信息。
    """
    url = f"{GITHUB_API_BASE}/users/{contributor_id}"
    try:
        response = session.get(url)
        response.raise_for_status()
        user_data = response.json()
        return user_data.get('location', '') if user_data.get('location') else ''
    except requests.exceptions.RequestException as e:
        print(f"警告: 获取贡献者 {contributor_id} 位置失败: {e}", file=sys.stderr)
        return ''

# --- 核心转换逻辑 ---

def collect_project_data(
    project_full_name: str, 
    session: requests.Session,
    is_mock: bool = True
) -> List[OptimizedCommitData]:
    """
    从 GitHub API 获取一个项目的提交数据，并将其转换为优化的结构。
    
    Args:
        project_full_name (str): 项目名称，如 'tensorflow/tensorflow'。
        session (requests.Session): 带有认证信息的会话。
        is_mock (bool): 是否使用模拟数据模式。
        
    Returns:
        List[OptimizedCommitData]: 转换后的提交数据列表。
    """
    
    parts = project_full_name.split('/')
    if len(parts) != 2:
        print(f"错误: 项目名称格式错误 '{project_full_name}'", file=sys.stderr)
        return []
        
    owner, repo = parts

    # 1. 获取提交数据
    if is_mock:
        raw_commits = fetch_github_commits_mock(owner, repo)
    else:
        # --- 真实 API 调用 (需要分页处理) ---
        url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/commits"
        params = {
            'per_page': 100,
            'since': (datetime.now() - timedelta(days=90)).isoformat() + 'Z' # 只获取最近90天的
        }
        # 实际代码需要实现循环调用 API 并处理分页 links
        try:
            print(f"-> 正在从 {project_full_name} 获取提交数据...")
            response = session.get(url, params=params)
            response.raise_for_status()
            raw_commits = response.json()
        except requests.exceptions.RequestException as e:
            print(f"警告: 无法获取 {project_full_name} 提交数据: {e}", file=sys.stderr)
            return []
    
    if not raw_commits:
        return []

    print(f"-> 成功获取 {len(raw_commits)} 条提交记录。")

    # 2. 收集所有独特的贡献者 ID
    contributor_locations = {}
    unique_contributors = set()
    for commit in raw_commits:
        if commit.get('author') and commit['author'].get('login'):
            unique_contributors.add(commit['author']['login'])

    # 3. 获取每个独特贡献者的位置 (这是性能瓶颈)
    print(f"-> 正在查询 {len(unique_contributors)} 个独特贡献者的位置...")
    for user_id in unique_contributors:
        if is_mock:
            # 模拟模式下，我们从 mock commit data 中提取 location，或使用默认值
            # 真实模式下，我们必须调用 fetch_contributor_location_real
            # 这里的 raw_location 只是为了在 mock 模式下演示数据流
            mock_loc = next((c.get('raw_location') for c in raw_commits if c.get('author', {}).get('login') == user_id), '')
            contributor_locations[user_id] = mock_loc if mock_loc else ''
        else:
            location = fetch_contributor_location_real(user_id, session)
            contributor_locations[user_id] = location
    
    # 4. 转换数据结构
    optimized_data: List[OptimizedCommitData] = []
    for commit in raw_commits:
        author_data = commit.get('author', {})
        commit_data = commit.get('commit', {})
        
        # 提取关键字段
        contributor_id = author_data.get('login')
        timestamp_str = commit_data.get('author', {}).get('date')
        
        if contributor_id and timestamp_str:
            try:
                # 转换时间戳为 Unix 秒数
                dt_obj = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                timestamp_unix = int(dt_obj.timestamp())
            except ValueError:
                timestamp_unix = 0 # 无法解析则置零
                
            raw_location = contributor_locations.get(contributor_id, '')
            
            optimized_data.append({
                "timestamp_unix": timestamp_unix,
                "raw_location": raw_location,
                # !!! 注意: location_iso3 字段必须通过 LLM 清洗后才能填充 !!!
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

    # 1. 设置 Requests Session
    session = requests.Session()
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
        project_data = collect_project_data(project, session, is_mock=args.mock)
        
        if project_data:
            all_projects_data[project] = project_data
        
        # 真实 API 模式下，为了避免速率限制，最好暂停一下
        if not args.mock and len(args.projects) > 1:
            time.sleep(1) 

    # 3. 保存到文件
    if all_projects_data:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(all_projects_data, f, indent=2, ensure_ascii=False)
            print(f"\n--- 成功完成收集。数据已保存到 {args.output} ---")
            print("下一步：使用 llm_geo_cleaner.py 清洗 raw_location 字段并填充 location_iso3。")
        except IOError as e:
            print(f"错误：无法写入输出文件 {args.output}. {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("\n未收集到任何有效数据。", file=sys.stderr)

if __name__ == "__main__":
    main()
