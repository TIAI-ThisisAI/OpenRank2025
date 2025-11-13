# -*- coding: utf-8 -*-

import math
import random
import json
import argparse
import sys
from collections import Counter
from datetime import datetime

def calculate_shannon_entropy(data: list) -> float:
    if not data:
        return 0.0

    n = len(data)
    counts = Counter(data)
    
    entropy = 0.0
    for count in counts.values():
        probability = count / n
        if probability > 0:
            entropy -= probability * math.log2(probability)
            
    return entropy

def calculate_24hri_raw(contribution_hours: list) -> float:
    if not contribution_hours:
        return 0.0
    
    n = len(contribution_hours)
    counts = Counter(contribution_hours)
    
    probabilities = []
    for hour in range(24):
        probability = counts.get(hour, 0) / n
        probabilities.append(probability)
        
    entropy = -sum(p * math.log2(p) for p in probabilities if p > 0)
    
    return entropy

def calculate_geo_diversity_raw(contributor_locations: list) -> float:
    return calculate_shannon_entropy(contributor_locations)

def get_openrank_simulated(project_name: str) -> float:
    mock_rank = (hash(project_name) % 1000) / 50.0
    return mock_rank

class ProjectAnalyzer:
    
    MAX_24HRI_ENTROPY = math.log2(24)

    def __init__(self, project_name: str):
        self.project_name = project_name
        self.commit_hours = []
        self.locations = []
        self.unique_locations = set()
        self.total_contributions = 0

    def load_contributions(self, contributions_list: list):
        processed_hours = []
        processed_locations = []
        
        for item in contributions_list:
            try:
                ts = datetime.fromisoformat(item.get("timestamp", "").replace("Z", "+00:00"))
                processed_hours.append(ts.hour)
            except (ValueError, TypeError):
                pass
            
            location = item.get("location")
            if location:
                processed_locations.append(location)

        self.commit_hours = processed_hours
        self.locations = processed_locations
        self.unique_locations = set(processed_locations)
        self.total_contributions = len(contributions_list)
        
        if not self.commit_hours and not self.locations:
            print(f"警告: 项目 {self.project_name} 加载了 0 条有效数据。")

    def get_raw_24hri(self) -> float:
        return calculate_24hri_raw(self.commit_hours)
        
    def get_normalized_24hri(self) -> float:
        if not self.commit_hours:
            return 0.0
        return self.get_raw_24hri() / self.MAX_24HRI_ENTROPY

    def get_raw_geo_diversity(self) -> float:
        return calculate_geo_diversity_raw(self.locations)

    def get_normalized_geo_diversity(self)D -> float:
        if not self.locations or len(self.unique_locations) <= 1:
            return 0.0
        
        max_geo_entropy = math.log2(len(self.unique_locations))
        if max_geo_entropy == 0:
            return 0.0
        
        return self.get_raw_geo_diversity() / max_geo_entropy

    def get_combined_globalization_index(self, w_hri=0.6, w_geo=0.4) -> float:
        norm_hri = self.get_normalized_24hri()
        norm_geo = self.get_normalized_geo_diversity()
        
        return (norm_hri * w_hri) + (norm_geo * w_geo)

    def get_full_report(self) -> dict:
        raw_hri = self.get_raw_24hri()
        raw_geo = self.get_raw_geo_diversity()
        
        return {
            "project_name": self.project_name,
            "total_contributions_analyzed": len(self.commit_hours),
            "unique_locations_found": len(self.unique_locations),
            "metrics": {
                "24HRI (Raw)": raw_hri,
                "24HRI (Normalized)": self.get_normalized_24hri(),
                "GeoDiversity (Raw)": raw_geo,
                "GeoDiversity (Normalized)": self.get_normalized_geo_diversity(),
                "OpenRank (Simulated)": get_openrank_simulated(self.project_name),
                "GlobalizationIndex (Combined)": self.get_combined_globalization_index()
            },
            "interpretation": {
                "HRI_Max_Entropy": self.MAX_24HRI_ENTROPY,
                "Geo_Max_Entropy": math.log2(len(self.unique_locations)) if self.unique_locations else 0
            }
        }

def generate_mock_contributions(config: dict) -> list:
    num_contributions = config["num_contributions"]
    
    hours_dist = config["hours_dist"]
    center_hour, spread = hours_dist
    
    contributions = []
    
    location_weights = config["location_weights"]
    locations = list(location_weights.keys())
    weights = list(location_weights.values())
    
    for _ in range(num_contributions):
        hour = int(random.gauss(center_hour, spread)) % 24
        
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        day = random.randint(1, 28)
        month = 10
        year = 2023
        
        try:
            timestamp_str = datetime(year, month, day, hour, minute, second).isoformat() + "Z"
        except ValueError:
            timestamp_str = datetime(year, month, 1, hour, minute, second).isoformat() + "Z"

        
        location = random.choices(locations, weights=weights, k=1)[0]
        
        contributions.append({
            "timestamp": timestamp_str,
            "location": location
        })
        
    return contributions

def create_mock_data_file(filepath: str):
    print(f"正在生成模拟数据文件: {filepath} ...")
    
    project_configs = [
        {
            "name": "Project-Global-Relay",
            "num_contributions": 1000,
            "hours_dist": (12, 12),
            "location_weights": {"USA": 1, "CHN": 1, "DEU": 1, "IND": 1, "BRA": 1, "NGA": 1}
        },
        {
            "name": "Project-US-Centric",
            "num_contributions": 500,
            "hours_dist": (18, 3),
            "location_weights": {"USA": 10, "CAN": 2, "GBR": 1}
        },
        {
            "name": "Project-Asia-Centric",
            "num_contributions": 500,
            "hours_dist": (4, 3),
            "location_weights": {"CHN": 10, "JPN": 3, "KOR": 2, "SGP": 1}
        }
    ]
    
    output_data = {}
    for config in project_configs:
        print(f"  - 生成项目: {config['name']}")
        output_data[config['name']] = generate_mock_contributions(config)
        
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        print(f"成功保存模拟数据到: {filepath}")
    except IOError as e:
        print(f"错误：无法写入文件 {filepath}. {e}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(
        description="开源项目全球化评估框架",
        epilog="示例用法:\n"
               "1. 生成模拟数据: python %(prog)s --generate-mock mock_data.json\n"
               "2. 分析所有项目:   python %(prog)s --analyze mock_data.json\n"
               "3. 分析单个项目: python %(prog)s --analyze mock_data.json --project Project-Global-Relay",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--generate-mock",
        metavar="FILEPATH",
        help="生成一个模拟的 JSON 数据文件并保存到指定路径。"
    )
    
    parser.add_argument(
        "--analyze",
        metavar="FILEPATH",
        help="加载并分析指定的 JSON 数据文件。"
    )
    parser.add_argument(
        "-p", "--project",
        metavar="PROJECT_NAME",
        help="（可选）只分析 JSON 文件中指定的单个项目名称。"
    )
    
    args = parser.parse_args()
    
    if args.generate_mock:
        create_mock_data_file(args.generate_mock)
        
    elif args.analyze:
        try:
            with open(args.analyze, 'r', encoding='utf-8') as f:
                all_project_data = json.load(f)
        except FileNotFoundError:
            print(f"错误：文件未找到 {args.analyze}", file=sys.stderr)
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"错误：文件 {args.analyze} 不是有效的 JSON 格式。", file=sys.stderr)
            sys.exit(1)
        
        projects_to_analyze = {}
        if args.project:
            if args.project not in all_project_data:
                print(f"错误：项目 '{args.project}' 在文件 {args.analyze} 中未找到。", file=sys.stderr)
                print(f"可用项目: {list(all_project_data.keys())}", file=sys.stderr)
                sys.exit(1)
            projects_to_analyze = {args.project: all_project_data[args.project]}
        else:
            projects_to_analyze = all_project_data
            
        print(f"--- 正在分析 {len(projects_to_analyze)} 个项目于 {args.analyze} ---")
        print("=" * 50)

        for project_name, contributions in projects_to_analyze.items():
            analyzer = ProjectAnalyzer(project_name)
            analyzer.load_contributions(contributions)
            
            report = analyzer.get_full_report()
            
            print(f"\n项目: {report['project_name']}")
            print(f"  总贡献数: {report['total_contributions_analyzed']}")
            print(f"  独立位置数: {report['unique_locations_found']}")
            print("-" * 30)
            
            metrics = report['metrics']
            print(f"  [综合指数] 全球化综合指数: {metrics['GlobalizationIndex (Combined)']:.4f}")
            print(f"  [指标 1] 24HRI (归一化): {metrics['24HRI (Normalized)']:.4f}")
            print(f"  [指标 2] 地理多样性 (归一化): {metrics['GeoDiversity (Normalized)']:.4f}")
            print(f"  [指标 3] OpenRank (模拟值): {metrics['OpenRank (Simulated)']:.4f}")
            print(f"  (原始 24HRI 熵: {metrics['24HRI (Raw)']:.4f} / 最大 {report['interpretation']['HRI_Max_Entropy']:.4f})")
            print(f"  (原始地理熵: {metrics['GeoDiversity (Raw)']:.4f} / 最大 {report['interpretation']['Geo_Max_Entropy']:.4f})")
            print("=" * 50)

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
