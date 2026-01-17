import json
import matplotlib.pyplot as plt
from matplotlib import rcParams
from typing import Dict, List


def setup_chinese_font():
    """
    设置 Matplotlib 中文字体
    """
    rcParams['font.sans-serif'] = ['SimHei']
    rcParams['axes.unicode_minus'] = False

def load_openrank_json(file_path: str) -> Dict:
    """
    加载 OpenRank JSON 数据
    """
    with open(file_path, 'r') as f:
        return json.load(f)

def extract_years_and_data(data: Dict) -> (List[int], Dict[str, List[int]]):
    """
    提取年份列表和公司排名数据
    """
    years = data['Year']
    data = {k: v for k, v in data.items() if k != 'Year'}
    return years, data

def filter_companies_by_recent_years(
    data: Dict[str, List[int]],
    years: List[int],
    recent_n: int = 2
) -> Dict[str, List[int]]:
    """
    过滤最近 N 年内至少一年有排名的公司
    """
    recent_years = years[-recent_n:]

    return {
        company: ranks
        for company, ranks in data.items()
        if any(
            ranks[years.index(y)] is not None
            for y in recent_years
        )
    }

def sort_companies_by_final_year_rank(
    data: Dict[str, List[int]],
    years: List[int]
) -> List[str]:
    """
    按最后一年排名对公司排序
    """
    final_year = years[-1]
    final_index = years.index(final_year)

    final_ranks = {
        company: (
            ranks[final_index]
            if ranks[final_index] is not None
            else float('inf')
        )
        for company, ranks in data.items()
    }

    return sorted(final_ranks, key=lambda x: final_ranks[x])

def plot_openrank_trends(
    years: List[int],
    data: Dict[str, List[int]],
    companies: List[str],
    title: str = "Chinese Enterprise 2015~2024 OpenRank",
    max_rank: int = 15
):
    """
    绘制 OpenRank 趋势折线图
    """
    plt.figure(figsize=(14, 8))

    for company in companies:
        plt.plot(years, data[company], marker='o', label=company)

    plt.gca().invert_yaxis()
    plt.xticks(years)
    plt.yticks(range(1, max_rank + 1))
    plt.xlabel("Time")
    plt.ylabel("Rank")
    plt.title(title)
    plt.legend(title="symbol", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def run_openrank_visualization(
    json_file: str,
    recent_n: int = 2,
    max_rank: int = 15
):
    """
    OpenRank 可视化主流程（Pipeline）
    """
    setup_chinese_font()
    raw_data = load_openrank_json(json_file)
    years, company_data = extract_years_and_data(raw_data)

    filtered_data = filter_companies_by_recent_years(
        company_data, years, recent_n
    )

    sorted_companies = sort_companies_by_final_year_rank(
        filtered_data, years
    )

    plot_openrank_trends(
        years,
        filtered_data,
        sorted_companies,
        max_rank=max_rank
    )

def sort_companies_by_average_rank(
    data: dict,
    ignore_none: bool = True
) -> list:
    """
    按公司历史平均排名排序（可忽略 None）
    """
    def avg_rank(ranks):
        valid = [r for r in ranks if r is not None] if ignore_none else ranks
        return sum(valid) / len(valid) if valid else float('inf')

    avg_ranks = {c: avg_rank(r) for c, r in data.items()}
    return sorted(avg_ranks, key=lambda x: avg_ranks[x])

def sort_companies_by_rank_improvement(
    data: dict
) -> list:
    """
    按排名提升幅度排序（首次出现 vs 最后一年）
    """
    def improvement(ranks):
        valid = [r for r in ranks if r is not None]
        return valid[0] - valid[-1] if len(valid) >= 2 else float('-inf')

    improvements = {c: improvement(r) for c, r in data.items()}
    return sorted(improvements, key=lambda x: improvements[x], reverse=True)
    
def filter_long_term_companies(
    data: dict,
    min_years: int = 5
) -> dict:
    """
    仅保留至少出现 min_years 次的公司
    """
    return {
        c: r for c, r in data.items()
        if sum(v is not None for v in r) >= min_years
    }

import numpy as np

def compute_rank_volatility(
    data: dict
) -> dict:
    """
    计算各公司排名波动性（标准差）
    """
    volatility = {}
    for company, ranks in data.items():
        valid = [r for r in ranks if r is not None]
        volatility[company] = np.std(valid) if len(valid) > 1 else 0
    return volatility

def compute_rank_concentration(data: dict, years: list, top_k: int = 5) -> dict:
    """
    计算每一年的 Top-K 排名集中度（出现公司数）
    """
    concentration = {}

    for idx, year in enumerate(years):
        companies_in_top_k = [
            company
            for company, ranks in data.items()
            if ranks[idx] is not None and ranks[idx] <= top_k
        ]
        concentration[year] = len(set(companies_in_top_k))

    return concentration



if __name__ == "__main__":
    run_openrank_visualization(
        json_file="openrank_chart_Chinese_data_2.json",
        recent_n=2,
        max_rank=15
    )

