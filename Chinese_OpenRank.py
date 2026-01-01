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

if __name__ == "__main__":
    run_openrank_visualization(
        json_file="openrank_chart_Chinese_data_2.json",
        recent_n=2,
        max_rank=15
    )

