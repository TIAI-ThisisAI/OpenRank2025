import json
import matplotlib.pyplot as plt
from matplotlib import rcParams
from typing import Dict, List


def set_plot_font():
    """
    设置支持中文的字体和解决负号显示问题
    """
    rcParams['font.sans-serif'] = ['SimHei']  # 黑体
    rcParams['axes.unicode_minus'] = False    # 解决负号无法显示的问题
    print("图表字体设置完成。")

def load_json_data(file_path: str) -> Dict:
    """
    从 JSON 文件中加载数据
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        print("数据加载成功。")
        return data
    except FileNotFoundError:
        print(f"错误：未找到文件 '{file_path}'。")
        return {}
    except Exception as e:
        print(f"读取 JSON 文件失败：{e}")
        return {}


def extract_years_and_data(data: Dict) -> (List[int], Dict):
    """
    提取年份和公司数据
    """
    years = data.get('Year', [])
    del data['Year']
    print("年份和公司数据提取完成。")
    return years, data

def filter_recent_data(data: Dict, years: List[int]) -> Dict:
    """
    过滤出最近两年内至少有一个有排名的公司
    """
    recent_years = years[-2:]
    filtered_data = {
        company: ranks for company, ranks in data.items()
        if any(ranks[years.index(y)] is not None for y in recent_years)
    }
    print("最近两年有排名的公司已筛选完成。")
    return filtered_data

def get_final_year_ranks(filtered_data: Dict, years: List[int]) -> Dict:
    """
    获取最后一年的排名，按排名排序
    """
    final_year = years[-1]
    final_year_ranks = {
        company: ranks[years.index(final_year)] if ranks[years.index(final_year)] is not None else float('inf')
        for company, ranks in filtered_data.items()
    }
    print("最后一年的排名已获取。")
    return final_year_ranks

def sort_companies_by_final_year_rank(final_year_ranks: Dict) -> List[str]:
    """
    按最后一年的排名排序公司
    """
    sorted_companies = sorted(final_year_ranks, key=lambda x: (final_year_ranks[x] is None, final_year_ranks[x]))
    print("公司已按最后一年的排名排序。")
    return sorted_companies

def plot_openrank_trends(years: List[int], filtered_data: Dict, sorted_companies: List[str]):
    """
    绘制企业的 OpenRank 趋势折线图
    """
    plt.figure(figsize=(14, 8))
    
    for company in sorted_companies:
        ranks = filtered_data[company]
        plt.plot(years, ranks, marker='o', label=company)
    
    plt.gca().invert_yaxis()  # 反转 y 轴，使排名 1 在顶部
    plt.xticks(years)
    plt.yticks(range(1, 16))
    plt.title('Chinese Enterprise 2015~2024 OpenRank')
    plt.xlabel('Time')
    plt.ylabel('Rank')
    plt.legend(title='symbol', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    print("OpenRank 趋势图绘制完成。")

def run_openrank_analysis(file_path: str):
    """
    主函数：执行从数据加载、处理、排序到绘图的完整分析流程
    """
    # 设置图表字体
    set_plot_font()

    # 1. 加载数据
    data = load_json_data(file_path)
    if not data:
        return

    # 2. 提取年份和公司数据
    years, filtered_data = extract_years_and_data(data)

    # 3. 过滤出最近两年内至少有排名的公司
    filtered_data = filter_recent_data(filtered_data, years)

    # 4. 获取最后一年的排名
    final_year_ranks = get_final_year_ranks(filtered_data, years)

    # 5. 按最后一年的排名排序公司
    sorted_companies = sort_companies_by_final_year_rank(final_year_ranks)

    # 6. 绘制趋势图
    plot_openrank_trends(years, filtered_data, sorted_companies)

def compute_rank_volatility(company_ranks: list) -> float:
    """
    计算单个公司历年排名的波动性（平均绝对变化幅度）
    """
    valid_ranks = [r for r in company_ranks if r is not None]
    if len(valid_ranks) < 2:
        return 0.0

    diffs = [
        abs(valid_ranks[i] - valid_ranks[i - 1])
        for i in range(1, len(valid_ranks))
    ]
    return sum(diffs) / len(diffs)

def analyze_all_volatility(data: dict) -> dict:
    """
    计算所有公司的排名波动性
    """
    return {
        company: compute_rank_volatility(ranks)
        for company, ranks in data.items()
    }

def get_stable_top_companies(data: dict, years: list, top_k: int = 10, min_years: int = 5) -> list:
    """
    筛选在至少 min_years 年内保持 Top-K 的公司
    """
    stable_companies = []

    for company, ranks in data.items():
        count = sum(
            1 for r in ranks if r is not None and r <= top_k
        )
        if count >= min_years:
            stable_companies.append(company)

    return stable_companies


def classify_rank_trend(company_ranks: list) -> str:
    """
    判断公司排名趋势：improving / declining / volatile
    """
    valid_ranks = [r for r in company_ranks if r is not None]
    if len(valid_ranks) < 3:
        return "insufficient_data"

    start, end = valid_ranks[0], valid_ranks[-1]

    if end < start:
        return "improving"
    elif end > start:
        return "declining"
    else:
        return "stable"



if __name__ == "__main__":
    FILE_PATH = 'openrank_chart_Chinese_data_2.json'
    
    # 执行完整的分析流程
    run_openrank_analysis(FILE_PATH)

