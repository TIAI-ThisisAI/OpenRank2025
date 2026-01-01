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
