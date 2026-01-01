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
