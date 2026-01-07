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
