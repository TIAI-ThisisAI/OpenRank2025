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
