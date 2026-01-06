import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from typing import Dict, List


# 数据加载与预处理

def load_data(file_path: str, sheet_index: int = 0, header: int = None) -> pd.DataFrame:
    """
    从 Excel 文件中加载数据并返回 DataFrame
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_index, header=header)
        print("数据加载成功。")
        return df
    except FileNotFoundError:
        print(f"错误：未找到文件 '{file_path}'。")
        return None
    except Exception as e:
        print(f"读取 Excel 文件失败：{e}")
        return None


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    清洗数据，去除空值和无效值
    """
    df_cleaned = df.dropna()
    df_cleaned = df_cleaned[df_cleaned.apply(lambda row: not any([str(val).lower() == 'nan' for val in row]), axis=1)]
    print("数据清洗完成。")
    return df_cleaned
