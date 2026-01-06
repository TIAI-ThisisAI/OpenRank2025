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

# 基本统计分析

def calculate_statistics(df: pd.DataFrame, column_index: int) -> Dict:
    """
    计算指定列的基本统计数据（均值、标准差、最大值、最小值等）
    """
    stats = {
        'mean': df.iloc[:, column_index].mean(),
        'std': df.iloc[:, column_index].std(),
        'min': df.iloc[:, column_index].min(),
        'max': df.iloc[:, column_index].max(),
    }
    return stats


def calculate_percentiles(df: pd.DataFrame, column_index: int, percentiles: List[float]) -> Dict:
    """
    计算指定列的指定分位数（如 25%，50%，75%）
    """
    percentiles_values = np.percentile(df.iloc[:, column_index], percentiles)
    return {f"{p}%": value for p, value in zip(percentiles, percentiles_values)}

def summary_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算 DataFrame 中所有数值列的汇总统计数据
    """
    return df.describe()


# 排序与过滤

def sort_by_column(df: pd.DataFrame, column_index: int, ascending: bool = False) -> pd.DataFrame:
    """
    按指定列排序数据
    """
    return df.sort_values(by=df.columns[column_index], ascending=ascending)

def filter_top_n(df: pd.DataFrame, column_index: int, n: int) -> pd.DataFrame:
    """
    过滤出排名前 N 的数据
    """
    return df.nlargest(n, df.columns[column_index])


def filter_by_threshold(df: pd.DataFrame, column_index: int, threshold: float) -> pd.DataFrame:
    """
    过滤出大于指定阈值的行
    """
    return df[df.iloc[:, column_index] > threshold]




