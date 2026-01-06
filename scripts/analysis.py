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

# 相关性分析

def calculate_correlation(df: pd.DataFrame, method: str = 'pearson') -> pd.DataFrame:
    """
    计算 DataFrame 中所有数值列之间的相关性矩阵
    """
    return df.corr(method=method)


def plot_correlation_heatmap(df: pd.DataFrame, title: str = 'Correlation Heatmap'):
    """
    绘制相关性矩阵的热力图
    """
    correlation_matrix = df.corr()
    plt.figure(figsize=(10, 6))
    plt.imshow(correlation_matrix, cmap='coolwarm', interpolation='none')
    plt.title(title)
    plt.colorbar()
    plt.xticks(np.arange(correlation_matrix.shape[1]), correlation_matrix.columns, rotation=45)
    plt.yticks(np.arange(correlation_matrix.shape[0]), correlation_matrix.index)
    plt.tight_layout()
    plt.show()

# 可视化

def plot_column_distribution(df: pd.DataFrame, column_index: int, title: str = 'Column Distribution'):
    """
    绘制指定列的数据分布图（直方图）
    """
    plt.figure(figsize=(10, 6))
    plt.hist(df.iloc[:, column_index], bins=20, color='skyblue', edgecolor='black')
    plt.title(title)
    plt.xlabel(df.columns[column_index])
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plot_boxplot(df: pd.DataFrame, column_index: int, title: str = 'Boxplot Distribution'):
    """
    绘制指定列的箱线图
    """
    plt.figure(figsize=(10, 6))
    plt.boxplot(df.iloc[:, column_index], vert=False)
    plt.title(title)
    plt.xlabel(df.columns[column_index])
    plt.tight_layout()

# 数据保存

def save_analysis_results(df: pd.DataFrame, output_file: str):
    """
    保存分析结果到新的 Excel 文件
    """
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        print(f"分析结果已成功保存到 '{output_file}'。")
    except Exception as e:
        print(f"保存分析结果时发生错误：{e}")

# 主函数

def run_analysis_pipeline(excel_file_path: str, output_file: str, column_index: int, top_n: int = 10):
    """
    主流程：执行数据加载、清洗、分析、可视化、保存等任务
    """
    # 1. 读取数据
    df = load_data(excel_file_path)
    if df is None:
        return

    # 2. 清洗数据
    df_cleaned = clean_data(df)

    # 3. 计算统计数据
    stats = calculate_statistics(df_cleaned, column_index)
    print("统计数据：", stats)

    # 4. 计算指定分位数
    percentiles = calculate_percentiles(df_cleaned, column_index, [25, 50, 75])
    print("分位数数据：", percentiles)

    # 5. 排序并选择排名前 N 的数据
    top_data = filter_top_n(df_cleaned, column_index, top_n)
    print(f"排名前 {top_n} 的数据：")
    print(top_data)

    # 6. 绘制相关性热力图
    plot_correlation_heatmap(df_cleaned, title="Data Correlation Heatmap")

    # 7. 绘制列数据分布图
    plot_column_distribution(df_cleaned, column_index, title="Column Distribution")

    # 8. 绘制箱线图
    plot_boxplot(df_cleaned, column_index, title="Boxplot Distribution")

    # 9. 保存分析结果
    save_analysis_results(df_cleaned, output_file)








