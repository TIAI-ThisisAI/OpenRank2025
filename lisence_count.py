import pandas as pd

def read_excel_sheet(excel_file: str, sheet_name: str) -> pd.DataFrame:
    """
    读取指定 Excel 文件和 Sheet
    """
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"未找到文件 {excel_file}")
    except ValueError:
        raise ValueError(f"未找到名为 '{sheet_name}' 的工作表")

def get_column_series(
    df: pd.DataFrame,
    column_name: str,
    fallback_index: int
) -> pd.Series:
    """
    获取指定列，如果列名不存在则使用列索引
    """
    if column_name in df.columns:
        return df[column_name]
    else:
        print(f"警告：未找到 '{column_name}' 列，使用第 {fallback_index + 1} 列")
        return df.iloc[:, fallback_index]

def license_statistics(license_series: pd.Series) -> pd.DataFrame:
    """
    统计 license 数量及占比
    """
    counts = license_series.value_counts()
    total = len(license_series)
    percentages = (counts / total) * 100

    return pd.DataFrame({
        "数量": counts,
        "占比 (%)": percentages
    })
    

def save_to_excel(df: pd.DataFrame, output_file: str):
    """
    保存 DataFrame 到 Excel
    """
    df.to_excel(output_file)
    print(f"结果已保存到 {output_file}")

