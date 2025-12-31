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


def run_license_analysis(
    excel_file: str,
    sheet_name: str = "Sheet3",
    license_column: str = "license",
    fallback_index: int = 2,
    output_file: str = "license_summary.xlsx"
):
    """
    License 分析主流程（pipeline）
    """
    df = read_excel_sheet(excel_file, sheet_name)
    license_series = get_column_series(df, license_column, fallback_index)
    summary_df = license_statistics(license_series)

    print("License 统计结果：")
    print(summary_df)

    save_to_excel(summary_df, output_file)

def normalize_license(license_series: pd.Series) -> pd.Series:
    """
    对 license 字段做基础清洗与标准化
    """
    return (
        license_series
        .fillna("Unknown")
        .astype(str)
        .str.strip()
        .str.lower()
    )

def license_coverage_rate(license_series: pd.Series) -> pd.DataFrame:
    """
    统计 license 覆盖率（是否声明 license）
    """
    total = len(license_series)
    covered = license_series.notna().sum()

    return pd.DataFrame({
        "总项目数": [total],
        "声明 License 项目数": [covered],
        "覆盖率 (%)": [covered / total * 100]
    })

def license_openrank_weighted(df: pd.DataFrame) -> pd.DataFrame:
    """
    基于 OpenRank 的 License 影响力加权统计
    """
    result = (
        df.groupby("license")["openrank"]
        .sum()
        .sort_values(ascending=False)
    )

    return result.to_frame(name="OpenRank 加权影响力")


if __name__ == "__main__":
    run_license_analysis(
        excel_file="item_with_openrank.xlsx",
        sheet_name="Sheet3",
        license_column="license",
        output_file="license_summary.xlsx"
    )

