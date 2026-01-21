import pandas as pd
import clickhouse_connect


def get_clickhouse_client(config: dict):
    """
    连接到 ClickHouse 数据库并返回客户端对象
    """
    try:
        client = clickhouse_connect.get_client(**config)
        print("数据库连接成功。")
        return client
    except Exception as e:
        print(f"连接数据库失败：{e}")
        return None
        
def load_excel_data(file_path: str, sheet_index: int = 0, header: int = None) -> pd.DataFrame:
    """
    从 Excel 文件中加载数据，返回 DataFrame
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_index, header=header)
        print("Excel 文件加载成功。")
        return df
    except FileNotFoundError:
        print(f"错误：未找到文件 '{file_path}'。")
        return None
    except Exception as e:
        print(f"读取 Excel 文件失败：{e}")
        return None

def extract_repo_names(df: pd.DataFrame, column_index: int) -> list:
    """
    提取指定列的仓库名列表
    """
    return df.iloc[1:, column_index].astype(str).tolist()


def generate_sql_query(repo_name: str) -> str:
    """
    根据仓库名生成 SQL 查询语句
    """
    safe_repo_name = repo_name.strip().replace("'", "''")
    return f"""
    SELECT
        t2.description,
        t2.primary_language,
        t2.license,
        t2.topics
    FROM
        opensource.events AS t1
    LEFT JOIN
        opensource.gh_repo_info AS t2
        ON t1.repo_id = t2.id
    WHERE
        t1.repo_name = '{safe_repo_name}'
    LIMIT 1
    """

def query_clickhouse(client, query: str):
    """
    执行 ClickHouse 查询并返回结果
    """
    try:
        result = client.query(query)
        return result
    except Exception as e:
        print(f"查询时发生错误：{e}")
        return None

def process_repo_data(client, repo_names: list, df: pd.DataFrame, b_column_index: int, start_column_index: int):
    """
    遍历仓库名列表并执行查询，将数据写入 Excel
    """
    for index, repo_name in enumerate(repo_names):
        # 忽略空值或无效值
        if not repo_name or repo_name.lower() == 'nan':
            continue

        query = generate_sql_query(repo_name)
        result = query_clickhouse(client, query)

        if result and result.row_count > 0:
            # 提取查询结果
            description, primary_language, license, topics = result.first_row
            print(f"处理中... {repo_name} - 成功获取数据。")

            # 将结果写入DataFrame的对应位置
            df.iloc[index + 1, start_column_index] = description
            df.iloc[index + 1, start_column_index + 1] = primary_language
            df.iloc[index + 1, start_column_index + 2] = license
            df.iloc[index + 1, start_column_index + 3] = topics
        else:
            print(f"处理中... {repo_name} - 未找到匹配数据。")
            df.iloc[index + 1, start_column_index:start_column_index + 4] = [None] * 4

def save_to_excel(df: pd.DataFrame, file_path: str):
    """
    保存修改后的 DataFrame 到 Excel 文件
    """
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='w') as writer:
            df.to_excel(writer, index=False, header=False)
        print(f"结果已成功写入 Excel 文件: {file_path}")
    except Exception as e:
        print(f"保存 Excel 文件时发生错误：{e}")

def clean_repo_names(repo_names: list) -> list:
    """
    清洗仓库名列表：去除空值、nan、重复项
    """
    cleaned = []
    for name in repo_names:
        if not name or name.lower() == 'nan':
            continue
        if name not in cleaned:
            cleaned.append(name)
    return cleaned

def validate_write_columns(df: pd.DataFrame, start_column_index: int, width: int = 4) -> bool:
    """
    Validate whether the target write columns exist in the DataFrame.
    """
    return start_column_index + width <= df.shape[1]


def parse_repo_query_result(result) -> dict:
    """
    Parse ClickHouse query result into a structured dictionary.
    """
    if not is_valid_query_result(result):
        return {}

    description, primary_language, license, topics = result.first_row
    return {
        "description": description,
        "primary_language": primary_language,
        "license": license,
        "topics": topics
    }


def write_repo_metadata(
    df: pd.DataFrame,
    row_index: int,
    start_column_index: int,
    repo_info: dict
):
    """
    Write repository metadata dictionary into a DataFrame row.
    """
    df.iloc[row_index, start_column_index] = repo_info.get("description")
    df.iloc[row_index, start_column_index + 1] = repo_info.get("primary_language")
    df.iloc[row_index, start_column_index + 2] = repo_info.get("license")
    df.iloc[row_index, start_column_index + 3] = repo_info.get("topics")

def build_repo_metadata_cache(client, repo_names: list) -> dict:
    """
    Build a repository metadata cache to avoid repeated database queries.
    """
    cache = {}
    for repo in repo_names:
        query = generate_sql_query(repo)
        result = query_clickhouse(client, query)
        cache[repo] = parse_repo_query_result(result)
    return cache

def analyze_primary_language_distribution(df: pd.DataFrame, language_column_index: int) -> dict:
    """
    Analyze the distribution of primary programming languages.
    """
    series = df.iloc[1:, language_column_index]
    return series.value_counts(dropna=True).to_dict()


def analyze_license_distribution(df: pd.DataFrame, license_column_index: int) -> dict:
    """
    Analyze the distribution of open-source licenses.
    """
    series = df.iloc[1:, license_column_index]
    return series.value_counts(dropna=True).to_dict()


import json

def export_analysis_to_json(data: dict, output_path: str):
    """
    Export analysis results to a JSON file.
    """
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def build_repository_summary(df: pd.DataFrame, start_column_index: int) -> dict:
    """
    Build a high-level repository metadata summary for analysis.
    """
    summary = {
        "total_repositories": len(df) - 1,
        "languages": analyze_primary_language_distribution(df, start_column_index + 1),
        "licenses": analyze_license_distribution(df, start_column_index + 2),
    }
    return summary

def compute_missing_ratio(df: pd.DataFrame, column_index: int) -> float:
    """
    Compute the missing value ratio of a specific column.
    """
    total = len(df) - 1
    missing = df.iloc[1:, column_index].isna().sum()
    return missing / total if total > 0 else 0.0


def build_completeness_report(df: pd.DataFrame, column_indices: list) -> dict:
    """
    Build a completeness report for multiple columns.
    """
    report = {}
    for idx in column_indices:
        report[idx] = {
            "missing_ratio": compute_missing_ratio(df, idx),
            "non_missing_count": df.iloc[1:, idx].notna().sum()
        }
    return report


def extract_top_n_categories(series: pd.Series, top_n: int = 10) -> dict:
    """
    Extract top-N most frequent categories from a pandas Series.
    """
    return series.value_counts(dropna=True).head(top_n).to_dict()


def normalize_topic_field(topic_value) -> list:
    """
    Normalize repository topic field into a list.
    """
    if topic_value is None or pd.isna(topic_value):
        return []
    if isinstance(topic_value, list):
        return topic_value
    if isinstance(topic_value, str):
        return [t.strip() for t in topic_value.split(',') if t.strip()]
    return []

import math

def compute_entropy(distribution: dict) -> float:
    """
    Compute entropy for a categorical distribution.
    """
    total = sum(distribution.values())
    if total == 0:
        return 0.0

    entropy = 0.0
    for count in distribution.values():
        p = count / total
        entropy -= p * math.log(p)
    return entropy

def compute_language_diversity(df: pd.DataFrame, language_column_index: int) -> float:
    """
    Compute language diversity score using entropy.
    """
    distribution = df.iloc[1:, language_column_index].value_counts(dropna=True).to_dict()
    return compute_entropy(distribution)


def analyze_long_tail(distribution: dict, threshold: float = 0.8) -> dict:
    """
    Analyze long-tail contribution based on cumulative proportion.
    """
    total = sum(distribution.values())
    cumulative = 0
    head_items = []
    tail_items = []

    for key, value in sorted(distribution.items(), key=lambda x: x[1], reverse=True):
        cumulative += value
        if cumulative / total <= threshold:
            head_items.append(key)
        else:
            tail_items.append(key)

    return {
        "head_count": len(head_items),
        "tail_count": len(tail_items),
        "head_items": head_items,
        "tail_items": tail_items
    }


def run_pipeline(excel_file_path: str, b_column_index: int, start_column_index: int, client_config: dict):
    """
    主流程：执行连接、数据读取、处理、保存等任务
    """
    # 1. 连接到 ClickHouse 数据库
    client = get_clickhouse_client(client_config)
    if not client:
        return

    # 2. 读取 Excel 数据
    df = load_excel_data(excel_file_path)
    if df is None:
        return

    # 3. 提取项目名列表
    repo_names = extract_repo_names(df, b_column_index)

    print("开始处理仓库列表...")

    # 4. 处理每个仓库的数据
    process_repo_data(client, repo_names, df, b_column_index, start_column_index)

    # 5. 保存修改后的数据
    save_to_excel(df, excel_file_path)

    # 6. 关闭数据库连接
    if client.is_connected():
        client.close()
    print("\n程序执行完毕！")

