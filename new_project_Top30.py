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

