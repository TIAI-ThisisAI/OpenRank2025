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
