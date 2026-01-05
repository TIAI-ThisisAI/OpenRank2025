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
