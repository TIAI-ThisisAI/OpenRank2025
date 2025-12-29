import pandas as pd
import clickhouse_connect
import time
import logging

# 数据库连接配置
CLIENT_CONFIG = {
}

# Excel 文件名和列索引配置
excel_file_path = 'item.xlsx'
b_column_index = 1
start_column_index = 4

# SQL 查询模板
SQL_TEMPLATE = """
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
    t1.repo_name = '{}'
LIMIT 1
"""

# 设置日志配置
logging.basicConfig(filename='query_log.log', level=logging.INFO, 
                    format='%(asctime)s - %(message)s')
error_log = logging.getLogger('error_log')
error_log.setLevel(logging.ERROR)

def connect_to_db():
    """连接到 ClickHouse 数据库"""
    try:
        client = clickhouse_connect.get_client(**CLIENT_CONFIG)
        logging.info("数据库连接成功。")
        return client
    except Exception as e:
        logging.error(f"数据库连接失败: {e}")
        raise e

def read_excel_data(excel_path):
    """读取 Excel 文件并返回项目名列表"""
    try:
        df = pd.read_excel(excel_path, sheet_name=0, header=None)
        repo_names = df.iloc[1:, b_column_index].astype(str).tolist()
        logging.info(f"成功读取 Excel 文件 {excel_path}。")
        return df, repo_names
    except FileNotFoundError:
        logging.error(f"错误：未找到文件 '{excel_path}'。")
        raise FileNotFoundError(f"未找到文件 '{excel_path}'。")
    except Exception as e:
        logging.error(f"读取 Excel 文件时发生错误：{e}")
        raise e

def main():
    """主函数，执行整个流程"""
    try:
        client = connect_to_db()  # 连接数据库
        df, repo_names = read_excel_data(excel_file_path)  # 读取 Excel 文件
        
        logging.info("开始处理仓库列表...")

        # 3. 遍历每个项目名并执行查询
        for index, repo_name in enumerate(repo_names):
            # 忽略空值或无效值
            if not repo_name or repo_name.lower() == 'nan':
                continue

            result = execute_query(client, repo_name)  # 执行查询

            if result:
                process_query_result(result, repo_name, df, index)  # 处理查询结果
            else:
                df.iloc[index + 1, start_column_index:start_column_index+4] = ['查询失败'] * 4

        # 4. 保存修改后的 DataFrame 到原始 Excel 文件
        save_to_excel(df, excel_file_path)  # 保存结果到 Excel

        print("\n程序执行完毕！")

    except Exception as e:
        logging.error(f"程序执行失败：{e}")

    finally:
        if 'client' in locals() and client.is_connected():
            client.close()


if __name__ == "__main__":
    main()
