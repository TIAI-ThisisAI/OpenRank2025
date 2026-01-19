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

try:
    # 1. 连接到 ClickHouse 数据库
    client = clickhouse_connect.get_client(**CLIENT_CONFIG)
    logging.info("数据库连接成功。")

    # 2. 读取 Excel 文件，获取项目名列表
    df = pd.read_excel(excel_file_path, sheet_name=0, header=None)
    repo_names = df.iloc[1:, b_column_index].astype(str).tolist()

    logging.info("开始处理仓库列表...")

    # 3. 遍历每个项目名并执行查询
    for index, repo_name in enumerate(repo_names):
        # 忽略空值或无效值
        if not repo_name or repo_name.lower() == 'nan':
            continue

        # 格式化SQL语句，处理可能包含的单引号和空格
        safe_repo_name = repo_name.strip().replace("'", "''")
        query = SQL_TEMPLATE.format(safe_repo_name)

        try:
            # 记录查询开始时间
            query_start_time = time.time()

            # 执行查询
            result = client.query(query)

            # 记录查询结束时间并计算时间差
            query_end_time = time.time()
            query_duration = query_end_time - query_start_time
            logging.info(f"查询 {repo_name} 完成，用时 {query_duration:.2f} 秒。")

            # 提取查询结果
            if result.row_count > 0:
                description, primary_language, license, topics = result.first_row
                logging.info(f"处理 {repo_name} - 成功获取数据。")

                # 将结果写入DataFrame的对应位置
                df.iloc[index + 1, start_column_index] = description
                df.iloc[index + 1, start_column_index + 1] = primary_language
                df.iloc[index + 1, start_column_index + 2] = license
                df.iloc[index + 1, start_column_index + 3] = topics
            else:
                logging.warning(f"处理 {repo_name} - 未找到匹配数据。")
                df.iloc[index + 1, start_column_index:start_column_index+4] = [None] * 4

        except Exception as e:
            error_msg = f"查询 {repo_name} 时发生错误：{e}"
            logging.error(error_msg)
            error_log.error(f"查询 {repo_name} 时发生错误：{e}")
            df.iloc[index + 1, start_column_index:start_column_index+4] = ['查询失败'] * 4

    # 4. 保存修改后的 DataFrame 到原始 Excel 文件
    with pd.ExcelWriter(excel_file_path, engine='openpyxl', mode='w') as writer:
        df.to_excel(writer, index=False, header=False)

    logging.info(f"结果已成功写入 Excel 文件: {excel_file_path}")
    print("\n程序执行完毕！")

except FileNotFoundError:
    logging.error(f"错误：未找到文件 '{excel_file_path}'。请确保文件与脚本在同一目录下。")
except Exception as e:
    logging.error(f"发生了一个错误：{e}")
finally:
    if 'client' in locals() and client.is_connected():
        client.close()


def compute_yearly_growth_rate(company_ranks: list, years: list) -> list:
    """
    计算公司每年排名的变化率（年比年增长）
    """
    growth_rates = []

    for i in range(1, len(company_ranks)):
        if company_ranks[i - 1] is None or company_ranks[i] is None:
            growth_rates.append(None)
        else:
            growth_rate = (company_ranks[i] - company_ranks[i - 1]) / company_ranks[i - 1]
            growth_rates.append(growth_rate)

    return growth_rates

