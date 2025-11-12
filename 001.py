import pandas as pd
import clickhouse_connect

# 数据库连接配置
# 你的ClickHouse连接信息
CLIENT_CONFIG = {
    'host': 'cc-2ze7189376o5m9759.public.clickhouse.ads.aliyuncs.com',
    'port': 8123,
    'user': 'xlab',
    'password': 'Xlab2025!',
    'database': 'opensource'
}

# Excel 文件名和列索引配置
excel_file_path = 'item.xlsx'
# B列的项目名，Excel列索引为1
b_column_index = 1
# E列开始写入，Excel列索引为4
start_column_index = 4

# SQL 查询模板
# 使用 {} 作为占位符，稍后会用项目名来替换
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

try:
    # 1. 连接到 ClickHouse 数据库
    client = clickhouse_connect.get_client(**CLIENT_CONFIG)
    print("数据库连接成功。")

    # 2. 读取 Excel 文件，获取项目名列表
    # header=None 表示Excel没有列名，所以B列的索引是1
    df = pd.read_excel(excel_file_path, sheet_name=0, header=None)

    # 提取B列数据，并转换为字符串列表以避免类型错误
    repo_names = df.iloc[1:, b_column_index].astype(str).tolist()

    print("开始处理仓库列表...")

    # 3. 遍历每个项目名并执行查询
    for index, repo_name in enumerate(repo_names):
        # 忽略空值或无效值
        if not repo_name or repo_name.lower() == 'nan':
            continue

        # 格式化SQL语句，处理可能包含的单引号和空格
        safe_repo_name = repo_name.strip().replace("'", "''")
        query = SQL_TEMPLATE.format(safe_repo_name)

        try:
            # 执行查询
            result = client.query(query)

            # 提取结果
            if result.row_count > 0:
                # 获取查询结果的四个字段
                description, primary_language, license, topics = result.first_row
                print(f"处理中... {repo_name} - 成功获取数据。")

                # 将结果写入DataFrame的对应位置
                # index + 1 是因为我们从Excel的第二行（索引1）开始读取，所以需要偏移
                df.iloc[index + 1, start_column_index] = description
                df.iloc[index + 1, start_column_index + 1] = primary_language
                df.iloc[index + 1, start_column_index + 2] = license
                df.iloc[index + 1, start_column_index + 3] = topics
            else:
                print(f"处理中... {repo_name} - 未找到匹配数据。")
                # 如果没有匹配数据，将这些单元格设置为空
                df.iloc[index + 1, start_column_index:start_column_index+4] = [None] * 4

        except Exception as e:
            print(f"查询 {repo_name} 时发生错误：{e}")
            df.iloc[index + 1, start_column_index:start_column_index+4] = ['查询失败'] * 4

    # 4. 保存修改后的 DataFrame 到原始 Excel 文件
    # 使用 pd.ExcelWriter 来覆盖写入指定列
    with pd.ExcelWriter(excel_file_path, engine='openpyxl', mode='w') as writer:
        df.to_excel(writer, index=False, header=False)

    print("\n程序执行完毕！")
    print(f"结果已成功写入 Excel 文件: {excel_file_path}")

except FileNotFoundError:
    print(f"错误：未找到文件 '{excel_file_path}'。请确保文件与脚本在同一目录下。")
except Exception as e:
    print(f"发生了一个错误：{e}")
finally:
    if 'client' in locals() and client.is_connected():
        client.close()