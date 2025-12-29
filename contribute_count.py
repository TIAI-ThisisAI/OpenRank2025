import pandas as pd
import clickhouse_connect
import time
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from time import sleep
import requests
import schedule
import time

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

def execute_query(client, repo_name, retry=3):
    """执行 SQL 查询并返回结果，支持重试机制"""
    safe_repo_name = repo_name.strip().replace("'", "''")
    query = SQL_TEMPLATE.format(safe_repo_name)
    
    # 记录查询开始时间
    query_start_time = time.time()
    
    for attempt in range(retry):
        try:
            result = client.query(query)
            # 记录查询结束时间并计算时间差
            query_end_time = time.time()
            query_duration = query_end_time - query_start_time
            logging.info(f"查询 {repo_name} 完成，用时 {query_duration:.2f} 秒。")
            return result
        except Exception as e:
            logging.warning(f"查询 {repo_name} 第 {attempt+1} 次尝试失败，错误：{e}")
            if attempt < retry - 1:
                sleep(2)  # 暂停 2 秒后重试
            else:
                logging.error(f"查询 {repo_name} 最终失败，错误：{e}")
                return None
            import concurrent.futures

def parallel_query_execution(client, repo_names):
    """并行执行多个仓库的查询"""
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_repo = {executor.submit(execute_query, client, repo_name): repo_name for repo_name in repo_names}
        for future in concurrent.futures.as_completed(future_to_repo):
            repo_name = future_to_repo[future]
            try:
                result = future.result()
                results.append((repo_name, result))
            except Exception as e:
                logging.error(f"查询 {repo_name} 失败: {e}")
                results.append((repo_name, None))
    return results


def process_query_result(result, repo_name, df, index):
    """处理查询结果并将数据写入 DataFrame"""
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

def clean_and_format_result(result):
    """清理查询结果数据，去除多余的空格和特殊字符"""
    if result:
        # 假设 result 是一个元组，包含 description, primary_language, license, topics
        cleaned_result = [str(item).strip() if item else "无数据" for item in result]
        return cleaned_result
    return ["无数据"] * 4

def send_slack_notification(message, webhook_url):
    """通过 Slack Webhook 发送通知"""
    payload = {
        "text": message
    }
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        logging.info("成功发送 Slack 通知。")
    except requests.exceptions.RequestException as e:
        logging.error(f"发送 Slack 通知失败: {e}")

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

def save_to_excel(df, excel_path):
    """将处理后的 DataFrame 保存到 Excel 文件"""
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl', mode='w') as writer:
            df.to_excel(writer, index=False, header=False)
        logging.info(f"结果已成功写入 Excel 文件: {excel_path}")
    except Exception as e:
        logging.error(f"保存 Excel 文件时发生错误：{e}")
        raise e


def send_email(subject, body, to_email):
    """发送邮件通知"""
    from_email = "your_email@example.com"  # 发送方邮箱
    from_password = "your_email_password"  # 发送方邮箱密码
    smtp_server = "smtp.example.com"  # 邮件服务器（例如Gmail的SMTP服务器）

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, 587)
        server.starttls()
        server.login(from_email, from_password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
        logging.info(f"成功发送邮件通知至 {to_email}")
    except Exception as e:
        logging.error(f"发送邮件失败: {e}")
        raise e

def save_to_csv(df, csv_file_path):
    """将处理后的 DataFrame 保存到 CSV 文件"""
    try:
        df.to_csv(csv_file_path, index=False, header=False, encoding='utf-8')
        logging.info(f"结果已成功写入 CSV 文件: {csv_file_path}")
    except Exception as e:
        logging.error(f"保存 CSV 文件时发生错误：{e}")
        raise e
def analyze_query_results(df):
    """对查询结果进行统计分析，计算各仓库的 OpenRank 平均值、最大值、最小值"""
    try:
        # 假设 OpenRank 数据在 DataFrame 的某一列中，这里以第4列为例
        openrank_column = df.iloc[:, start_column_index:start_column_index + 1]
        mean_value = openrank_column.mean()
        max_value = openrank_column.max()
        min_value = openrank_column.min()

        logging.info(f"OpenRank 平均值: {mean_value}, 最大值: {max_value}, 最小值: {min_value}")
        return mean_value, max_value, min_value
    except Exception as e:
        logging.error(f"分析查询结果时发生错误：{e}")
        raise e
        
def build_dynamic_sql_query(repo_name, fields=["description", "primary_language", "license", "topics"]):
    """构建动态 SQL 查询，支持查询不同字段"""
    fields_str = ", ".join(fields)
    query = f"""
    SELECT
        {fields_str}
    FROM
        opensource.events AS t1
    LEFT JOIN
        opensource.gh_repo_info AS t2
        ON t1.repo_id = t2.id
    WHERE
        t1.repo_name = '{repo_name}'
    LIMIT 1
    """
    return query

def generate_error_report(failed_repos, error_report_path):
    """生成查询失败仓库的错误报告"""
    try:
        with open(error_report_path, 'w') as f:
            f.write("查询失败的仓库:\n")
            for repo in failed_repos:
                f.write(f"{repo}\n")
        logging.info(f"查询失败的仓库已保存到错误报告文件: {error_report_path}")
    except Exception as e:
        logging.error(f"生成错误报告时发生错误：{e}")
        raise e

def update_incrementally(df, new_data):
    """根据新数据更新 DataFrame（增量更新）"""
    try:
        for index, row in new_data.iterrows():
            repo_name = row['repo_name']
            if repo_name in df['repo_name'].values:
                # 只更新已有仓库的行
                df.loc[df['repo_name'] == repo_name, ['description', 'primary_language', 'license', 'topics']] = row[['description', 'primary_language', 'license', 'topics']].values
            else:
                # 如果仓库不存在，则添加新行
                df = df.append(row, ignore_index=True)
        logging.info("增量数据更新完成。")
        return df
    except Exception as e:
        logging.error(f"增量更新数据时发生错误：{e}")
        raise e

def remove_duplicates(df, subset_columns):
    """去除 DataFrame 中指定列的重复项"""
    try:
        df_cleaned = df.drop_duplicates(subset=subset_columns)
        logging.info("数据去重完成。")
        return df_cleaned
    except Exception as e:
        logging.error(f"去重时发生错误：{e}")
        raise e

def filter_data(df, column_name, condition_value):
    """根据指定列的条件筛选 DataFrame 数据"""
    try:
        filtered_df = df[df[column_name] == condition_value]
        logging.info(f"数据过滤完成，筛选条件: {column_name} = {condition_value}")
        return filtered_df
    except Exception as e:
        logging.error(f"数据过滤时发生错误：{e}")
        raise e

import matplotlib.pyplot as plt

def plot_bar_chart(df, column_name, title="OpenRank 分布图", xlabel="仓库名称", ylabel="OpenRank 平均值"):
    """绘制条形图显示仓库的 OpenRank 分布"""
    try:
        # 取出仓库名和 OpenRank 平均值的列
        data = df[[column_name, 'repo_name']].groupby('repo_name').mean()
        
        # 绘制条形图
        plt.figure(figsize=(10, 6))
        data[column_name].plot(kind='bar', color='skyblue')
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # 保存图表
        chart_path = 'openrank_bar_chart.png'
        plt.savefig(chart_path)
        logging.info(f"条形图已保存为: {chart_path}")
        plt.close()
    except Exception as e:
        logging.error(f"绘制图表时发生错误：{e}")
        raise e


def run_scheduled_task(job_function, interval_minutes=60):
    """定时任务调度，每隔指定分钟执行一次任务"""
    schedule.every(interval_minutes).minutes.do(job_function)
    logging.info(f"任务已设置为每 {interval_minutes} 分钟执行一次。")

    while True:
        schedule.run_pending()
        time.sleep(1)

def batch_update_repository_info(df, updates):
    """批量更新 DataFrame 中仓库的信息"""
    try:
        for repo_name, update_values in updates.items():
            if repo_name in df['repo_name'].values:
                df.loc[df['repo_name'] == repo_name, ['description', 'primary_language', 'license', 'topics']] = update_values
                logging.info(f"更新仓库 {repo_name} 的信息。")
            else:
                logging.warning(f"仓库 {repo_name} 不在 DataFrame 中，跳过更新。")
        return df
    except Exception as e:
        logging.error(f"批量更新仓库信息时发生错误：{e}")
        raise e
        
def merge_dataframes(dfs):
    """合并多个 DataFrame"""
    try:
        merged_df = pd.concat(dfs, ignore_index=True)
        logging.info("DataFrame 合并完成。")
        return merged_df
    except Exception as e:
        logging.error(f"合并 DataFrame 时发生错误：{e}")
        raise e

def save_to_json(df, json_file_path):
    """将处理后的 DataFrame 保存为 JSON 文件"""
    try:
        df.to_json(json_file_path, orient='records', lines=True, force_ascii=False)
        logging.info(f"结果已成功写入 JSON 文件: {json_file_path}")
    except Exception as e:
        logging.error(f"保存 JSON 文件时发生错误：{e}")
        raise e

def generate_contributors_report(df):
    """生成仓库贡献者统计报告"""
    try:
        # 假设贡献者信息存储在 `contributors` 列中，并且数据格式是列表
        contributor_count = df['contributors'].apply(lambda x: len(eval(x)) if x else 0)
        df['contributor_count'] = contributor_count

        # 生成报告
        report = df[['repo_name', 'contributor_count']]
        report = report.sort_values(by='contributor_count', ascending=False)

        # 保存报告
        report_file = 'contributors_report.csv'
        report.to_csv(report_file, index=False)
        logging.info(f"贡献者统计报告已保存为: {report_file}")
    except Exception as e:
        logging.error(f"生成贡献者统计报告时发生错误：{e}")
        raise e

def filter_by_keywords(df, keyword_dict):
    """根据关键词过滤 DataFrame 中的仓库"""
    try:
        query = " & ".join([f"({column} == '{value}')" for column, value in keyword_dict.items()])
        filtered_df = df.query(query)
        logging.info(f"根据关键词过滤仓库完成，筛选条件: {keyword_dict}")
        return filtered_df
    except Exception as e:
        logging.error(f"根据关键词过滤仓库时发生错误：{e}")
        raise e
        
def generate_markdown_report(df, report_file='repositories_report.md'):
    """生成仓库信息的 Markdown 格式报告"""
    try:
        with open(report_file, 'w') as f:
            f.write("# 仓库信息报告\n\n")
            for _, row in df.iterrows():
                f.write(f"## {row['repo_name']}\n")
                f.write(f"**描述**: {row['description']}\n")
                f.write(f"**语言**: {row['primary_language']}\n")
                f.write(f"**许可证**: {row['license']}\n")
                f.write(f"**主题**: {row['topics']}\n")
                f.write("\n---\n")
        logging.info(f"Markdown 格式的报告已保存为: {report_file}")
    except Exception as e:
        logging.error(f"生成 Markdown 报告时发生错误：{e}")
        raise e


if __name__ == "__main__":
    main()
