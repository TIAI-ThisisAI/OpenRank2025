import matplotlib.pyplot as plt
import seaborn as sns
import time
import logging

# 设置日志配置
logging.basicConfig(filename='query_errors.log', level=logging.ERROR)

# 计算查询执行时间
start_time = time.time()

try:
    print("\n正在执行数据库查询...")
    result = client.query(sql_query)

    # 将查询结果转换为 Pandas DataFrame
    df_result = pd.DataFrame(result.result_rows, columns=result.column_names)

    if not df_result.empty:
        # 关键步骤：将 'month' 列转换为日期时间类型
        df_result['month'] = pd.to_datetime(df_result['month'])

        # 将日期列格式化为 'YYYY-MM' 格式，作为新列名
        df_result['month'] = df_result['month'].dt.strftime('%Y-%m')

        # 核心步骤：使用 pivot_table 将数据透视成您要的格式
        df_pivot = df_result.pivot_table(
            index='repo_name',
            columns='month',
            values='monthly_avg_openrank'
        )

        # 重新排序列，让月份按时间顺序排列
        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)

        # 将结果保存到新的 Excel 文件
        output_file_path = 'multi_repo_openrank_summary.xlsx'
        df_pivot.to_excel(output_file_path)
        print("\n查询成功！")
        print(f"结果已保存到新的 Excel 文件: {output_file_path}")
        
        # 可视化数据（热力图）
        plt.figure(figsize=(10, 6))
        sns.heatmap(df_pivot, annot=True, cmap="YlGnBu", fmt=".4f", linewidths=0.5)
        plt.title('Monthly Average OpenRank of Repositories')
        plt.ylabel('Repository Name')
        plt.xlabel('Month')
        plt.tight_layout()

        # 保存热力图为文件
        heatmap_file_path = 'openrank_heatmap.png'
        plt.savefig(heatmap_file_path)
        print(f"热力图已保存为: {heatmap_file_path}")
        
    else:
        print("\n查询完成，但未找到任何数据。")

except Exception as e:
    print("\n查询失败，请检查数据库连接或 SQL 语句。")
    print(f"错误信息：{e}")
    logging.error(f"查询失败: {str(e)}")

# 计算查询时间
end_time = time.time()
execution_time = end_time - start_time
print(f"\n查询执行时间: {execution_time:.2f} 秒")
