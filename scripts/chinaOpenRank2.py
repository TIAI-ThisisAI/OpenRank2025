import json
import matplotlib.pyplot as plt
from matplotlib import rcParams

# 设置支持中文的字体
rcParams['font.sans-serif'] = ['SimHei']  # 黑体
rcParams['axes.unicode_minus'] = False    # 解决负号无法显示的问题

# 读取数据
with open('openrank_chart_Chinese_data_2.json', 'r') as f:
    data = json.load(f)

# 提取年份和公司数据
years = data['Year']
del data['Year']

# 定义最近的两年
recent_years = years[-2:]  # 最近两年

# 过滤出最近两年内至少一个有排名的公司
filtered_data = {
    company: ranks for company, ranks in data.items()
    if any(ranks[years.index(y)] is not None for y in recent_years)
}

# 获取最后一年的排名，按排名排序图例的顺序
final_year = years[-1]
final_year_ranks = {
    company: ranks[years.index(final_year)] if ranks[years.index(final_year)] is not None else float('inf')
    for company, ranks in filtered_data.items()
}

# 按最后一年排名排序公司，未在2024年前15的公司排在最后
sorted_companies = sorted(final_year_ranks, key=lambda x: (final_year_ranks[x] is None, final_year_ranks[x]))

# 绘制折线图
plt.figure(figsize=(14, 8))

for company in sorted_companies:
    ranks = filtered_data[company]
    plt.plot(years, ranks, marker='o', label=company)

plt.gca().invert_yaxis()  # 反转 y 轴，使排名 1 在顶部
plt.xticks(years)
plt.yticks(range(1, 16))
plt.title('Chinese Enterprise 2015~2024 OpenRank')
plt.xlabel('Time')
plt.ylabel('Rank')
plt.legend(title='symbol', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.tight_layout()
plt.show()
