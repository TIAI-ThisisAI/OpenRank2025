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

# 过滤出最近三年有排名的公司
recent_years = years[-3:]  # 最近三年
filtered_data = {company: ranks for company, ranks in data.items() if any(ranks[years.index(y)] is not None for y in recent_years)}

# 绘制折线图
plt.figure(figsize=(14, 8))

for company, ranks in filtered_data.items():
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
