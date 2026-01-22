# 🌐 Global Open Source Data Analytics & Visualization Hub

![Owner](https://img.shields.io/badge/Maintainer-TIAI_Team-blue)
![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![React](https://img.shields.io/badge/Frontend-React%20%7C%20Vite-61DAFB?logo=react&logoColor=black)
![Database](https://img.shields.io/badge/Database-ClickHouse-FFCC00?logo=clickhouse&logoColor=black)

**Scripts 目录** 是项目的核心引擎室。这里汇集了从底层数据清洗、核心算法计算到高保真前端呈现的全链路工具代码。

---

## 📂 目录结构 (File Map)

本目录按功能模块划分，形成了从“数据获取”到“价值呈现”的完整闭环。

```text
scripts/
├── 📥 Data Extraction (数据采集与清洗)
│   ├── gh_data_extractor.py       # GitHub Activity 数据自动化抓取脚本
│   ├── norm4geo.py                # 地理位置数据标准化工具 (Geo-Normalization)
│   └── Geo24hriFW.py              # 24小时地理数据流处理框架
│
├── 💾 Database Ops (数据库交互)
│   ├── PY4Clickhouse.py           # Clickhouse 高性能 Python 驱动封装
│   └── ClickhouseTest.py          # 数据库连接与查询单元测试
│
├── 🧠 Analytics Core (核心算法引擎)
│   ├── chinaOpenRank.py           # 中国区开源影响力 OpenRank 算法实现
│   ├── trends.py                  # 技术趋势时间序列分析
│   └── analysis.py                # 通用数据分析与指标聚合逻辑
│
├── 📊 Visualizations (前端可视化)
│   ├── GlobalPulse_Analytics_Pro.jsx  # [React] 高保真动态仪表盘组件
│   └── openrank_chart_data.json       # [Data] 预处理后的前端渲染数据源
│
└── 📘 Knowledge Base (知识库)
    ├── LLMs-Related.md            # LLM 生产化部署与工具链指南
    └── DataAnalysis-Tools.md      # BI 工具 (Tableau/QuickBI) 集成文档
