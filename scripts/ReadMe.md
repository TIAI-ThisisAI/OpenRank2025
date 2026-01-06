# **Global Open Source Data Analytics & Visualization Hub**

This folder serves as a centralized repository for high-fidelity frontend designs, data extraction scripts, and analytical tools. It bridges the gap between raw data collection (GitHub/Clickhouse) and professional visual presentation.

## **📂 Directory Structure & File Map**

| Category            | Key Files                                          | Description                                                  |
| :------------------ | :------------------------------------------------- | :----------------------------------------------------------- |
| **Data Extraction** | gh\_data\_extractor.py, norm4geo.py, Geo24hriFW.py | Scripts for fetching GitHub activity and normalizing geographical data. |
| **Database Ops**    | ClickhouseTest.py, PY4Clickhouse.py                | Utilities for connecting, testing, and managing Clickhouse database integrations. |
| **Analytics Core**  | analysis.py, trends.py, chinaOpenRank.py           | Core logic for processing trends and calculating metrics like OpenRank. |
| **Visualizations**  | GlobalPulse\_Analytics\_Pro.jsx                    | A high-fidelity React dashboard for real-time data monitoring. |
| **Knowledge Base**  | LLMs-Related.md, DataAnalysis-Tools.md             | Documentation on essential tools for LLM production and BI platforms. |
| **Data Assets**     | openrank\_chart\_data.json                         | Pre-processed datasets ready for frontend rendering.         |

## **🚀 Key Content**

### **1\. Data Intelligence & ETL**

This module handles the heavy lifting of data procurement and cleaning:

- **GitHub Extraction**: gh\_data\_extractor.py provides automated workflows to pull contributor and commit data.  
- **Geo-Normalization**: norm4geo.py and Geo24hriFW.py ensure that raw location strings are mapped to standard geographical formats for mapping.  
- **Clickhouse Integration**: Specialized Python drivers (PY4Clickhouse.py) optimized for high-performance OLAP queries.

### **2\. Analytics & Metrics Engine**

Detailed analysis of open-source ecosystems, with a focus on:

- **OpenRank Calculation**: chinaOpenRank.py (and v2) implements specific algorithms to measure influence within the Chinese open-source community.  
- **Trend Analysis**: trends.py and analysis.py process temporal data to identify emerging technologies and active repositories.  
- **Static Datasets**: openrank\_chart\_data.json provides the historical baseline for time-series visualizations.

### **3\. Professional Frontend & Dashboards**

High-fidelity UI components that bring data to life:

- **GlobalPulse Analytics Pro**: A state-of-the-art React component (.jsx) designed for executive-level data storytelling, featuring responsive layouts and deep data integration.  
- **Effect Demos**: Implementation of complex micro-interactions and motion design within the dashboard environment.

### **4\. Knowledge & Tooling Docs**

Curated resources for the modern data stack:

- **LLM Production**: LLMs-Related.md tracks industry-standard tools for deploying large language models.  
- **BI Integration**: DataAnalysis-Tools.md explores advanced configurations for platforms like Alibaba Cloud Quick BI.

## **🛠 Usage**

- **Data Engineers**: Start with the .py scripts to extract or normalize data, ensuring your GITHUB\_TOKEN is configured.  
- **Data Analysts**: Utilize the OpenRank and Trend scripts to generate insights from the openrank\_chart\_data.json.  
- **Frontend Developers**: Integrate GlobalPulse\_Analytics\_Pro.jsx into your React application for high-end visualization.  
- **Researchers**: Refer to the .md files for a deep dive into the technical stack and methodology.

*Maintained by TIAI Team.*
