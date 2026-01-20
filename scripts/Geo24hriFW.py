"""
GitHub 高性能数据采集系统 (Single File Modularized Edition)

设计理念:
    1. [Config] 配置模块: 管理常量、环境变量和 API 模板
    2. [Models] 模型模块: 定义数据结构和序列化逻辑
    3. [Infrastructure] 基础设施模块: 处理数据库和 Token 资源池
    4. [Core] 核心逻辑模块: 实现生产者-消费者采集引擎
    5. [Main] 入口模块: 依赖检查、任务编排与生命周期管理
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Any

# ==============================================================================
# 0. 全局初始化与依赖检查
# ==============================================================================

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("GH-Collector")

# 第三方依赖检查
try:
    import aiohttp
    import aiosqlite
    from tqdm.asyncio import tqdm
except ImportError:
    print("错误: 缺少必要依赖。请执行: pip install aiohttp aiosqlite tqdm")
    sys.exit(1)
