"""
Gemini Geo Standardizer (Modularized Single-Script Version)
---------------------------------------------------------
功能：调用 Gemini API 将非结构化地理文本转换为标准结构化数据。
特点：
1. 单文件架构，易于部署。
2. 模块化设计：配置、存储、网络、IO、调度逻辑分离。
3. 具备缓存(SQLite)、并发控制(Semaphore)、断点续传能力。
"""

import asyncio
import csv
import json
import logging
import os
import signal
import sqlite3
import sys
import time
import re
import platform
from argparse import ArgumentParser, RawTextHelpFormatter
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# ==============================================================================
# MODULE 1: 依赖检查与环境配置
# ==============================================================================
# 作用：确保运行时具备必要的第三方库，避免运行时崩溃。

try:
    import aiohttp
    from tqdm.asyncio import tqdm
except ImportError:
    sys.stderr.write("❌ [环境错误] 缺少必要依赖。\n请运行安装命令: pip install aiohttp tqdm\n")
    sys.exit(1)
