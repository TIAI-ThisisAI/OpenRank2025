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

# ==============================================================================
# MODULE 2: 常量定义与 Prompt 工程
# ==============================================================================
# 作用：集中管理 API 配置、JSON Schema 和 System Prompt，方便调整模型行为。

API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"
ENV_API_KEY_NAME = "GEMINI_API_KEY"

# [Prompt工程] 严格模式 Schema
# 目的：强制 Gemini 返回确定的 JSON 数组结构，减少解析错误。
LOCATION_RESPONSE_SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "input_location": {"type": "STRING"},
            "city": {"type": "STRING"},
            "subdivision": {"type": "STRING"},
            "country_alpha2": {"type": "STRING"},
            "country_alpha3": {"type": "STRING"},
            "confidence": {"type": "NUMBER"},
            "reasoning": {"type": "STRING"}
        },
        "required": ["input_location", "country_alpha3", "confidence"]
    }
}

SYSTEM_PROMPT = (
    "您是一个高精度的地理信息标准化引擎。\n"
    "任务：将输入的地理描述列表转换为标准的结构化数据。\n"
    "规则：\n"
    "1. 严格遵守 JSON Schema，返回 JSON 数组。\n"
    "2. country_alpha3 必须符合 ISO 3166-1 Alpha-3 标准。\n"
    "3. 无法识别的输入，country_alpha3='UNK'，confidence=0。\n"
    "4. 仅输出纯 JSON，不要包含 Markdown 标记。"
)

# ==============================================================================
# MODULE 3: 数据模型 (Data Models)
# ==============================================================================
# 作用：定义应用程序内部流转的数据结构，使用 dataclass 减少样板代码。

@dataclass
class AppConfig:
    """应用程序全局配置对象"""
    input_path: Optional[str]   # 输入文件路径
    output_path: str            # 输出文件路径
    api_key: str                # Gemini API Key
    model_name: str             # 模型名称
    batch_size: int             # 单次请求的地理位置数量
    concurrency: int            # 并发协程数量
    max_retries: int            # 最大重试次数
    cache_db_path: str          # SQLite缓存路径
    target_column: str          # CSV/JSON中的目标字段名
    is_demo: bool               # 是否为演示模式
    verbose: bool               # 是否开启详细日志

@dataclass
class GeoRecord:
    """单条地理位置记录实体"""
    input_location: str
    city: str = ""
    subdivision: str = ""
    country_alpha2: str = ""
    country_alpha3: str = "UNK"
    confidence: float = 0.0
    reasoning: str = ""
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class Statistics:
    """运行时统计信息"""
    total_inputs: int = 0
    unique_inputs: int = 0
    cached_hits: int = 0
    api_processed: int = 0
    api_errors: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def speed(self) -> float:
        """计算处理速度 (条/秒)"""
        return self.total_inputs / self.elapsed if self.elapsed > 0.1 else 0.0

# ==============================================================================
# MODULE 4: 工具与日志 (Utils & Logging)
# ==============================================================================

def setup_logger(verbose: bool) -> logging.Logger:
    """配置控制台日志输出"""
    logger = logging.getLogger("GeoStandardizer")
    # 避免重复添加 handler
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%H:%M:%S"))
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logger

