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
from typing import List, Dict, Any, Optional, Generator

# ======================== 依赖检查 ========================
try:
    import aiohttp
    from tqdm.asyncio import tqdm
except ImportError:
    sys.stderr.write("❌ 错误: 缺少必要依赖。\n请运行: pip install aiohttp tqdm\n")
    sys.exit(1)

# ======================== 常量与 Schema ========================

API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"
ENV_API_KEY_NAME = "GEMINI_API_KEY"

# [核心逻辑] 定义 JSON Schema
# 作用：通过 strict mode 强制 Gemini 返回确定的 JSON 数组结构，
# 避免自然语言干扰，方便后续程序解析。
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

# ======================== 数据模型 ========================

@dataclass
class AppConfig:
    input_path: Optional[str]
    output_path: str
    api_key: str
    model_name: str
    batch_size: int
    concurrency: int
    max_retries: int
    cache_db_path: str
    target_column: str
    is_demo: bool
    verbose: bool

@dataclass
class GeoRecord:
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
        return self.total_inputs / self.elapsed if self.elapsed > 0.1 else 0.0
