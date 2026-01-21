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

# ==============================================================================
# MODULE 5: 持久化层 (Persistence Layer)
# ==============================================================================
# 作用：封装 SQLite 操作，负责缓存的读写。使用 WAL 模式优化并发性能。

class StorageEngine:
    """
    SQLite 存储引擎
    
    关键特性：
    1. 启用 WAL (Write-Ahead Logging) 模式，支持高并发读写。
    2. 使用批量插入 (executemany) 提高写入性能。
    3. 自动处理 SQL 变量限制的分块查询。
    """
    
    def __init__(self, db_path: str, logger: logging.Logger):
        self.db_path = db_path
        self.logger = logger
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """获取数据库连接（懒加载）"""
        if not self._conn:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            # [优化] 开启 WAL 模式，极大提高并发下的数据库吞吐量
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;") 
        return self._conn

    def _init_db(self):
        """初始化数据表结构"""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS geo_cache (
                    input_text TEXT PRIMARY KEY,
                    city TEXT, subdivision TEXT, country_alpha2 TEXT, country_alpha3 TEXT,
                    confidence REAL, reasoning TEXT, updated_at TEXT
                )
            """)

    def get_cached_records(self, inputs: List[str]) -> Dict[str, GeoRecord]:
        """
        批量获取缓存记录
        
        Args:
            inputs: 原始输入文本列表
        Returns:
            Dict: key为输入文本, value为GeoRecord对象
        """
        if not inputs: return {}
        results = {}
        conn = self._get_conn()
        
        # [防错] SQLite 默认限制 SQL 语句变量数为 999，需分块查询
        chunk_size = 900 
        
        try:
            for i in range(0, len(inputs), chunk_size):
                chunk = inputs[i:i + chunk_size]
                # 动态构建 SQL 参数占位符
                placeholders = ','.join(['?'] * len(chunk))
                query = f"SELECT * FROM geo_cache WHERE input_text IN ({placeholders})"
                
                cursor = conn.execute(query, chunk)
                cols = [d[0] for d in cursor.description]
                
                for row in cursor:
                    d = dict(zip(cols, row))
                    results[d['input_text']] = GeoRecord(**d)
        except sqlite3.Error as e:
            self.logger.error(f"Cache read error: {e}")
            
        return results

    def save_batch(self, records: List[GeoRecord]):
        """批量保存或更新记录"""
        if not records: return
        sql = """
            INSERT OR REPLACE INTO geo_cache 
            (input_text, city, subdivision, country_alpha2, country_alpha3, confidence, reasoning, updated_at) 
            VALUES (:input_location, :city, :subdivision, :country_alpha2, :country_alpha3, :confidence, :reasoning, :updated_at)
        """
        try:
            conn = self._get_conn()
            # [优化] 使用事务批量提交
            conn.executemany(sql, [r.to_dict() for r in records])
            conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Cache write error: {e}")

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

# ==============================================================================
# MODULE 6: 网络层 (Network Layer)
# ==============================================================================
# 作用：封装 Gemini API 调用，处理 HTTP 请求、重试逻辑、速率限制和 JSON 解析。

class GeminiClient:
    """
    Gemini API 客户端
    
    关键特性：
    1. 指数退避 (Exponential Backoff) 处理 429 限流和 5xx 服务端错误。
    2. 鲁棒的 JSON 清洗逻辑，处理 LLM 可能返回的 Markdown 标记。
    """
    
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.url = f"{API_BASE_URL}/{config.model_name}:generateContent?key={config.api_key}"

    def _clean_json_string(self, text: str) -> str:
        """清洗 LLM 返回的文本，移除 ```json 包裹"""
        text = re.sub(r'^```json\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'^```\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'```\s*$', '', text, flags=re.MULTILINE)
        return text.strip()

    async def standardize_batch(self, session: aiohttp.ClientSession, batch: List[str]) -> List[GeoRecord]:
        """异步处理单个批次"""
        payload = {
            "contents": [{"parts": [{"text": f"Input List: {json.dumps(batch, ensure_ascii=False)} "}]}],
            "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": LOCATION_RESPONSE_SCHEMA
            }
        }

        # [健壮性] 重试循环
        for attempt in range(self.config.max_retries):
            try:
                async with session.post(self.url, json=payload, timeout=60) as resp:
                    # 429 Too Many Requests: 触发退避等待
                    if resp.status == 429:
                        delay = (2 ** attempt) + 1
                        await asyncio.sleep(delay)
                        continue
                    
                    if resp.status != 200:
                        err_text = await resp.text()
                        # 5xx Server Error: 可重试
                        if 500 <= resp.status < 600:
                            self.logger.warning(f"Server Error {resp.status}, retrying...")
                            await asyncio.sleep(2 ** attempt)
                            continue
                        # 4xx Client Error: 不可重试，直接报错退出当前批次
                        self.logger.error(f"API Error {resp.status}: {err_text[:200]}")
                        break 

                    data = await resp.json()
                    return self._parse_response(data, batch)

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.logger.warning(f"Network error (Try {attempt+1}/{self.config.max_retries}): {e}")
                await asyncio.sleep(2 ** attempt)

        # 兜底返回：防止整个程序因单个批次失败而崩溃
        return [GeoRecord(loc, reasoning="API Failed") for loc in batch]

    def _parse_response(self, data: Dict, original_batch: List[str]) -> List[GeoRecord]:
        """解析 API 返回的 JSON 数据并映射回原始输入"""
        try:
            content = data['candidates'][0]['content']['parts'][0]['text']
            clean_content = self._clean_json_string(content)
            items = json.loads(clean_content)
            
            # [映射逻辑] 建立 hash map 以便 O(1) 查找
            result_map = {item.get('input_location'): item for item in items}
            records = []
            
            for loc in original_batch:
                if item := result_map.get(loc):
                    # 过滤掉不在 dataclass 定义中的多余字段，防止报错
                    valid_keys = GeoRecord.__dataclass_fields__.keys()
                    filtered_data = {k: v for k, v in item.items() if k in valid_keys}
                    filtered_data['input_location'] = loc 
                    records.append(GeoRecord(**filtered_data))
                else:
                    records.append(GeoRecord(loc, reasoning="Model skipped item"))
            return records

        except (KeyError, json.JSONDecodeError, IndexError) as e:
            self.logger.error(f"Parsing failed: {e}")
            return [GeoRecord(loc, reasoning="Parse Error") for loc in original_batch]

# ==============================================================================
# MODULE 7: 输入输出层 (IO Layer)
# ==============================================================================
# 作用：处理不同格式文件（CSV, JSON, TXT）的读取和写入。

class IOHandler:
    @staticmethod
    def read_input(config: AppConfig) -> List[str]:
        """读取输入文件，根据后缀名自动适配策略"""
        if config.is_demo:
            return ["New York", "London", "Shanghai", "Tokyo", "Berlin"] * 6
        
        path = Path(config.input_path)
        if not path.exists(): raise FileNotFoundError(f"{path} not found")

        try:
            # 策略1: CSV
            if path.suffix.lower() == '.csv':
                with open(path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames: return []
                    col = config.target_column if config.target_column in reader.fieldnames else reader.fieldnames[0]
                    return [row[col].strip() for row in reader if row.get(col)]
            
            # 策略2: JSON
            elif path.suffix.lower() == '.json':
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return [str(x) for x in (data if isinstance(data, list) else data.values())]
            
            # 策略3: TXT / 其他
            else:
                with open(path, 'r', encoding='utf-8') as f:
                    return [line.strip() for line in f if line.strip()]
        except Exception as e:
            raise ValueError(f"Error reading file: {e}")

    @staticmethod
    def save_output(results: List[GeoRecord], path_str: str):
        """保存处理结果"""
        path = Path(path_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = [r.to_dict() for r in results]
        
        if path.suffix.lower() == '.json':
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        else:
            if not data: return
            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
