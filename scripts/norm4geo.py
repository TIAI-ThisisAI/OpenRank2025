# -*- coding: utf-8 -*-
"""
Gemini 地理信息标准化专业版 (GeoStandardizer Pro)

功能描述:
    利用 Google Gemini API 将非结构化的地名列表清洗为标准的结构化地理数据。
    具备高性能并发、本地 SQLite 缓存、断点续传及详细的统计报告功能。

架构设计:
    - Config: 配置管理
    - StorageEngine: 数据持久化层 (SQLite)
    - GeminiClient: API 交互层 (Async HTTP)
    - IOHandler: 文件输入输出处理
    - BatchProcessor: 核心工作流控制器

依赖:
    pip install aiohttp tqdm
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
from argparse import ArgumentParser, RawTextHelpFormatter
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple, Generator, Iterable

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

# 定义严格的输出 Schema，确保模型返回格式可控
LOCATION_RESPONSE_SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "input_location": {"type": "STRING", "description": "原始输入文本"},
            "city": {"type": "STRING", "description": "城市或地方名称"},
            "subdivision": {"type": "STRING", "description": "省、州或一级行政区"},
            "country_alpha2": {"type": "STRING", "description": "ISO 3166-1 Alpha-2 代码"},
            "country_alpha3": {"type": "STRING", "description": "ISO 3166-1 Alpha-3 代码"},
            "confidence": {"type": "NUMBER", "description": "0.0-1.0 之间的置信度"},
            "reasoning": {"type": "STRING", "description": "简短的推断依据"}
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
    "3. 如果输入是'California'，subdivision='California', country_alpha3='USA'。\n"
    "4. 无法识别的输入，country_alpha3='UNK'，confidence=0。\n"
    "5. 不要输出 Markdown 标记（如 ```json），仅输出纯文本 JSON。"
)

# ======================== 数据模型与配置 ========================

@dataclass
class AppConfig:
    """应用程序配置对象"""
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
    """标准化的地理数据记录"""
    input_location: str
    city: str = ""
    subdivision: str = ""
    country_alpha2: str = ""
    country_alpha3: str = "UNK"
    confidence: float = 0.0
    reasoning: str = ""
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['updated_at'] = d['updated_at'].isoformat()
        return d

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
        if self.elapsed < 0.1: return 0.0
        return self.total_inputs / self.elapsed

# ======================== 日志工具 ========================

def setup_logger(verbose: bool) -> logging.Logger:
    """配置全局日志"""
    logger = logging.getLogger("GeoStandardizer")
    level = logging.DEBUG if verbose else logging.INFO
    
    # 清除旧的 handlers
    if logger.handlers:
        logger.handlers.clear()
        
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s", 
        datefmt="%H:%M:%S"
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger

# ======================== 数据持久层 ========================

class StorageEngine:
    """
    基于 SQLite 的缓存引擎。
    负责数据的去重、缓存读取与结果持久化。
    """
    def __init__(self, db_path: str, logger: logging.Logger):
        self.db_path = db_path
        self.logger = logger
        self._conn: Optional[sqlite3.Connection] = None
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        if not self._conn:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            # 开启 WAL 模式以提高并发读写性能
            self._conn.execute("PRAGMA journal_mode=WAL;")
        return self._conn

    def _init_schema(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS geo_cache (
                    input_text TEXT PRIMARY KEY,
                    city TEXT,
                    subdivision TEXT,
                    country_alpha2 TEXT,
                    country_alpha3 TEXT,
                    confidence REAL,
                    reasoning TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def get_cached_records(self, inputs: List[str]) -> Dict[str, GeoRecord]:
        """批量获取缓存数据"""
        if not inputs:
            return {}
        
        results = {}
        conn = self._get_conn()
        
        # SQLite 默认变量限制通常是 999 或 32766，分块查询更安全
        chunk_size = 900
        for i in range(0, len(inputs), chunk_size):
            chunk = inputs[i:i + chunk_size]
            placeholders = ','.join(['?'] * len(chunk))
            try:
                cursor = conn.execute(
                    f"SELECT * FROM geo_cache WHERE input_text IN ({placeholders})", 
                    chunk
                )
                rows = cursor.fetchall()
                # 获取列名映射
                cols = [desc[0] for desc in cursor.description]
                
                for row in rows:
                    data = dict(zip(cols, row))
                    # 转换回 GeoRecord 对象
                    results[data['input_text']] = GeoRecord(
                        input_location=data['input_text'],
                        city=data.get('city', ''),
                        subdivision=data.get('subdivision', ''),
                        country_alpha2=data.get('country_alpha2', ''),
                        country_alpha3=data.get('country_alpha3', 'UNK'),
                        confidence=data.get('confidence', 0.0),
                        reasoning=data.get('reasoning', ''),
                        updated_at=data['updated_at'] # 保持原始字符串或转换皆可，此处主要用于展示
                    )
            except sqlite3.Error as e:
                self.logger.error(f"数据库读取错误: {e}")

        return results

    def save_batch(self, records: List[GeoRecord]):
        """批量写入或更新缓存"""
        if not records:
            return

        sql = """
            INSERT OR REPLACE INTO geo_cache 
            (input_text, city, subdivision, country_alpha2, country_alpha3, confidence, reasoning, updated_at) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        data = [
            (
                r.input_location, r.city, r.subdivision, r.country_alpha2, 
                r.country_alpha3, r.confidence, r.reasoning, datetime.now()
            ) 
            for r in records
        ]
        
        try:
            conn = self._get_conn()
            conn.executemany(sql, data)
            conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"数据库写入错误: {e}")

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

# ======================== API 客户端 ========================

class GeminiClient:
    """
    Gemini API 交互客户端。
    处理请求构建、重试逻辑及错误解析。
    """
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.endpoint = f"{API_BASE_URL}/{config.model_name}:generateContent?key={config.api_key}"

    def _build_payload(self, batch_inputs: List[str]) -> Dict:
        prompt_text = f"请标准化以下地点清单：\n{json.dumps(batch_inputs, ensure_ascii=False)}"
        return {
            "contents": [{"parts": [{"text": prompt_text}]}],
            "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": LOCATION_RESPONSE_SCHEMA
            }
        }

    async def standardize_batch(self, session: aiohttp.ClientSession, batch: List[str]) -> List[GeoRecord]:
        """
        发送 API 请求并返回解析后的记录列表。
        包含指数退避重试机制。
        """
        payload = self._build_payload(batch)
        
        for attempt in range(self.config.max_retries):
            try:
                async with session.post(self.endpoint, json=payload, timeout=60) as response:
                    if response.status == 429:
                        wait_time = (2 ** attempt) + 1
                        self.logger.debug(f"API 限流 (429)，休眠 {wait_time}s 后重试...")
                        await asyncio.sleep(wait_time)
                        continue

                    if response.status != 200:
                        error_msg = await response.text()
                        self.logger.warning(f"API 错误 [{response.status}]: {error_msg[:100]}...")
                        # 5xx 错误才重试，4xx 直接跳过
                        if 500 <= response.status < 600:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        else:
                            break 

                    data = await response.json()
                    return self._parse_response(data, batch)

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.logger.warning(f"网络异常 (尝试 {attempt+1}/{self.config.max_retries}): {e}")
                await asyncio.sleep(2 ** attempt)

        # 最终失败，返回 fallback 记录
        self.logger.error(f"批次处理失败: {batch[:3]}...")
        return [self._create_fallback_record(loc, "API processing failed") for loc in batch]

    def _parse_response(self, api_response: Dict, original_batch: List[str]) -> List[GeoRecord]:
        """解析 API 返回的 JSON，并处理可能的遗漏或格式错误"""
        records = []
        try:
            candidates = api_response.get('candidates', [])
            if not candidates:
                raise ValueError("No candidates returned")
            
            content_text = candidates[0]['content']['parts'][0]['text']
            raw_data = json.loads(content_text)
            
            # 建立映射以防乱序
            result_map = {item['input_location']: item for item in raw_data}
            
            for input_loc in original_batch:
                if input_loc in result_map:
                    item = result_map[input_loc]
                    records.append(GeoRecord(
                        input_location=item['input_location'],
                        city=item.get('city', ''),
                        subdivision=item.get('subdivision', ''),
                        country_alpha2=item.get('country_alpha2', ''),
                        country_alpha3=item.get('country_alpha3', 'UNK'),
                        confidence=item.get('confidence', 0),
                        reasoning=item.get('reasoning', '')
                    ))
                else:
                    records.append(self._create_fallback_record(input_loc, "Model skipped item"))
                    
        except Exception as e:
            self.logger.error(f"响应解析失败: {e}")
            # 解析失败则全部回退
            return [self._create_fallback_record(loc, f"Parse error: {str(e)}") for loc in original_batch]
            
        return records

    @staticmethod
    def _create_fallback_record(location: str, reason: str) -> GeoRecord:
        return GeoRecord(
            input_location=location,
            country_alpha3="ERROR",
            reasoning=reason
        )

# ======================== IO 处理 ========================

class IOHandler:
    """负责文件的读取和结果导出"""
    
    @staticmethod
    def read_input(path_str: Optional[str], column: str, is_demo: bool) -> List[str]:
        if is_demo:
            return ["New York", "London", "上海", "Tokyo", "Berlin", "Paris", "California", "UnknownCity123"] * 5
        
        if not path_str:
            raise ValueError("未指定输入文件")

        path = Path(path_str)
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {path}")

        data = []
        # 处理 CSV
        if path.suffix.lower() == '.csv':
            try:
                with open(path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames:
                        raise ValueError("CSV 为空或无表头")
                    
                    # 智能判断列名：如果指定的列不存在，尝试使用第一列
                    target_col = column if column in reader.fieldnames else reader.fieldnames[0]
                    
                    for row in reader:
                        if val := row.get(target_col):
                            data.append(str(val).strip())
            except UnicodeDecodeError:
                raise ValueError("文件编码错误，请使用 UTF-8")
        
        # 处理 JSON
        elif path.suffix.lower() == '.json':
            with open(path, 'r', encoding='utf-8') as f:
                content = json.load(f)
                if isinstance(content, list):
                    data = [str(x) for x in content]
                elif isinstance(content, dict):
                    data = [str(v) for v in content.values()]
        else:
            # 默认按行读取文本
            with open(path, 'r', encoding='utf-8') as f:
                data = [line.strip() for line in f if line.strip()]

        return [d for d in data if d]

    @staticmethod
    def export_results(results: List[GeoRecord], output_path: str):
        path = Path(output_path)
        dicts = [r.to_dict() for r in results]
        fieldnames = ["input_location", "city", "subdivision", "country_alpha2", "country_alpha3", "confidence", "reasoning", "updated_at"]
        
        path.parent.mkdir(parents=True, exist_ok=True)
        
        if path.suffix.lower() == '.json':
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(dicts, f, ensure_ascii=False, indent=2)
        else:
            # 默认导出 CSV
            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(dicts)

# ======================== 核心控制器 ========================

class BatchProcessor:
    """
    核心工作流控制器。
    协调 IO、缓存、API 和并发处理。
    """
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = setup_logger(config.verbose)
        self.stats = Statistics()
        self.storage = StorageEngine(config.cache_db_path, self.logger)
        self.client = GeminiClient(config, self.logger)
        self.is_running = True
        
        # 信号注册
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, sig, frame):
        if self.is_running:
            self.logger.warning("\n🛑 接收到停止信号，正在保存进度并优雅退出...")
            self.is_running = False

    def _batch_generator(self, data: List[str], batch_size: int) -> Generator[List[str], None, None]:
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]

    async def _process_worker(self, session: aiohttp.ClientSession, batch: List[str], semaphore: asyncio.Semaphore, pbar: tqdm):
        """单个工作协程：请求 API -> 保存 DB -> 更新进度条"""
        async with semaphore:
            if not self.is_running: return

            records = await self.client.standardize_batch(session, batch)
            
            # 统计成功与失败
            valid_count = sum(1 for r in records if r.country_alpha3 != 'ERROR')
            self.stats.api_processed += valid_count
            self.stats.api_errors += (len(records) - valid_count)

            # 持久化
            self.storage.save_batch(records)
            pbar.update(len(batch))

    async def run(self):
        try:
            # 1. 加载数据
            self.logger.info("正在读取输入数据...")
            raw_inputs = IOHandler.read_input(self.config.input_path, self.config.target_column, self.config.is_demo)
            
            self.stats.total_inputs = len(raw_inputs)
            unique_inputs = list(dict.fromkeys(raw_inputs)) # 保持顺序去重
            self.stats.unique_inputs = len(unique_inputs)

            if not raw_inputs:
                self.logger.warning("输入数据为空，任务结束。")
                return

            # 2. 检查缓存
            self.logger.info("正在比对本地缓存...")
            cached_map = self.storage.get_cached_records(unique_inputs)
            self.stats.cached_hits = len(cached_map)
            
            # 筛选待处理列表
            to_process = [loc for loc in unique_inputs if loc not in cached_map]
            
            self.logger.info(
                f"任务概览 | 总量: {self.stats.total_inputs} | 唯一: {self.stats.unique_inputs} | "
                f"已缓存: {self.stats.cached_hits} | 待请求: {len(to_process)}"
            )

            # 3. 并发处理
            if to_process:
                concurrency_sem = asyncio.Semaphore(self.config.concurrency)
                batches = list(self._batch_generator(to_process, self.config.batch_size))
                
                async with aiohttp.ClientSession() as session:
                    tasks = []
                    with tqdm(total=len(to_process), desc="API 处理进度", unit="loc") as pbar:
                        for batch in batches:
                            if not self.is_running: break
                            task = asyncio.create_task(
                                self._process_worker(session, batch, concurrency_sem, pbar)
                            )
                            tasks.append(task)
                        
                        # 等待所有任务完成
                        await asyncio.gather(*tasks)
            
            # 4. 结果整合与导出
            if self.is_running:
                self.logger.info("正在整合最终结果...")
                # 重新获取所有数据的完整记录
                final_cache = self.storage.get_cached_records(list(set(raw_inputs)))
                final_results = [
                    final_cache.get(loc, GeoRecord(input_location=loc, country_alpha3="MISSING")) 
                    for loc in raw_inputs
                ]
                
                IOHandler.export_results(final_results, self.config.output_path)
                self._print_summary()

        except Exception as e:
            self.logger.error(f"严重运行时错误: {e}", exc_info=self.config.verbose)
        finally:
            self.storage.close()

    def _print_summary(self):
        self.logger.info("=" * 40)
        self.logger.info("✅ 处理完成")
        self.logger.info(f"耗时: {self.stats.elapsed:.2f} 秒")
        self.logger.info(f"平均处理速度: {self.stats.speed:.1f} 条/秒")
        self.logger.info(f"API 调用成功: {self.stats.api_processed} | 失败: {self.stats.api_errors}")
        self.logger.info(f"结果已保存至: {Path(self.config.output_path).absolute()}")
        self.logger.info("=" * 40)

# ======================== 入口函数 ========================

def main():
    parser = ArgumentParser(description="Gemini 地理标准化引擎 (专业版)", formatter_class=RawTextHelpFormatter)
    
    # 基础参数
    base_group = parser.add_argument_group("基础设置")
    base_group.add_argument("--input", "-i", help="输入文件路径 (支持 CSV, JSON, TXT)")
    base_group.add_argument("--output", "-o", default="geo_output_pro.csv", help="结果输出路径")
    base_group.add_argument("--key", "-k", default=os.environ.get(ENV_API_KEY_NAME), help=f"API Key (默认读取环境变量 {ENV_API_KEY_NAME})")
    
    # 性能参数
    perf_group = parser.add_argument_group("性能调优")
    perf_group.add_argument("--model", default="gemini-2.5-flash-preview-09-2025", help="使用的 Gemini 模型版本")
    perf_group.add_argument("--batch", "-b", type=int, default=30, help="批处理大小 (推荐 20-50)")
    perf_group.add_argument("--concurrency", "-c", type=int, default=5, help="并发协程数")
    perf_group.add_argument("--retry", type=int, default=3, help="API 失败重试次数")
    
    # 其他选项
    misc_group = parser.add_argument_group("其他选项")
    misc_group.add_argument("--cache", default="geo_cache_v2.db", help="SQLite 缓存数据库路径")
    misc_group.add_argument("--column", default="location", help="CSV 中的目标列名")
    misc_group.add_argument("--demo", action="store_true", help="使用内置测试数据运行")
    misc_group.add_argument("--verbose", "-v", action="store_true", help="启用详细调试日志")

    args = parser.parse_args()

    # 参数校验
    if not args.key:
        parser.error(f"未提供 API Key。请设置环境变量 {ENV_API_KEY_NAME} 或使用 --key 参数。")
    
    if not args.input and not args.demo:
        parser.error("需要提供输入文件 (--input) 或使用演示模式 (--demo)。")

    # 构建配置对象
    config = AppConfig(
        input_path=args.input,
        output_path=args.output,
        api_key=args.key,
        model_name=args.model,
        batch_size=args.batch,
        concurrency=args.concurrency,
        max_retries=args.retry,
        cache_db_path=args.cache,
        target_column=args.column,
        is_demo=args.demo,
        verbose=args.verbose
    )

    # 启动应用
    processor = BatchProcessor(config)
    try:
        asyncio.run(processor.run())
    except KeyboardInterrupt:
        pass  # 已由信号处理程序处理，此处只需捕获以避免打印堆栈

if __name__ == "__main__":
    main()
