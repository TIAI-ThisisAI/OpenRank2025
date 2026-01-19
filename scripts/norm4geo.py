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

# ======================== 日志与存储 ========================

def setup_logger(verbose: bool) -> logging.Logger:
    logger = logging.getLogger("GeoStandardizer")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%H:%M:%S"))
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logger

class StorageEngine:
    """SQLite 存储引擎，负责缓存管理"""
    def __init__(self, db_path: str, logger: logging.Logger):
        self.db_path = db_path
        self.logger = logger
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not self._conn:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            # [性能优化] 开启 WAL 模式 (Write-Ahead Logging)
            # 允许读写并发执行，极大提高多线程/异步环境下的数据库性能
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;") 
        return self._conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS geo_cache (
                    input_text TEXT PRIMARY KEY,
                    city TEXT, subdivision TEXT, country_alpha2 TEXT, country_alpha3 TEXT,
                    confidence REAL, reasoning TEXT, updated_at TEXT
                )
            """)

    def get_cached_records(self, inputs: List[str]) -> Dict[str, GeoRecord]:
        if not inputs: return {}
        results = {}
        conn = self._get_conn()
        chunk_size = 900 # [防错逻辑] SQLite 默认限制 SQL 语句变量数为 999，分块查询防止超限
        
        try:
            for i in range(0, len(inputs), chunk_size):
                chunk = inputs[i:i + chunk_size]
                # 动态构建 SQL，利用 input_text 的索引进行快速查找
                q = f"SELECT * FROM geo_cache WHERE input_text IN ({','.join(['?']*len(chunk))})"
                cursor = conn.execute(q, chunk)
                cols = [d[0] for d in cursor.description]
                
                for row in cursor:
                    d = dict(zip(cols, row))
                    results[d['input_text']] = GeoRecord(**d)
        except sqlite3.Error as e:
            self.logger.error(f"Cache read error: {e}")
            
        return results

    def save_batch(self, records: List[GeoRecord]):
        if not records: return
        sql = """
            INSERT OR REPLACE INTO geo_cache 
            (input_text, city, subdivision, country_alpha2, country_alpha3, confidence, reasoning, updated_at) 
            VALUES (:input_location, :city, :subdivision, :country_alpha2, :country_alpha3, :confidence, :reasoning, :updated_at)
        """
        try:
            conn = self._get_conn()
            # [性能优化] 使用 executemany 进行批量事务写入，比逐条 insert 快得多
            conn.executemany(sql, [r.to_dict() for r in records])
            conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Cache write error: {e}")

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

# ======================== API 客户端 ========================

class GeminiClient:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.url = f"{API_BASE_URL}/{config.model_name}:generateContent?key={config.api_key}"

    def _clean_json_string(self, text: str) -> str:
        """从模型输出中提取并清理 JSON 字符串"""
        # [容错逻辑] LLM 经常会在 JSON 外包裹 ```json ... ``` 标记
        # 这里使用正则强制移除 markdown 标记，确保 json.loads 能成功
        text = re.sub(r'^```json\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'^```\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'```\s*$', '', text, flags=re.MULTILINE)
        return text.strip()

    async def standardize_batch(self, session: aiohttp.ClientSession, batch: List[str]) -> List[GeoRecord]:
        payload = {
            "contents": [{"parts": [{"text": f"Input List: {json.dumps(batch, ensure_ascii=False)} "}]}],
            "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": LOCATION_RESPONSE_SCHEMA
            }
        }

        # [健壮性逻辑] 指数退避重试机制
        for attempt in range(self.config.max_retries):
            try:
                async with session.post(self.url, json=payload, timeout=60) as resp:
                    # 处理速率限制 (Rate Limit)
                    if resp.status == 429:
                        delay = (2 ** attempt) + 1 # 1s, 3s, 5s...
                        await asyncio.sleep(delay)
                        continue
                    
                    if resp.status != 200:
                        err_text = await resp.text()
                        # 处理服务端临时错误 (5xx)，可以重试
                        if 500 <= resp.status < 600:
                            self.logger.warning(f"Server Error {resp.status}, retrying...")
                            await asyncio.sleep(2 ** attempt)
                            continue
                        # 客户端错误 (400, 401, 403) 不应重试
                        self.logger.error(f"API Error {resp.status}: {err_text[:200]}")
                        break 

                    data = await resp.json()
                    return self._parse_response(data, batch)

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.logger.warning(f"Network error (Try {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)

        # 最终失败时的兜底返回，防止整个程序崩溃
        return [GeoRecord(loc, reasoning="API Failed") for loc in batch]

    def _parse_response(self, data: Dict, original_batch: List[str]) -> List[GeoRecord]:
        try:
            content = data['candidates'][0]['content']['parts'][0]['text']
            clean_content = self._clean_json_string(content)
            items = json.loads(clean_content)
            
            # [映射逻辑] 将返回结果映射回原始输入
            # 因为 API 可能会漏掉某些条目或打乱顺序，这里构建字典进行 O(1) 查找
            result_map = {item.get('input_location'): item for item in items}
            records = []
            
            for loc in original_batch:
                if item := result_map.get(loc):
                    # 动态过滤掉 Schema 中多余的字段（如果模型幻觉返回了额外字段）
                    valid_keys = GeoRecord.__dataclass_fields__.keys()
                    filtered_data = {k: v for k, v in item.items() if k in valid_keys}
                    filtered_data['input_location'] = loc 
                    records.append(GeoRecord(**filtered_data))
                else:
                    # 记录模型漏处理的数据
                    records.append(GeoRecord(loc, reasoning="Model skipped item"))
            return records

        except (KeyError, json.JSONDecodeError, IndexError) as e:
            self.logger.error(f"Parsing failed: {e}")
            return [GeoRecord(loc, reasoning="Parse Error") for loc in original_batch]


# ======================== 核心控制器 ========================

class IOHandler:
    # (省略文件读写逻辑，主要是根据扩展名判断 CSV/JSON/TXT)
    @staticmethod
    def read_input(config: AppConfig) -> List[str]:
        if config.is_demo:
            return ["New York", "London", "Shanghai", "Tokyo", "Berlin"] * 6
        
        path = Path(config.input_path)
        if not path.exists(): raise FileNotFoundError(f"{path} not found")

        try:
            if path.suffix.lower() == '.csv':
                with open(path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames: return []
                    col = config.target_column if config.target_column in reader.fieldnames else reader.fieldnames[0]
                    return [row[col].strip() for row in reader if row.get(col)]
            
            elif path.suffix.lower() == '.json':
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return [str(x) for x in (data if isinstance(data, list) else data.values())]
            
            else:
                with open(path, 'r', encoding='utf-8') as f:
                    return [line.strip() for line in f if line.strip()]
        except Exception as e:
            raise ValueError(f"Error reading file: {e}")

    @staticmethod
    def save_output(results: List[GeoRecord], path_str: str):
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

class BatchProcessor:
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = setup_logger(config.verbose)
        self.stats = Statistics()
        self.storage = StorageEngine(config.cache_db_path, self.logger)
        self.client = GeminiClient(config, self.logger)
        self.running = True
        
        # [控制逻辑] 优雅退出 (Graceful Shutdown)
        # 捕捉 Ctrl+C 信号，设置标志位，让异步任务完成后再退出，防止数据损坏
        if platform.system() != 'Windows':
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, self._signal_handler)

    def _signal_handler(self, sig, frame):
        self.logger.warning("\nStopping...")
        self.running = False

    async def _worker(self, session: aiohttp.ClientSession, batch: List[str], sem: asyncio.Semaphore, pbar: tqdm):
        """单个批次处理逻辑"""
        # [并发控制] 使用信号量 (Semaphore) 限制同时运行的协程数量
        # 避免瞬间发出数千个请求导致系统资源耗尽或触发硬性限流
        async with sem:
            if not self.running: return
            records = await self.client.standardize_batch(session, batch)
            
            self.stats.api_processed += len(records)
            self.storage.save_batch(records) # 实时保存，防止程序中途崩溃丢失进度
            pbar.update(len(batch))

    async def run(self):
        try:
            # 1. 准备数据
            raw_inputs = IOHandler.read_input(self.config)
            self.stats.total_inputs = len(raw_inputs)
            # [优化逻辑] 内存去重：相同的地址只请求一次 API，节省 Token 费用
            unique_inputs = list(dict.fromkeys(raw_inputs))
            self.stats.unique_inputs = len(unique_inputs)

            if not raw_inputs:
                self.logger.warning("No input data found.")
                return

            # 2. 缓存过滤
            # 查询 DB，分离出 "已缓存" 和 "待处理" 的数据
            cached_map = self.storage.get_cached_records(unique_inputs)
            self.stats.cached_hits = len(cached_map)
            to_process = [x for x in unique_inputs if x not in cached_map]

            self.logger.info(f"Total: {self.stats.total_inputs} | Unique: {self.stats.unique_inputs} | "
                             f"Cached: {self.stats.cached_hits} | API Todo: {len(to_process)}")

            # 3. 并发执行
            if to_process:
                sem = asyncio.Semaphore(self.config.concurrency)
                async with aiohttp.ClientSession() as session:
                    tasks = []
                    # 将待处理数据切分为小批次 (Batching)
                    batches = [to_process[i:i + self.config.batch_size] 
                               for i in range(0, len(to_process), self.config.batch_size)]
                    
                    with tqdm(total=len(to_process), desc="Processing", unit="loc") as pbar:
                        for batch in batches:
                            if not self.running: break
                            # 创建 Task 但不 await，实现并发调度
                            tasks.append(asyncio.create_task(self._worker(session, batch, sem, pbar)))
                        
                        # [同步点] 等待所有并发任务完成
                        if tasks:
                            await asyncio.gather(*tasks)

            # 4. 结果合并与导出
            self.logger.info("Exporting results...")
            # 重新获取完整缓存（包括刚刚 API 处理完写入 DB 的数据）
            final_cache = self.storage.get_cached_records(unique_inputs)
            # [数据还原] 将去重后的结果映射回原始输入的顺序和数量
            final_results = [final_cache.get(loc, GeoRecord(loc, reasoning="Missing")) for loc in raw_inputs]
            
            IOHandler.save_output(final_results, self.config.output_path)
            self._print_stats()

        except Exception as e:
            self.logger.error(f"Fatal Error: {e}", exc_info=True)
        finally:
            self.storage.close()

    def _print_stats(self):
        self.logger.info("-" * 40)
        self.logger.info(f"Done in {self.stats.elapsed:.2f}s | Speed: {self.stats.speed:.1f}/s")
        self.logger.info(f"Saved to: {self.config.output_path}")

# ======================== 入口 ========================

def main():
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = ArgumentParser(description="Gemini Geo Standardizer", formatter_class=RawTextHelpFormatter)
    parser.add_argument("--input", "-i", help="Input file path")
    parser.add_argument("--output", "-o", default="geo_output.csv", help="Output path")
    parser.add_argument("--key", "-k", default=os.environ.get(ENV_API_KEY_NAME), help="API Key")
    parser.add_argument("--model", default="gemini-2.5-flash-preview-09-2025")
    parser.add_argument("--batch", "-b", type=int, default=30)
    parser.add_argument("--concurrency", "-c", type=int, default=10)
    parser.add_argument("--cache", default="geo_cache.db")
    parser.add_argument("--column", default="location")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    if not args.key:
        print(f"Error: API Key needed via --key or env {ENV_API_KEY_NAME}")
        sys.exit(1)

    config = AppConfig(
        input_path=args.input, output_path=args.output, api_key=args.key,
        model_name=args.model, batch_size=args.batch, concurrency=args.concurrency,
        max_retries=3, cache_db_path=args.cache, target_column=args.column,
        is_demo=args.demo, verbose=args.verbose
    )

    try:
        asyncio.run(BatchProcessor(config).run())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
