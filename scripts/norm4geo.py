# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import os
import sys
import time
import csv
import sqlite3
import signal
from typing import List, Dict, Any, Optional, Iterable
from argparse import ArgumentParser, RawTextHelpFormatter
from dataclasses import dataclass, field
from pathlib import Path

# 依赖检查：确保安装了异步 HTTP 请求库 aiohttp 和进度条库 tqdm
try:
    import aiohttp
    from tqdm.asyncio import tqdm
except ImportError:
    print("Error: Missing dependencies. Run: pip install aiohttp tqdm")
    sys.exit(1)

# ======================== Constants (常量配置) ========================
API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"
DEFAULT_MODEL = "gemini-2.5-flash-preview-09-2025"
ENV_API_KEY_NAME = "GEMINI_API_KEY"

# 定义 JSON Schema：强制要求 Gemini 模型返回特定格式的数组对象，便于代码直接解析
LOCATION_SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "input_location": {"type": "STRING"},
            "standardized_country_code": {"type": "STRING"}
        },
        "required": ["input_location", "standardized_country_code"]
    }
}

# 系统提示词：设定 AI 的角色、逻辑规则和输出约束（仅输出 ISO 3166-1 Alpha-3 代码）
SYSTEM_INSTRUCTION = (
    "You are a high-precision geographic standardization engine. "
    "Convert various location descriptions to ISO 3166-1 Alpha-3 country codes.\n"
    "Rules:\n"
    "1. Strict adherence to JSON Schema.\n"
    "2. Output ONLY Alpha-3 codes (e.g., CHN, USA).\n"
    "3. For specific regions (e.g., 'California'), output the parent country code ('USA').\n"
    "4. Use 'UNK' for unknown or nonsensical inputs.\n"
    "5. NO explanatory text, only valid JSON."
)

# ======================== Models (数据模型) ========================
@dataclass
class ProcessingStats:
    """用于追踪任务处理进度的状态类"""
    total_unique: int = 0
    cached: int = 0
    processed: int = 0
    failed: int = 0
    start_time: float = field(default_factory=time.time)

# ======================== Storage Engine (存储引擎) ========================
class StorageEngine:
    """基于 SQLite 的缓存引擎，用于存储已处理的地名，避免重复调用 API 消耗额度"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None
        self._init_db()

    def _init_db(self):
        """初始化数据库表结构：input_text 为主键，确保地名唯一"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS location_cache (
                    input_text TEXT PRIMARY KEY,
                    country_code TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
        # 保持一个长连接以提升写入性能
        self._conn = sqlite3.connect(self.db_path)

    def get_batch(self, texts: List[str]) -> Dict[str, str]:
        """批量查询缓存，返回已存在的 {原始地名: 国家代码} 映射"""
        if not texts: return {}
        placeholders = ','.join(['?'] * len(texts))
        cursor = self._conn.execute(
            f"SELECT input_text, country_code FROM location_cache WHERE input_text IN ({placeholders})", 
            texts
        )
        return {row[0]: row[1] for row in cursor.fetchall()}

    def save_batch(self, results: List[Dict[str, str]]):
        """批量写入或更新缓存结果"""
        data = [(item['input_location'], item['standardized_country_code']) for item in results]
        try:
            self._conn.executemany(
                "INSERT OR REPLACE INTO location_cache (input_text, country_code) VALUES (?, ?)", 
                data
            )
            self._conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Database write error: {e}")

    def close(self):
        if self._conn:
            self._conn.close()

# ======================== Core Processor (核心处理器) ========================
class LocationStandardizer:
    def __init__(self, args):
        self.args = args
        self.api_url = f"{API_BASE_URL}/{args.model}:generateContent?key={args.key}"
        self.storage = StorageEngine(args.cache)
        self.stats = ProcessingStats()
        self.is_running = True # 用于控制优雅退出
        self._setup_logging()
        self._setup_signals()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger("GeoStandardizer")

    def _setup_signals(self):
        """注册系统信号拦截，如按下 Ctrl+C 时能安全保存已处理的数据"""
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_exit)

    def _handle_exit(self, signum, frame):
        if self.is_running:
            self.logger.warning("\nGraceful exit triggered. Cleaning up...")
            self.is_running = False

    def _build_payload(self, batch: List[str]) -> Dict:
        """构建发送给 Gemini 的 JSON Payload，包含系统指令和结构化输出配置"""
        return {
            "contents": [{"parts": [{"text": f"Standardize these locations:\n{json.dumps(batch, ensure_ascii=False)}"}]}],
            "systemInstruction": {"parts": [{"text": SYSTEM_INSTRUCTION}]},
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": LOCATION_SCHEMA # 强制返回 Schema 格式
            }
        }

    async def _call_api_with_retry(self, session: aiohttp.ClientSession, batch: List[str]) -> List[Dict]:
        """核心 API 调用逻辑，内置指数退避重试机制处理 429 (频率限制) 错误"""
        payload = self._build_payload(batch)
        for attempt in range(5): # 最多重试 5 次
            if not self.is_running: break
            try:
                async with session.post(self.api_url, json=payload, timeout=45) as resp:
                    if resp.status == 429: # Rate limit hit
                        await asyncio.sleep(2 ** attempt + 1) # 指数等待
                        continue
                    
                    if resp.status != 200:
                        text = await resp.text()
                        self.logger.debug(f"API Error {resp.status}: {text}")
                        await asyncio.sleep(1)
                        continue

                    data = await resp.json()
                    # 提取 LLM 返回的 JSON 文本并转为 Python 对象
                    raw_text = data['candidates'][0]['content']['parts'][0]['text']
                    results = json.loads(raw_text)
                    
                    # 校验与补全：如果模型遗漏了某些条目，用 UNK 填充，确保输入输出数量对等
                    result_map = {item['input_location']: item['standardized_country_code'] for item in results}
                    return [{"input_location": loc, "standardized_country_code": result_map.get(loc, "UNK")} for loc in batch]

            except Exception as e:
                self.logger.debug(f"Attempt {attempt} failed: {e}")
                await asyncio.sleep(2 ** attempt)
        
        # 最终失败则标记为 ERROR
        return [{"input_location": loc, "standardized_country_code": "ERROR"} for loc in batch]

    async def _worker(self, session: aiohttp.ClientSession, batch: List[str], semaphore: asyncio.Semaphore):
        """工作协程，通过信号量控制并发数"""
        async with semaphore:
            if not self.is_running: return
            results = await self._call_api_with_retry(session, batch)
            if results:
                self.storage.save_batch(results) # 实时存入数据库
                self.stats.processed += len(results)

    def _load_input_data(self) -> List[str]:
        """加载输入数据：支持智能检测 CSV 分隔符、表头，同时也支持 JSON 列表"""
        if self.args.demo:
            return ["New York", "London", "Tokyo", "Beijing", "Paris"] * 20
        
        path = Path(self.args.input)
        if not path.exists():
            self.logger.error(f"Input file not found: {path}")
            sys.exit(1)

        data = []
        try:
            if path.suffix.lower() == '.csv':
                with open(path, 'r', encoding='utf-8-sig') as f:
                    # 使用 Sniffer 自动探测 CSV 格式（逗号还是分号）
                    sample = f.read(4096)
                    f.seek(0)
                    dialect = csv.Sniffer().sniff(sample) if sample else None
                    has_header = csv.Sniffer().has_header(sample) if sample else False
                    
                    if has_header:
                        reader = csv.DictReader(f, dialect=dialect)
                        col = self.args.column
                        for row in reader:
                            # 优先取指定列，否则取第一列
                            val = row.get(col) or (list(row.values())[0] if row.values() else None)
                            if val: data.append(str(val).strip())
                    else:
                        reader = csv.reader(f, dialect=dialect)
                        for row in reader:
                            if row: data.append(str(row[0]).strip())
            else:
                with open(path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                    data = content if isinstance(content, list) else [str(v) for v in content.values()]
            return data
        except Exception as e:
            self.logger.error(f"Error reading input: {e}")
            sys.exit(1)

    async def run(self):
        """主运行逻辑：去重 -> 查缓存 -> 分批异步请求 -> 导出"""
        raw_inputs = self._load_input_data()
        if not raw_inputs:
            self.logger.warning("No data found to process.")
            return

        # 1. 地名去重：只处理唯一的地点，节省 API 额度
        unique_inputs = list(dict.fromkeys(raw_inputs))
        self.stats.total_unique = len(unique_inputs)

        # 2. 从缓存读取：获取已处理过的结果
        cached_map = self.storage.get_batch(unique_inputs)
        self.stats.cached = len(cached_map)
        to_process = [loc for loc in unique_inputs if loc not in cached_map]

        self.logger.info(
            f"Status | Total Items: {len(raw_inputs)} | Unique: {self.stats.total_unique} | "
            f"Cached: {self.stats.cached} | To Process: {len(to_process)}"
        )

        # 3. 分批异步处理剩余数据
        if to_process and self.is_running:
            # 切分批次 (Batch Size)
            batches = [to_process[i:i + self.args.batch] for i in range(0, len(to_process), self.args.batch)]
            # 限制并发连接数，防止被服务器封禁
            semaphore = asyncio.Semaphore(self.args.concurrency)
            
            async with aiohttp.ClientSession() as session:
                tasks = [self._worker(session, b, semaphore) for b in batches]
                # 使用 tqdm 显示进度条
                for _ in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Standardizing"):
                    await _

        # 4. 汇总最终结果并写入文件
        self._finalize(raw_inputs)
        self.storage.close()

    def _finalize(self, original_order: List[str]):
        """将缓存中的结果映射回原始数据的顺序并导出"""
        all_results = self.storage.get_batch(list(set(original_order)))
        
        output_data = [
            {"input_location": loc, "country_code": all_results.get(loc, "UNK")}
            for loc in original_order
        ]

        try:
            out_path = Path(self.args.output)
            if out_path.suffix.lower() == '.csv':
                with open(out_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["input_location", "country_code"])
                    writer.writeheader()
                    writer.writerows(output_data)
            else:
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, ensure_ascii=False, indent=2)

            elapsed = time.time() - self.stats.start_time
            self.logger.info("--- Execution Summary ---")
            self.logger.info(f"Time: {elapsed:.2f}s | Speed: {len(original_order)/max(elapsed, 0.1):.1f} items/s")
            self.logger.info(f"Saved to: {out_path}")
        except Exception as e:
            self.logger.error(f"Failed to save output: {e}")

# ======================== Main Entry ========================
def main():
    """命令行接口配置"""
    parser = ArgumentParser(description="Gemini Geo-Standardization Pro (Optimized)", formatter_class=RawTextHelpFormatter)
    
    cfg = parser.add_argument_group("Config")
    cfg.add_argument("--input", "-i", help="Input file (CSV/JSON)")
    cfg.add_argument("--output", "-o", default="results.csv", help="Output path")
    cfg.add_argument("--key", "-k", default=os.environ.get(ENV_API_KEY_NAME), help="Gemini API Key")
    
    adv = parser.add_argument_group("Advanced")
    adv.add_argument("--model", default=DEFAULT_MODEL, help="Gemini model version")
    adv.add_argument("--batch", "-b", type=int, default=50, help="Items per API request") # 单个请求处理多少地名
    adv.add_argument("--concurrency", "-c", type=int, default=5, help="Concurrent workers") # 同时开启多少个连接
    adv.add_argument("--cache", default="geo_cache.db", help="Cache database path")
    adv.add_argument("--column", default="location", help="Target column name in CSV")
    adv.add_argument("--demo", action="store_true", help="Run with demo data")

    args = parser.parse_args()

    if not args.key:
        print(f"Error: API Key missing. Set {ENV_API_KEY_NAME} env var or use --key.")
        return

    if not args.input and not args.demo:
        parser.print_help()
        return

    app = LocationStandardizer(args)
    try:
        # 启动 asyncio 事件循环
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f"Application crashed: {e}")

if __name__ == "__main__":
    main()
