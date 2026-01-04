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
from typing import List, Dict, Any, Optional, Set, Tuple
from argparse import ArgumentParser, RawTextHelpFormatter
from dataclasses import dataclass, field
from datetime import datetime

# 依赖检查
try:
    import aiohttp
    from tqdm.asyncio import tqdm
except ImportError:
    print("错误: 缺少必要依赖库。请运行: pip install aiohttp tqdm")
    sys.exit(1)

# ======================== 配置常量 ========================
API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"
DEFAULT_MODEL = "gemini-2.5-flash-preview-09-2025"
ENV_API_KEY_NAME = "GEMINI_API_KEY"

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

SYSTEM_INSTRUCTION = (
    "你是一个高精度的地理位置标准化引擎。你的任务是将各种语言描述的位置转换为 ISO 3166-1 Alpha-3 国家代码。\n"
    "规则：\n"
    "1. 严格遵守 JSON Schema 输出格式。\n"
    "2. 仅输出 ISO 3166-1 Alpha-3 代码（例如：CHN, USA, GBR, FRA）。\n"
    "3. 如果位置模糊但能确定国家（如 '加州'），输出该国代码（'USA'）。\n"
    "4. 无法识别或完全无意义的输入，请使用 'UNK'。\n"
    "5. 不要输出任何解释文字，只输出合法的 JSON。"
)

# ======================== 数据模型 ========================
@dataclass
class ProcessingStats:
    total: int = 0
    cached: int = 0
    processed: int = 0
    failed: int = 0
    start_time: float = field(default_factory=time.time)

# ======================== 存储引擎 (SQLite) ========================
class StorageEngine:
    """使用 SQLite 替代 JSON 缓存，提供更高的可靠性和查询效率"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS location_cache (
                    input_text TEXT PRIMARY KEY,
                    country_code TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def get_batch(self, texts: List[str]) -> Dict[str, str]:
        if not texts: return {}
        placeholders = ','.join(['?'] * len(texts))
        query = f"SELECT input_text, country_code FROM location_cache WHERE input_text IN ({placeholders})"
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, texts)
            return {row[0]: row[1] for row in cursor.fetchall()}

    def save_batch(self, results: List[Dict[str, str]]):
        data = [(item['input_location'], item['standardized_country_code']) for item in results]
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO location_cache (input_text, country_code) VALUES (?, ?)", 
                data
            )
            conn.commit()

# ======================== 核心处理器 ========================
class LocationStandardizerPro:
    def __init__(self, args):
        self.args = args
        self.api_url = f"{API_BASE_URL}/{args.model}:generateContent?key={args.key}"
        self.storage = StorageEngine(args.cache)
        self.stats = ProcessingStats()
        self.is_running = True
        
        # 日志配置
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger("Standardizer")
        
        # 信号处理
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_exit)

    def _handle_exit(self, signum, frame):
        self.logger.warning("\n正在执行安全退出，请稍候...")
        self.is_running = False

    def _build_payload(self, batch: List[str]) -> Dict:
        return {
            "contents": [{"parts": [{"text": f"Standardize these locations:\n{json.dumps(batch, ensure_ascii=False)}"}]}],
            "systemInstruction": {"parts": [{"text": SYSTEM_INSTRUCTION}]},
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": LOCATION_SCHEMA
            }
        }

    async def _call_api(self, session: aiohttp.ClientSession, batch: List[str]) -> List[Dict]:
        """执行 API 请求，带指数退避重试"""
        payload = self._build_payload(batch)
        max_retries = 5
        
        for attempt in range(max_retries):
            if not self.is_running: break
            try:
                async with session.post(self.api_url, json=payload, timeout=45) as resp:
                    if resp.status == 429:  # Rate limit
                        wait = (2 ** attempt) + 2
                        await asyncio.sleep(wait)
                        continue
                    
                    if resp.status != 200:
                        err_msg = await resp.text()
                        raise Exception(f"API Error {resp.status}: {err_msg}")

                    data = await resp.json()
                    text = data['candidates'][0]['content']['parts'][0]['text']
                    return json.loads(text)

            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Batch failed after {max_retries} retries: {e}")
                    return [{"input_location": loc, "standardized_country_code": "ERROR"} for loc in batch]
                await asyncio.sleep(2 ** attempt)
        
        return []

    async def run(self):
        # 1. 数据加载与预处理
        raw_inputs = self._load_data()
        unique_inputs = list(dict.fromkeys(raw_inputs)) # 保序去重
        self.stats.total = len(unique_inputs)
        
        # 2. 检查缓存
        cached_map = self.storage.get_batch(unique_inputs)
        self.stats.cached = len(cached_map)
        to_process = [loc for loc in unique_inputs if loc not in cached_map]
        
        self.logger.info(f"任务启动 | 总计: {len(raw_inputs)} | 唯一值: {self.stats.total} | 命中缓存: {self.stats.cached} | 待处理: {len(to_process)}")

        if not to_process:
            self._finalize(raw_inputs)
            return

        # 3. 分批异步处理
        batches = [to_process[i:i + self.args.batch] for i in range(0, len(to_process), self.args.batch)]
        semaphore = asyncio.Semaphore(self.args.concurrency)
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for b in batches:
                tasks.append(self._worker(session, b, semaphore))
            
            # 使用 tqdm 实时追踪
            for _ in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="标准化进度"):
                await _

        # 4. 导出结果
        if self.is_running:
            self._finalize(raw_inputs)

    async def _worker(self, session, batch, semaphore):
        async with semaphore:
            if not self.is_running: return
            results = await self._call_api(session, batch)
            if results:
                self.storage.save_batch(results)
                self.stats.processed += len(results)

    def _load_data(self) -> List[str]:
        """灵活的数据读取：支持 CSV 指定列或 JSON 列表"""
        path = self.args.input
        if self.args.demo: return ["New York", "Shanghai", "Berlin", "Paris", "Unknown"] * 10
        
        ext = os.path.splitext(path)[1].lower()
        data = []
        try:
            if ext == '.csv':
                with open(path, 'r', encoding='utf-8') as f:
                    # 自动检测表头
                    sample = f.read(2048)
                    f.seek(0)
                    has_header = csv.Sniffer().has_header(sample)
                    reader = csv.DictReader(f) if has_header else csv.reader(f)
                    
                    for row in reader:
                        if isinstance(row, dict):
                            val = row.get(self.args.column) or list(row.values())[0]
                        else:
                            val = row[0]
                        if val: data.append(str(val).strip())
            else:
                with open(path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                    data = content if isinstance(content, list) else [str(x) for x in content.values()]
            return data
        except Exception as e:
            self.logger.error(f"加载数据失败: {e}")
            sys.exit(1)

    def _finalize(self, original_order: List[str]):
        """根据原始顺序导出最终结果"""
        final_cache = self.storage.get_batch(list(set(original_order)))
        output_data = [
            {"input": loc, "country": final_cache.get(loc, "UNK")} 
            for loc in original_order
        ]
        
        out_path = self.args.output
        try:
            if out_path.endswith('.csv'):
                with open(out_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["input", "country"])
                    writer.writeheader()
                    writer.writerows(output_data)
            else:
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            elapsed = time.time() - self.stats.start_time
            self.logger.info(f"--- 处理报告 ---")
            self.logger.info(f"总耗时: {elapsed:.2f}s | 平均速度: {len(original_order)/max(elapsed,1):.2f}条/秒")
            self.logger.info(f"结果已保存至: {out_path}")
        except Exception as e:
            self.logger.error(f"保存失败: {e}")

# ======================== 入口 ========================
def main():
    parser = ArgumentParser(description="Gemini 地理标准化 Pro", formatter_class=RawTextHelpFormatter)
    group = parser.add_argument_group("基础配置")
    group.add_argument("--input", "-i", help="输入文件 (CSV/JSON)")
    group.add_argument("--output", "-o", default="standardized_results.csv", help="输出路径")
    group.add_argument("--key", "-k", default=os.environ.get(ENV_API_KEY_NAME), help="API Key")
    
    group = parser.add_argument_group("高级参数")
    group.add_argument("--model", default=DEFAULT_MODEL, help="使用的模型版本")
    group.add_argument("--batch", "-b", type=int, default=50, help="每批次处理条数 (建议 30-100)")
    group.add_argument("--concurrency", "-c", type=int, default=10, help="并行工作协程数")
    group.add_argument("--cache", default="geo_cache.db", help="SQLite 缓存数据库路径")
    group.add_argument("--column", default="location", help="CSV 输入中的目标列名")
    group.add_argument("--demo", action="store_true", help="运行演示模式")

    args = parser.parse_args()

    if not args.key:
        print("错误: 缺少 API Key。请设置环境变量 GEMINI_API_KEY 或使用 --key。")
        return

    if not args.input and not args.demo:
        parser.print_help()
        return

    app = LocationStandardizerPro(args)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
