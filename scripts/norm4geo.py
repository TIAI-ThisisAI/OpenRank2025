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
from typing import List, Dict, Any, Optional
from argparse import ArgumentParser, RawTextHelpFormatter
from dataclasses import dataclass, field
from pathlib import Path

# 依赖检查
try:
    import aiohttp
    from tqdm.asyncio import tqdm
except ImportError:
    print("错误: 缺少依赖。请运行: pip install aiohttp tqdm")
    sys.exit(1)

# ======================== 常量配置 ========================
API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"
DEFAULT_MODEL = "gemini-2.5-flash-preview-09-2025"
ENV_API_KEY_NAME = "GEMINI_API_KEY"

# 拓展后的 JSON Schema：要求更丰富的信息
LOCATION_SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "input_location": {"type": "STRING"},
            "city": {"type": "STRING", "description": "城市或地方名称"},
            "subdivision": {"type": "STRING", "description": "省、州或一级行政区"},
            "country_alpha2": {"type": "STRING", "description": "ISO 3166-1 Alpha-2 代码"},
            "country_alpha3": {"type": "STRING", "description": "ISO 3166-1 Alpha-3 代码"},
            "confidence": {"type": "NUMBER", "description": "0-1 之间的置信度评分"},
            "reasoning": {"type": "STRING", "description": "简短的推断依据"}
        },
        "required": ["input_location", "country_alpha3", "confidence"]
    }
}

SYSTEM_INSTRUCTION = (
    "您是一个高精度的地理信息标准化引擎。\n"
    "任务：将各种地理描述转换为标准的结构化数据。\n"
    "规则：\n"
    "1. 严格遵守提供的 JSON Schema。\n"
    "2. country_alpha3 必须符合 ISO 3166-1 Alpha-3 标准。\n"
    "3. 如果输入是州/省（如 'California'），请在 subdivision 填入 'California'，country 填入 'USA'。\n"
    "4. 对于无法识别的输入，country_alpha3 请使用 'UNK'，置信度设为 0。\n"
    "5. 严禁输出任何解释性文字，只返回有效的 JSON 数组。"
)

# ======================== 数据模型 ========================
@dataclass
class ProcessingStats:
    """追踪处理进度的状态类"""
    total_raw: int = 0
    total_unique: int = 0
    cached: int = 0
    processed: int = 0
    failed: int = 0
    start_time: float = field(default_factory=time.time)

# ======================== 存储引擎 ========================
class StorageEngine:
    """增强型 SQLite 缓存引擎，存储更完整的字段"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
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
        self._conn = sqlite3.connect(self.db_path)

    def get_batch(self, texts: List[str]) -> Dict[str, Dict]:
        if not texts: return {}
        # 考虑到 SQLite IN 子句限制，如果数量过大需分片，此处默认 batch_size (50) 没问题
        placeholders = ','.join(['?'] * len(texts))
        cursor = self._conn.execute(
            f"SELECT * FROM geo_cache WHERE input_text IN ({placeholders})", 
            texts
        )
        cols = [column[0] for column in cursor.description]
        results = {}
        for row in cursor.fetchall():
            row_dict = dict(zip(cols, row))
            results[row_dict['input_text']] = row_dict
        return results

    def save_batch(self, results: List[Dict[str, Any]]):
        sql = """
            INSERT OR REPLACE INTO geo_cache 
            (input_text, city, subdivision, country_alpha2, country_alpha3, confidence, reasoning) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        data = [
            (
                item['input_location'], 
                item.get('city'), 
                item.get('subdivision'), 
                item.get('country_alpha2'), 
                item['country_alpha3'], 
                item.get('confidence', 0),
                item.get('reasoning', '')
            ) for item in results
        ]
        try:
            self._conn.executemany(sql, data)
            self._conn.commit()
        except sqlite3.Error as e:
            logging.error(f"数据库写入错误: {e}")

    def close(self):
        if self._conn:
            self._conn.close()

# ======================== 核心处理器 ========================
class LocationStandardizer:
    def __init__(self, args):
        self.args = args
        self.api_url = f"{API_BASE_URL}/{args.model}:generateContent?key={args.key}"
        self.storage = StorageEngine(args.cache)
        self.stats = ProcessingStats()
        self.is_running = True
        self._setup_logging()
        self._setup_signals()

    def _setup_logging(self):
        level = logging.DEBUG if self.args.verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger("GeoStandardizer")

    def _setup_signals(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_exit)

    def _handle_exit(self, signum, frame):
        if self.is_running:
            self.logger.warning("\n检测到退出信号，正在安全关闭并保存进度...")
            self.is_running = False

    def _build_payload(self, batch: List[str]) -> Dict:
        return {
            "contents": [{"parts": [{"text": f"请标准化以下地点清单：\n{json.dumps(batch, ensure_ascii=False)}"}]}],
            "systemInstruction": {"parts": [{"text": SYSTEM_INSTRUCTION}]},
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": LOCATION_SCHEMA
            }
        }

    async def _call_api_with_retry(self, session: aiohttp.ClientSession, batch: List[str]) -> List[Dict]:
        payload = self._build_payload(batch)
        for attempt in range(args.retry):
            if not self.is_running: break
            try:
                async with session.post(self.api_url, json=payload, timeout=60) as resp:
                    if resp.status == 429:
                        wait = (2 ** attempt) + 2
                        self.logger.debug(f"触发频率限制 (429)，等待 {wait} 秒...")
                        await asyncio.sleep(wait)
                        continue
                    
                    if resp.status != 200:
                        err_text = await resp.text()
                        self.logger.error(f"API 错误 {resp.status}: {err_text[:200]}")
                        await asyncio.sleep(2)
                        continue

                    data = await resp.json()
                    if 'candidates' not in data or not data['candidates']:
                        raise ValueError("API 返回内容为空")

                    raw_text = data['candidates'][0]['content']['parts'][0]['text']
                    results = json.loads(raw_text)
                    
                    # 结果对齐校验
                    result_map = {item['input_location']: item for item in results}
                    final_results = []
                    for loc in batch:
                        if loc in result_map:
                            final_results.append(result_map[loc])
                        else:
                            # 补全模型遗漏的条目
                            final_results.append({
                                "input_location": loc,
                                "country_alpha3": "UNK",
                                "confidence": 0,
                                "reasoning": "Model skipped this item"
                            })
                    return final_results

            except Exception as e:
                self.logger.debug(f"第 {attempt+1} 次尝试失败: {str(e)}")
                await asyncio.sleep(2 ** attempt)
        
        # 最终失败处理
        return [{
            "input_location": loc, 
            "country_alpha3": "ERROR", 
            "confidence": 0, 
            "reasoning": "Failed after retries"
        } for loc in batch]

    async def _worker(self, session: aiohttp.ClientSession, batch: List[str], semaphore: asyncio.Semaphore):
        async with semaphore:
            if not self.is_running: return
            results = await self._call_api_with_retry(session, batch)
            if results:
                self.storage.save_batch(results)
                self.stats.processed += len(results)

    def _load_input_data(self) -> List[str]:
        if self.args.demo:
            return ["New York", "London", "上海", "Tokyo", "Berlin", "Paris", "California", "UnknownCity123"] * 5
        
        path = Path(self.args.input)
        if not path.exists():
            self.logger.error(f"输入文件不存在: {path}")
            sys.exit(1)

        data = []
        try:
            if path.suffix.lower() == '.csv':
                with open(path, 'r', encoding='utf-8-sig') as f:
                    sample = f.read(8192)
                    f.seek(0)
                    dialect = csv.Sniffer().sniff(sample) if sample else csv.excel
                    reader = csv.DictReader(f, dialect=dialect)
                    col = self.args.column
                    for row in reader:
                        val = row.get(col) or (list(row.values())[0] if row.values() else None)
                        if val: data.append(str(val).strip())
            else:
                with open(path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                    data = content if isinstance(content, list) else [str(v) for v in content.values()]
            return [d for d in data if d]
        except Exception as e:
            self.logger.error(f"读取输入文件失败: {e}")
            sys.exit(1)

    async def run(self):
        raw_inputs = self._load_input_data()
        if not raw_inputs:
            self.logger.warning("未找到待处理的数据。")
            return

        self.stats.total_raw = len(raw_inputs)
        unique_inputs = list(dict.fromkeys(raw_inputs))
        self.stats.total_unique = len(unique_inputs)

        # 检查缓存
        cached_map = self.storage.get_batch(unique_inputs)
        self.stats.cached = len(cached_map)
        to_process = [loc for loc in unique_inputs if loc not in cached_map]

        self.logger.info(
            f"任务就绪 | 总条数: {self.stats.total_raw} | 唯一值: {self.stats.total_unique} | "
            f"已缓存: {self.stats.cached} | 待请求: {len(to_process)}"
        )

        if to_process and self.is_running:
            batches = [to_process[i:i + self.args.batch] for i in range(0, len(to_process), self.args.batch)]
            semaphore = asyncio.Semaphore(self.args.concurrency)
            
            async with aiohttp.ClientSession() as session:
                tasks = [self._worker(session, b, semaphore) for b in batches]
                for _ in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="标准化处理中"):
                    await _

        self._finalize(raw_inputs)
        self.storage.close()

    def _finalize(self, original_order: List[str]):
        """导出结果：包含所有提取的字段"""
        all_results = self.storage.get_batch(list(set(original_order)))
        
        output_rows = []
        for loc in original_order:
            res = all_results.get(loc, {})
            output_rows.append({
                "input_location": loc,
                "city": res.get("city", ""),
                "subdivision": res.get("subdivision", ""),
                "country_alpha2": res.get("country_alpha2", ""),
                "country_alpha3": res.get("country_alpha3", "UNK"),
                "confidence": res.get("confidence", 0),
                "reasoning": res.get("reasoning", "")
            })

        try:
            out_path = Path(self.args.output)
            fieldnames = ["input_location", "city", "subdivision", "country_alpha2", "country_alpha3", "confidence", "reasoning"]
            
            if out_path.suffix.lower() == '.csv':
                with open(out_path, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(output_rows)
            else:
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(output_rows, f, ensure_ascii=False, indent=2)

            elapsed = time.time() - self.stats.start_time
            self.logger.info("="*30)
            self.logger.info("执行摘要")
            self.logger.info(f"耗时: {elapsed:.2f}秒")
            self.logger.info(f"平均速度: {len(original_order)/max(elapsed, 0.1):.1f} 条/秒")
            self.logger.info(f"输出文件: {out_path.absolute()}")
            self.logger.info("="*30)
        except Exception as e:
            self.logger.error(f"导出文件失败: {e}")

# ======================== 程序入口 ========================
def main():
    parser = ArgumentParser(description="Gemini 地理标准化专业版", formatter_class=RawTextHelpFormatter)
    
    cfg = parser.add_argument_group("基础配置")
    cfg.add_argument("--input", "-i", help="输入文件 (CSV/JSON)")
    cfg.add_argument("--output", "-o", default="geo_results.csv", help="输出文件路径")
    cfg.add_argument("--key", "-k", default=os.environ.get(ENV_API_KEY_NAME), help="Gemini API Key")
    
    adv = parser.add_argument_group("进阶选项")
    adv.add_argument("--model", default=DEFAULT_MODEL, help="Gemini 模型版本")
    adv.add_argument("--batch", "-b", type=int, default=30, help="单个请求包含的地名数量 (建议 20-50)")
    adv.add_argument("--concurrency", "-c", type=int, default=3, help="并发请求数 (免费版建议 1-2, 付费版可设为 5+)")
    adv.add_argument("--retry", type=int, default=5, help="单个批次最大重试次数")
    adv.add_argument("--cache", default="geo_cache_pro.db", help="SQLite 缓存数据库路径")
    adv.add_argument("--column", default="location", help="CSV 文件中的目标列名")
    adv.add_argument("--demo", action="store_true", help="使用演示数据运行")
    adv.add_argument("--verbose", action="store_true", help="显示详细调试日志")

    args = parser.parse_args()

    if not args.key:
        print(f"错误: 缺少 API Key。请设置环境变量 {ENV_API_KEY_NAME} 或使用 --key 参数。")
        return

    if not args.input and not args.demo:
        parser.print_help()
        return

    # 全局变量供部分逻辑引用（如 retry）
    global _global_args
    _global_args = args

    app = LocationStandardizer(args)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f"应用程序崩溃: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
