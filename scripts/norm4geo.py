# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import math
import os
import sys
import time
import csv
from typing import List, Dict, Any, Optional, Set
from argparse import ArgumentParser, RawTextHelpFormatter
from dataclasses import dataclass, field

# 需要安装第三方库: pip install aiohttp tqdm
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

# JSON Schema (保持原样，用于强制结构化输出)
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
    "你是专业的地理位置数据标准化引擎。规则："
    "1. 输入为原始位置列表，输出必须符合JSON Schema；"
    "2. 输出 ISO 3166-1 Alpha-3 代码（如 CHN, USA）；"
    "3. 无法识别用 'UNK'；"
    "4. 仅输出JSON数据。"
)

# ======================== 数据结构与配置 ========================
@dataclass
class Config:
    api_key: str
    input_file: str
    output_file: str
    cache_file: str = "location_cache.json"
    batch_size: int = 50
    concurrency: int = 5  #并发请求数限制
    model_name: str = DEFAULT_MODEL
    demo_mode: bool = False

# ======================== 缓存管理器 ========================
class CacheManager:
    """简单的基于JSON文件的本地缓存，用于断点续传和节省API调用"""
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.cache: Dict[str, str] = {}
        self.loaded = False

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    self.cache = json.load(f)
                logging.info(f"已加载本地缓存: {len(self.cache)} 条记录")
            except Exception as e:
                logging.warning(f"加载缓存失败，将创建新缓存: {e}")
        self.loaded = True

    def save(self):
        try:
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"保存缓存失败: {e}")

    def get(self, key: str) -> Optional[str]:
        return self.cache.get(key)

    def set(self, key: str, value: str):
        self.cache[key] = value

    def update_batch(self, items: List[Dict[str, str]]):
        """批量更新缓存"""
        for item in items:
            self.cache[item['input_location']] = item['standardized_country_code']
        # 每次批次处理完都保存一次，防止程序崩溃丢失进度
        self.save()

# ======================== 核心处理器类 ========================
class LocationStandardizer:
    def __init__(self, config: Config):
        self.cfg = config
        self.api_url = f"{API_BASE_URL}/{self.cfg.model_name}:generateContent?key={self.cfg.api_key}"
        self.cache_manager = CacheManager(self.cfg.cache_file)
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("processor.log", encoding="utf-8"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger("Standardizer")

    def _build_payload(self, batch_locations: List[str]) -> Dict:
        user_query = f"Standardize these locations to ISO 3166-1 Alpha-3:\n{json.dumps(batch_locations, ensure_ascii=False)}"
        return {
            "contents": [{"parts": [{"text": user_query}]}],
            "systemInstruction": {"parts": [{"text": SYSTEM_INSTRUCTION}]},
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": LOCATION_SCHEMA
            }
        }

    async def _process_batch(self, session: aiohttp.ClientSession, batch: List[str], semaphore: asyncio.Semaphore) -> List[Dict[str, str]]:
        """处理单个批次，包含信号量控制和重试逻辑"""
        async with semaphore:  # 限制并发数
            payload = self._build_payload(batch)
            retry_delay = 1
            max_retries = 3

            for attempt in range(max_retries + 1):
                try:
                    async with session.post(self.api_url, json=payload, timeout=30) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            raise aiohttp.ClientResponseError(
                                response.request_info, response.history, 
                                status=response.status, message=error_text
                            )
                        
                        result = await response.json()
                        # 解析 Gemini 响应
                        candidates = result.get("candidates", [])
                        if not candidates:
                            raise ValueError("API返回无candidates")
                        
                        content_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text")
                        if not content_text:
                            raise ValueError("API返回内容为空")

                        parsed_data = json.loads(content_text)
                        
                        # 简单校验
                        if isinstance(parsed_data, list):
                            return parsed_data
                        else:
                            raise ValueError("API返回格式非列表")

                except Exception as e:
                    if attempt < max_retries:
                        sleep_time = retry_delay * (2 ** attempt) # 指数退避
                        # self.logger.warning(f"批次请求失败 ({e})，{sleep_time}秒后重试...")
                        await asyncio.sleep(sleep_time)
                    else:
                        self.logger.error(f"批次最终失败: {e}")
                        # 失败降级：将该批次所有数据标记为 UNK
                        return [{"input_location": loc, "standardized_country_code": "UNK_ERROR"} for loc in batch]
            return []

    async def run(self):
        """主执行流程"""
        # 1. 加载缓存
        self.cache_manager.load()

        # 2. 获取数据
        raw_data = self._load_input()
        if not raw_data:
            self.logger.error("无数据需要处理")
            return

        # 3. 过滤已缓存数据
        to_process = []
        cached_results = []
        
        # 去重并检查缓存
        seen = set()
        unique_raw = []
        for loc in raw_data:
            if loc not in seen:
                seen.add(loc)
                unique_raw.append(loc)

        for loc in unique_raw:
            cached_val = self.cache_manager.get(loc)
            if cached_val:
                cached_results.append({"input_location": loc, "standardized_country_code": cached_val})
            else:
                to_process.append(loc)

        self.logger.info(f"总数据: {len(raw_data)}, 唯一数据: {len(unique_raw)}")
        self.logger.info(f"命中缓存: {len(cached_results)}, 待处理API: {len(to_process)}")

        # 4. 批次切分
        batches = [to_process[i:i + self.cfg.batch_size] for i in range(0, len(to_process), self.cfg.batch_size)]
        new_results = []

        # 5. 异步并发处理
        if batches:
            semaphore = asyncio.Semaphore(self.cfg.concurrency)
            async with aiohttp.ClientSession() as session:
                tasks = [self._process_batch(session, batch, semaphore) for batch in batches]
                
                # 使用 tqdm 显示进度条
                for coro in tqdm.as_completed(tasks, desc="API请求进度", total=len(batches)):
                    batch_res = await coro
                    if batch_res:
                        new_results.extend(batch_res)
                        # 实时更新缓存，避免程序中途崩溃数据丢失
                        self.cache_manager.update_batch(batch_res)
        
        # 6. 合并结果并映射回原始顺序
        final_lookup = {item["input_location"]: item["standardized_country_code"] for item in cached_results + new_results}
        
        final_output = []
        for loc in raw_data:
            final_output.append({
                "input_location": loc,
                "standardized_country_code": final_lookup.get(loc, "UNK")
            })

        # 7. 保存
        self._save_output(final_output)

    def _load_input(self) -> List[str]:
        """支持 CSV 和 JSON"""
        if self.cfg.demo_mode:
            return ["BeiJing", "NYC", "London", "Tokyo", "Unknown Place"] * 5
        
        path = self.cfg.input_file
        if not os.path.exists(path):
            raise FileNotFoundError(f"找不到文件: {path}")

        ext = os.path.splitext(path)[1].lower()
        data = []
        
        try:
            if ext == '.json':
                with open(path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                    if isinstance(content, list):
                        # 兼容 ["Loc"] 和 [{"location": "Loc"}] 两种格式
                        data = [x if isinstance(x, str) else list(x.values())[0] for x in content]
            elif ext == '.csv':
                with open(path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if row: data.append(row[0]) # 假设第一列是地址
            
            return [str(d).strip() for d in data if d]
        except Exception as e:
            self.logger.error(f"读取文件失败: {e}")
            sys.exit(1)

    def _save_output(self, data: List[Dict]):
        path = self.cfg.output_file
        ext = os.path.splitext(path)[1].lower()
        
        try:
            if ext == '.csv':
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(["input_location", "standardized_country_code"])
                    for item in data:
                        writer.writerow([item["input_location"], item["standardized_country_code"]])
            else: # 默认为 JSON
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"处理完成，结果已保存至: {path}")
        except Exception as e:
            self.logger.error(f"保存文件失败: {e}")

# ======================== 入口函数 ========================
def main():
    parser = ArgumentParser(description="Gemini 地理位置标准化工具 (Async Pro版)")
    parser.add_argument("--input", help="输入文件路径 (支持 JSON list 或 CSV 单列)")
    parser.add_argument("--output", default="output.json", help="输出文件路径 (支持 .json 或 .csv)")
    parser.add_argument("--cache", default="location_cache.json", help="本地缓存文件路径")
    parser.add_argument("--key", default=os.environ.get(ENV_API_KEY_NAME), help="Gemini API Key")
    parser.add_argument("--batch", type=int, default=50, help="每批次处理数量")
    parser.add_argument("--concurrency", type=int, default=5, help="并发请求数限制 (建议 3-10)")
    parser.add_argument("--demo", action="store_true", help="使用内置演示数据")

    args = parser.parse_args()

    if not args.key:
        print("错误: 未提供 API Key。请设置环境变量 GEMINI_API_KEY 或使用 --key 参数。")
        sys.exit(1)

    if not args.input and not args.demo:
        print("错误: 请提供 --input 文件或使用 --demo 模式。")
        sys.exit(1)

    config = Config(
        api_key=args.key,
        input_file=args.input,
        output_file=args.output,
        cache_file=args.cache,
        batch_size=args.batch,
        concurrency=args.concurrency,
        demo_mode=args.demo
    )

    runner = LocationStandardizer(config)
    asyncio.run(runner.run())

if __name__ == "__main__":
    main()
