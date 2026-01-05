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

# Dependency Check
try:
    import aiohttp
    from tqdm.asyncio import tqdm
except ImportError:
    print("Error: Missing dependencies. Run: pip install aiohttp tqdm")
    sys.exit(1)

# ======================== Constants ========================
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
    "You are a high-precision geographic standardization engine. "
    "Convert various location descriptions to ISO 3166-1 Alpha-3 country codes.\n"
    "Rules:\n"
    "1. Strict adherence to JSON Schema.\n"
    "2. Output ONLY Alpha-3 codes (e.g., CHN, USA).\n"
    "3. For specific regions (e.g., 'California'), output the parent country code ('USA').\n"
    "4. Use 'UNK' for unknown or nonsensical inputs.\n"
    "5. NO explanatory text, only valid JSON."
)

# ======================== Models ========================
@dataclass
class ProcessingStats:
    total_unique: int = 0
    cached: int = 0
    processed: int = 0
    failed: int = 0
    start_time: float = field(default_factory=time.time)

# ======================== Storage Engine ========================
class StorageEngine:
    """Thread-safe-ish SQLite storage with persistent connection."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None
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
        # Keep one connection open for the duration of the app
        self._conn = sqlite3.connect(self.db_path)

    def get_batch(self, texts: List[str]) -> Dict[str, str]:
        if not texts: return {}
        placeholders = ','.join(['?'] * len(texts))
        cursor = self._conn.execute(
            f"SELECT input_text, country_code FROM location_cache WHERE input_text IN ({placeholders})", 
            texts
        )
        return {row[0]: row[1] for row in cursor.fetchall()}

    def save_batch(self, results: List[Dict[str, str]]):
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

# ======================== Core Processor ========================
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
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger("GeoStandardizer")

    def _setup_signals(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_exit)

    def _handle_exit(self, signum, frame):
        if self.is_running:
            self.logger.warning("\nGraceful exit triggered. Cleaning up...")
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

    async def _call_api_with_retry(self, session: aiohttp.ClientSession, batch: List[str]) -> List[Dict]:
        payload = self._build_payload(batch)
        for attempt in range(5):
            if not self.is_running: break
            try:
                async with session.post(self.api_url, json=payload, timeout=45) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(2 ** attempt + 1)
                        continue
                    
                    if resp.status != 200:
                        text = await resp.text()
                        self.logger.debug(f"API Error {resp.status}: {text}")
                        await asyncio.sleep(1)
                        continue

                    data = await resp.json()
                    raw_text = data['candidates'][0]['content']['parts'][0]['text']
                    results = json.loads(raw_text)
                    
                    # Validation: Ensure all inputs are accounted for to prevent mismatch
                    # If LLM skips some, we fill them with UNK
                    result_map = {item['input_location']: item['standardized_country_code'] for item in results}
                    return [{"input_location": loc, "standardized_country_code": result_map.get(loc, "UNK")} for loc in batch]

            except Exception as e:
                self.logger.debug(f"Attempt {attempt} failed: {e}")
                await asyncio.sleep(2 ** attempt)
        
        return [{"input_location": loc, "standardized_country_code": "ERROR"} for loc in batch]

    async def _worker(self, session: aiohttp.ClientSession, batch: List[str], semaphore: asyncio.Semaphore):
        async with semaphore:
            if not self.is_running: return
            results = await self._call_api_with_retry(session, batch)
            if results:
                self.storage.save_batch(results)
                self.stats.processed += len(results)

    def _load_input_data(self) -> List[str]:
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
                    # Robust header detection
                    sample = f.read(4096)
                    f.seek(0)
                    dialect = csv.Sniffer().sniff(sample) if sample else None
                    has_header = csv.Sniffer().has_header(sample) if sample else False
                    
                    if has_header:
                        reader = csv.DictReader(f, dialect=dialect)
                        col = self.args.column
                        for row in reader:
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
        raw_inputs = self._load_input_data()
        if not raw_inputs:
            self.logger.warning("No data found to process.")
            return

        unique_inputs = list(dict.fromkeys(raw_inputs))
        self.stats.total_unique = len(unique_inputs)

        # 1. Fetch from Cache
        cached_map = self.storage.get_batch(unique_inputs)
        self.stats.cached = len(cached_map)
        to_process = [loc for loc in unique_inputs if loc not in cached_map]

        self.logger.info(
            f"Status | Total Items: {len(raw_inputs)} | Unique: {self.stats.total_unique} | "
            f"Cached: {self.stats.cached} | To Process: {len(to_process)}"
        )

        # 2. Process Remaining
        if to_process and self.is_running:
            batches = [to_process[i:i + self.args.batch] for i in range(0, len(to_process), self.args.batch)]
            semaphore = asyncio.Semaphore(self.args.concurrency)
            
            async with aiohttp.ClientSession() as session:
                tasks = [self._worker(session, b, semaphore) for b in batches]
                for _ in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Standardizing"):
                    await _

        # 3. Finalize Output
        self._finalize(raw_inputs)
        self.storage.close()

    def _finalize(self, original_order: List[str]):
        # Re-fetch everything from cache to ensure we have the latest results
        # Done in one big query for efficiency
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
    parser = ArgumentParser(description="Gemini Geo-Standardization Pro (Optimized)", formatter_class=RawTextHelpFormatter)
    
    cfg = parser.add_argument_group("Config")
    cfg.add_argument("--input", "-i", help="Input file (CSV/JSON)")
    cfg.add_argument("--output", "-o", default="results.csv", help="Output path")
    cfg.add_argument("--key", "-k", default=os.environ.get(ENV_API_KEY_NAME), help="Gemini API Key")
    
    adv = parser.add_argument_group("Advanced")
    adv.add_argument("--model", default=DEFAULT_MODEL, help="Gemini model version")
    adv.add_argument("--batch", "-b", type=int, default=50, help="Items per API request")
    adv.add_argument("--concurrency", "-c", type=int, default=5, help="Concurrent workers")
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
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f"Application crashed: {e}")

if __name__ == "__main__":
    main()
