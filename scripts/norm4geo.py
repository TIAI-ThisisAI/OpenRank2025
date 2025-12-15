# -*- coding: utf-8 -*-
import json
import math
import os
import random
import sys
import time
import logging
from typing import List, Dict, Any, Optional, Tuple
from argparse import ArgumentParser, RawTextHelpFormatter

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======================== 配置常量（集中管理）========================
API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"
MODEL_NAME = "gemini-2.5-flash-preview-09-2025"
API_URL = f"{API_BASE_URL}/{MODEL_NAME}:generateContent"
BATCH_SIZE = 50
MAX_RETRIES = 5
MAX_RETRY_DELAY = 30  # 最大重试延迟（秒），避免指数退避过大
REQUEST_TIMEOUT = 30  # 请求超时时间（秒）
ENV_API_KEY_NAME = "GEMINI_API_KEY"

# JSON 响应 Schema（类型提示+校验用）
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

# 系统指令（优化表述，增强LLM输出稳定性）
SYSTEM_INSTRUCTION = (
    "你是专业的地理位置数据标准化引擎，严格遵循以下规则：\n"
    "1. 输入为原始地理位置字符串列表，输出必须符合指定的JSON Schema；\n"
    "2. 输出的国家代码必须是 ISO 3166-1 Alpha-3 格式（如 CHN、USA、DEU）；\n"
    "3. 无法识别的位置统一使用 'UNK' 作为标准化代码；\n"
    "4. 输出仅包含JSON数据，不添加任何额外解释、说明或格式标记。"
)

# ======================== 日志配置 ========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("location_cleaner.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ======================== 工具函数 ========================
def create_requests_session() -> requests.Session:
    """创建带重试机制的requests会话，复用连接提升性能"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,  # 底层连接重试次数
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def validate_api_key(api_key: str) -> bool:
    """校验API Key的基本合法性"""
    if not api_key or not isinstance(api_key, str):
        return False
    # 简单校验Gemini API Key格式（以AIzaSy开头）
    return api_key.startswith("AIzaSy")

def build_llm_payload(batch: List[str]) -> Dict[str, Any]:
    """构建LLM API请求的Payload"""
    user_query = (
        "请将以下原始地理位置字符串列表标准化为 ISO 3166-1 Alpha-3 国家代码：\n"
        f"{json.dumps(batch, ensure_ascii=False)}"
    )
    return {
        "contents": [{"parts": [{"text": user_query}]}],
        "systemInstruction": {"parts": [{"text": SYSTEM_INSTRUCTION}]},
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": LOCATION_SCHEMA
        }
    }

def validate_batch_result(batch_result: Any) -> bool:
    """校验单个批次结果是否符合Schema要求"""
    if not isinstance(batch_result, list):
        logger.error("批次结果不是列表类型")
        return False
    
    for item in batch_result:
        if not isinstance(item, dict):
            logger.error(f"批次项不是字典类型: {item}")
            return False
        required_keys = ["input_location", "standardized_country_code"]
        if not all(key in item for key in required_keys):
            logger.error(f"批次项缺少必要字段: {item}")
            return False
        if not isinstance(item["input_location"], str) or not isinstance(item["standardized_country_code"], str):
            logger.error(f"批次项字段类型错误: {item}")
            return False
    return True

def ordered_unique_list(lst: List[str]) -> List[str]:
    """保留顺序的去重（Python3.7+ dict有序）"""
    seen = {}
    result = []
    for item in lst:
        if item not in seen:
            seen[item] = True
            result.append(item)
    return result

# ======================== 核心业务函数 ========================
def call_llm_api(
    payload: Dict[str, Any],
    api_key: str,
    batch_index: int,
    max_retries: int = MAX_RETRIES
) -> Optional[List[Dict[str, str]]]:
    """
    调用LLM API并处理重试逻辑
    
    Args:
        payload: API请求体
        api_key: Gemini API密钥
        batch_index: 批次索引（用于日志）
        max_retries: 最大重试次数
    
    Returns:
        符合Schema的批次结果，失败返回None
    """
    url = f"{API_URL}?key={api_key}"
    session = create_requests_session()
    batch_num = batch_index + 1

    for retry in range(max_retries):
        try:
            response = session.post(
                url,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            result = response.json()

            # 解析响应内容
            candidate = result.get("candidates", [{}])[0]
            if not candidate or not candidate.get("content") or not candidate["content"].get("parts"):
                raise ValueError("响应中未找到有效内容")
            
            json_text = candidate["content"]["parts"][0].get("text")
            if not json_text:
                raise ValueError("响应文本为空")
            
            batch_results = json.loads(json_text)
            if validate_batch_result(batch_results):
                return batch_results
            else:
                raise ValueError("批次结果不符合Schema要求")

        except Exception as e:
            # 计算重试延迟（指数退避+随机抖动，限制最大延迟）
            delay = min(2 ** retry + random.uniform(0, 1), MAX_RETRY_DELAY)
            if retry < max_retries - 1:
                logger.warning(
                    f"批次 {batch_num} 请求失败 (重试 {retry+1}/{max_retries}): {str(e)} "
                    f"- 等待 {delay:.2f} 秒后重试"
                )
                time.sleep(delay)
            else:
                logger.error(f"批次 {batch_num} 达到最大重试次数: {str(e)}")
                return None
    return None

def clean_locations_batched(
    messy_locations: List[str],
    api_key: str,
    max_retries: int = MAX_RETRIES
) -> List[Dict[str, str]]:
    """
    批量清洗地理位置数据
    
    Args:
        messy_locations: 原始位置字符串列表
        api_key: Gemini API密钥
        max_retries: 最大重试次数
    
    Returns:
        标准化后的完整结果列表
    """
    if not messy_locations:
        logger.warning("输入位置列表为空")
        return []

    # 去重（保留顺序）并计算批次
    unique_locs = ordered_unique_list(messy_locations)
    unique_count = len(unique_locs)
    total_batches = math.ceil(unique_count / BATCH_SIZE)
    
    logger.info(
        f"原始数据共 {len(messy_locations)} 条，唯一位置 {unique_count} 个，分 {total_batches} 批次处理"
    )

    # 存储唯一位置的标准化结果
    unique_cleaned_map: Dict[str, str] = {}

    # 处理每个批次
    for batch_index in range(total_batches):
        start = batch_index * BATCH_SIZE
        end = min((batch_index + 1) * BATCH_SIZE, unique_count)
        batch = unique_locs[start:end]
        batch_num = batch_index + 1
        
        logger.info(f"开始处理批次 {batch_num}/{total_batches} (共 {len(batch)} 条)")
        
        payload = build_llm_payload(batch)
        batch_results = call_llm_api(payload, api_key, batch_index, max_retries)
        
        if batch_results:
            # 安全更新映射表
            for item in batch_results:
                loc = item["input_location"]
                code = item["standardized_country_code"]
                unique_cleaned_map[loc] = code
            logger.info(f"批次 {batch_num} 处理完成，成功解析 {len(batch_results)} 条记录")
        else:
            logger.error(f"批次 {batch_num} 处理失败，跳过该批次")

    # 映射回原始列表
    final_results = []
    for raw_loc in messy_locations:
        final_results.append({
            "input_location": raw_loc,
            "standardized_country_code": unique_cleaned_map.get(raw_loc, "UNK")
        })
    
    return final_results

# ======================== 主函数 ========================
def load_input_data(input_path: str) -> List[str]:
    """加载并校验输入数据"""
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            raise ValueError("输入文件必须是JSON列表")
        if not all(isinstance(item, str) for item in data):
            raise ValueError("列表中所有元素必须是字符串")
        
        logger.info(f"成功加载输入文件: {input_path} (共 {len(data)} 条记录)")
        return data
    
    except FileNotFoundError:
        raise FileNotFoundError(f"输入文件不存在: {input_path}")
    except json.JSONDecodeError:
        raise ValueError(f"输入文件不是有效的JSON格式: {input_path}")
    except Exception as e:
        raise RuntimeError(f"加载输入文件失败: {str(e)}")

def save_output_data(output_path: str, data: List[Dict[str, str]]) -> None:
    """保存输出数据"""
    try:
        # 检查输出目录是否存在
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"结果已保存到: {output_path}")
    
    except IOError as e:
        raise RuntimeError(f"写入输出文件失败: {str(e)}")

def get_demo_data() -> List[str]:
    """生成演示模式的测试数据"""
    base_data = [
        "BeiJing, China", "NYC", "Munich", "CN", "Planet Earth",
        "Near Tokyo Bay, JP", "SF Bay Area", "Republic of India",
        "地球村", "Berlin, Germany", "Unspecified", "London",
        "Paris, FR", "Mexico City", "Sydney, AU"
    ]
    # 扩大数据量并添加重复项
    demo_data = base_data * 10 + ["NYC", "Paris, FR", "San Jose"]
    logger.info(f"演示模式 - 使用 {len(demo_data)} 条模拟数据")
    return demo_data

def main():
    """主函数：解析参数、处理数据、执行清洗流程"""
    parser = ArgumentParser(
        description="利用 LLM 对地理位置数据进行批量清洗和标准化。",
        formatter_class=RawTextHelpFormatter
    )
    
    parser.add_argument(
        "--input",
        metavar="INPUT_FILE",
        help="输入 JSON 文件路径。文件应包含一个待清洗位置字符串的 JSON 列表。"
    )
    
    parser.add_argument(
        "--output",
        required=True,
        metavar="OUTPUT_FILE",
        help="输出 JSON 文件路径。标准化结果将写入此文件。"
    )
    
    parser.add_argument(
        "--demo",
        action="store_true",
        help="运行演示模式，忽略输入文件，使用内置的模拟数据进行测试。"
    )

    parser.add_argument(
        "--api-key",
        default=os.environ.get(ENV_API_KEY_NAME, ""),
        help=f"Gemini API Key。优先级：参数 > 环境变量 {ENV_API_KEY_NAME}。"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"批次大小（默认：{BATCH_SIZE}）。"
    )
    
    args = parser.parse_args()

    # 1. 校验API Key
    api_key = args.api_key.strip()
    if not validate_api_key(api_key):
        logger.error(
            "API密钥无效！请通过 --api-key 参数或设置 "
            f"{ENV_API_KEY_NAME} 环境变量提供有效的Gemini API Key。"
        )
        sys.exit(1)

    # 2. 加载输入数据
    try:
        if args.demo:
            input_data = get_demo_data()
        else:
            if not args.input:
                parser.error("--demo 未启用时，必须指定 --input 参数")
            input_data = load_input_data(args.input)
    except Exception as e:
        logger.error(f"加载输入数据失败: {str(e)}")
        sys.exit(1)

    # 3. 执行数据清洗
    try:
        cleaned_results = clean_locations_batched(
            input_data,
            api_key,
            max_retries=MAX_RETRIES
        )
    except Exception as e:
        logger.error(f"数据清洗过程出错: {str(e)}")
        sys.exit(1)

    # 4. 保存结果
    if cleaned_results:
        logger.info(f"清洗完成 - 共处理 {len(cleaned_results)} 条记录")
        try:
            save_output_data(args.output, cleaned_results)
        except Exception as e:
            logger.error(f"保存结果失败: {str(e)}")
            sys.exit(1)
    else:
        logger.error("清洗结果为空，未生成输出文件")
        sys.exit(1)

if __name__ == "__main__":
    main()
