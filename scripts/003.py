# -*- coding: utf-8 -*-

import requests
import json
import time
import sys
import random
import argparse
import math

API_KEY = ""
API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent"
MODEL = "gemini-2.5-flash-preview-09-2025"
BATCH_SIZE = 50

LOCATION_SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "input_location": { 
                "type": "STRING", 
            },
            "standardized_country_code": { 
                "type": "STRING", 
            }
        },
        "required": ["input_location", "standardized_country_code"]
    }
}

def llm_clean_locations_batched(messy_locations: list, max_retries: int = 5) -> list:
    if not messy_locations:
        return []

    all_results = []
    num_total = len(messy_locations)
    num_batches = math.ceil(num_total / BATCH_SIZE)

    system_instruction = (
        "你是一个专业的地理位置数据清洗和标准化引擎。你的任务是分析提供的原始位置字符串列表，"
        "并将其映射到 ISO 3166-1 Alpha-3 国家代码。你必须严格按照提供的 JSON 模式输出。 "
        "如果一个位置无法识别，请使用 'UNK'。"
    )

    for batch_index in range(num_batches):
        start = batch_index * BATCH_SIZE
        end = min((batch_index + 1) * BATCH_SIZE, num_total)
        batch = messy_locations[start:end]
        
        print(f"-> 正在发送批次 {batch_index + 1}/{num_batches} ({len(batch)} 条记录)...")
        
        user_query = f"请将以下原始地理位置字符串列表标准化为 ISO 3166-1 Alpha-3 国家代码：\n{json.dumps(batch, ensure_ascii=False)}"
        
        payload = {
            "contents": [{ "parts": [{ "text": user_query }] }],
            "systemInstruction": { "parts": [{ "text": system_instruction }] },
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": LOCATION_SCHEMA
            }
        }

        for i in range(max_retries):
            try:
                url = f"{API_URL}?key={API_KEY}"
                response = requests.post(
                    url, 
                    headers={'Content-Type': 'application/json'},
                    data=json.dumps(payload)
                )
                response.raise_for_status()
                
                result = response.json()
                candidate = result.get('candidates', [{}])[0]
                
                if candidate and candidate.get('content') and candidate['content'].get('parts'):
                    json_text = candidate['content']['parts'][0].get('text')
                    if json_text:
                        all_results.extend(json.loads(json_text))
                        break

                print(f"警告: 批次 {batch_index + 1} 响应结构有效，但未包含预期的 JSON。")
                if i == max_retries - 1:
                     print(f"错误: 批次 {batch_index + 1} 重试失败，跳过此批次。", file=sys.stderr)
                     
            except requests.exceptions.RequestException as e:
                if i < max_retries - 1:
                    delay = 2 ** i + random.uniform(0, 1)
                    print(f"API 请求失败: {e}。等待 {delay:.2f} 秒后重试...")
                    time.sleep(delay)
                else:
                    print(f"错误: 批次 {batch_index + 1} 达到最大重试次数。最后错误: {e}", file=sys.stderr)
                    break 
            except json.JSONDecodeError:
                print(f"错误: 批次 {batch_index + 1} LLM 返回了无效的 JSON 格式。", file=sys.stderr)
                break 

    return all_results

def main():
    parser = argparse.ArgumentParser(
        description="利用 LLM 对地理位置数据进行批量清洗和标准化。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "--input",
        required=True,
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
        action='store_true',
        help="运行演示模式，忽略输入文件，使用内置的模拟数据进行测试。"
    )
    
    args = parser.parse_args()

    if args.demo:
        messy_data = [
            "BeiJing, China", "NYC", "Munich", "CN", "Planet Earth", 
            "Near Tokyo Bay, JP", "SF Bay Area", "Republic of India", 
            "地球村", "Berlin, Germany", "Unspecified", "London", 
            "Paris, FR", "Mexico City", "Sydney, AU"
        ]
        input_data = messy_data * 5 # 扩大到 75 条记录，演示批处理
        print(f"--- 演示模式：使用 {len(input_data)} 条模拟数据进行清洗 ---")
    else:
        try:
            with open(args.input, 'r', encoding='utf-8') as f:
                input_data = json.load(f)
            if not isinstance(input_data, list) or not all(isinstance(i, str) for i in input_data):
                print("错误：输入文件必须是一个包含字符串的 JSON 列表。", file=sys.stderr)
                sys.exit(1)
            print(f"--- 成功加载 {len(input_data)} 条记录自 {args.input} ---")
        except FileNotFoundError:
            print(f"错误：输入文件未找到 {args.input}", file=sys.stderr)
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"错误：文件 {args.input} 不是有效的 JSON 格式。", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"加载输入文件时发生错误: {e}", file=sys.stderr)
            sys.exit(1)

    cleaned_results = llm_clean_locations_batched(input_data)

    if cleaned_results:
        print(f"\n--- 成功清洗 {len(cleaned_results)} 条记录 ---")
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(cleaned_results, f, indent=2, ensure_ascii=False)
            print(f"标准化结果已保存到 {args.output}")
        except IOError as e:
            print(f"错误：无法写入输出文件 {args.output}. {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("\n清洗失败或返回结果为空。未生成输出文件。", file=sys.stderr)

if __name__ == "__main__":
    main()
