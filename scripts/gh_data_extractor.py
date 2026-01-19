import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
import argparse
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, AsyncGenerator, Callable, Set
from functools import wraps

# -----------------------------------------------------------------------------
# 依赖检查与导入
# -----------------------------------------------------------------------------
try:
    import aiohttp
    import yaml
    from tqdm.asyncio import tqdm
except ImportError as e:
    print(f"CRITICAL ERROR: 缺少必要依赖库: {e.name}")
    print("请运行: pip install aiohttp tqdm PyYAML")
    sys.exit(1)

# Windows 平台下的 asyncio 兼容性设置
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
