from __future__ import annotations

import asyncio
import json
import os

from .batch import BatchRunner
from .constants import DEFAULT_CONFIG_PATH

async def async_main(config_path: str = DEFAULT_CONFIG_PATH) -> int:
    resolved_config_path = os.environ.get('CRAWLER_CONFIG_PATH', config_path)
    runner = BatchRunner(config_path=resolved_config_path)
    summary = await runner.run()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0

def main(config_path: str = DEFAULT_CONFIG_PATH) -> int:
    return asyncio.run(async_main(config_path=config_path))
