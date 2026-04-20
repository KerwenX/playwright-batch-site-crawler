#!/usr/bin/env python3
import os

from site_batch_crawler import main


DEFAULT_SERVER_CONFIG = "config.server.json"


if __name__ == "__main__":
    raise SystemExit(main(os.environ.get("CRAWLER_CONFIG_PATH", DEFAULT_SERVER_CONFIG)))
