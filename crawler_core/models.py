from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from playwright.async_api import Browser, BrowserContext

from .constants import *
from .utils import *

@dataclass
class QueueItem:
    url: str
    depth: int
    discovered_from: str
    discovery_method: str
    attempts: int = 0


@dataclass
class Discovery:
    source_url: str
    target_url: str
    depth: int
    method: str
    same_site: bool
    queueable: bool
    note: str = ""


@dataclass
class PageVisit:
    requested_url: str
    final_url: str
    depth: int
    page_kind: str
    proxy: str
    title: str
    ok: bool
    error: str = ""
    discoveries: int = 0
    started_at: float = 0.0
    finished_at: float = 0.0

    @property
    def duration_ms(self) -> int:
        return int((self.finished_at - self.started_at) * 1000)


@dataclass(frozen=True)
class PortalAjaxAction:
    url: str
    payload: Dict[str, Any]
    method: str


@dataclass
class BatchConfig:
    input_urls_file: str
    output_root: str
    chromium_executable_path: str = ""
    log_level: str = "INFO"
    log_to_file: bool = True
    headless: bool = True
    aggressive_same_site_crawl: bool = True
    worker_process_count: int = 1
    max_concurrency: int = 8
    max_site_concurrency: int = 1
    max_heavy_page_concurrency: int = 0
    max_light_page_concurrency: int = 0
    max_pages_per_session: int = 0
    max_api_expansion_concurrency: int = 0
    page_timeout_ms: int = 20000
    settle_ms: int = 900
    heavy_page_settle_ms: int = 1800
    light_page_settle_ms: int = 500
    response_grace_ms: int = 1200
    transient_page_retry_limit: int = 1
    max_pages_per_site: int = 0
    checkpoint_every_pages: int = 10
    checkpoint_every_seconds: int = 30
    write_full_outputs_on_checkpoint: bool = True
    skip_completed_sites: bool = True
    visit_leaf_pages: bool = True
    include_site_homepage_seed: bool = True
    enable_generic_interactions: bool = True
    max_interaction_clicks_per_page: int = 40
    enable_cbpt_portal_ajax_expansion: bool = True
    max_cbpt_portal_ajax_requests_per_page: int = 12
    max_api_pages_per_series: int = 0
    enable_waf_slider_solver: bool = True
    max_waf_slider_attempts: int = 12
    waf_slider_candidate_count: int = 5
    playwright_driver_pool_size: int = 1
    session_rebuild_retries: int = 1
    session_failure_threshold: int = 2
    session_cooldown_seconds: int = 30
    proxy_servers: List[Dict[str, str]] = field(default_factory=list)
    proxy_session_count: int = 0
    skip_failed_proxies: bool = True
    browser_launch_args: List[str] = field(default_factory=list)
    enable_request_blocking: bool = True
    blocked_resource_types: List[str] = field(default_factory=list)
    blocked_url_suffixes: List[str] = field(default_factory=list)

    @classmethod
    def from_file(cls, path: Union[str, Path]) -> "BatchConfig":
        payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
        base_dir = Path(path).resolve().parent
        return cls(
            input_urls_file=str(payload.get("input_urls_file", DEFAULT_INPUT_URLS_FILE)),
            output_root=str(payload.get("output_root", "crawl_output")),
            chromium_executable_path=resolve_optional_path(payload.get("chromium_executable_path", ""), base_dir),
            log_level=str(payload.get("log_level", "INFO")).upper(),
            log_to_file=bool(payload.get("log_to_file", True)),
            headless=bool(payload.get("headless", True)),
            aggressive_same_site_crawl=bool(payload.get("aggressive_same_site_crawl", True)),
            worker_process_count=max(1, int(payload.get("worker_process_count", 1))),
            max_concurrency=int(payload.get("max_concurrency", 8)),
            max_site_concurrency=max(1, int(payload.get("max_site_concurrency", 1))),
            max_heavy_page_concurrency=max(0, int(payload.get("max_heavy_page_concurrency", 0))),
            max_light_page_concurrency=max(0, int(payload.get("max_light_page_concurrency", 0))),
            max_pages_per_session=max(0, int(payload.get("max_pages_per_session", 0))),
            max_api_expansion_concurrency=max(0, int(payload.get("max_api_expansion_concurrency", 0))),
            page_timeout_ms=int(payload.get("page_timeout_ms", 20000)),
            settle_ms=int(payload.get("settle_ms", 900)),
            heavy_page_settle_ms=max(0, int(payload.get("heavy_page_settle_ms", payload.get("settle_ms", 900)))),
            light_page_settle_ms=max(0, int(payload.get("light_page_settle_ms", min(int(payload.get("settle_ms", 900)), 500)))),
            response_grace_ms=max(0, int(payload.get("response_grace_ms", 1200))),
            transient_page_retry_limit=max(0, int(payload.get("transient_page_retry_limit", 1))),
            max_pages_per_site=int(payload.get("max_pages_per_site", 0)),
            checkpoint_every_pages=max(1, int(payload.get("checkpoint_every_pages", 10))),
            checkpoint_every_seconds=max(1, int(payload.get("checkpoint_every_seconds", 30))),
            write_full_outputs_on_checkpoint=bool(payload.get("write_full_outputs_on_checkpoint", True)),
            skip_completed_sites=bool(payload.get("skip_completed_sites", True)),
            visit_leaf_pages=bool(payload.get("visit_leaf_pages", True)),
            include_site_homepage_seed=bool(payload.get("include_site_homepage_seed", True)),
            enable_generic_interactions=bool(payload.get("enable_generic_interactions", True)),
            max_interaction_clicks_per_page=max(0, int(payload.get("max_interaction_clicks_per_page", 40))),
            enable_cbpt_portal_ajax_expansion=bool(payload.get("enable_cbpt_portal_ajax_expansion", True)),
            max_cbpt_portal_ajax_requests_per_page=max(0, int(payload.get("max_cbpt_portal_ajax_requests_per_page", 12))),
            max_api_pages_per_series=max(0, int(payload.get("max_api_pages_per_series", 0))),
            enable_waf_slider_solver=bool(payload.get("enable_waf_slider_solver", True)),
            max_waf_slider_attempts=max(0, int(payload.get("max_waf_slider_attempts", 12))),
            waf_slider_candidate_count=max(1, int(payload.get("waf_slider_candidate_count", 5))),
            playwright_driver_pool_size=max(1, int(payload.get("playwright_driver_pool_size", 1))),
            session_rebuild_retries=max(0, int(payload.get("session_rebuild_retries", 1))),
            session_failure_threshold=max(1, int(payload.get("session_failure_threshold", 2))),
            session_cooldown_seconds=max(1, int(payload.get("session_cooldown_seconds", 30))),
            proxy_servers=load_proxy_servers(payload.get("proxy_servers")),
            proxy_session_count=max(0, int(payload.get("proxy_session_count", 0))),
            skip_failed_proxies=bool(payload.get("skip_failed_proxies", True)),
            browser_launch_args=normalize_string_list(payload.get("browser_launch_args")) or list(DEFAULT_BROWSER_LAUNCH_ARGS),
            enable_request_blocking=bool(payload.get("enable_request_blocking", True)),
            blocked_resource_types=normalize_string_list(payload.get("blocked_resource_types"), lower=True) or list(DEFAULT_BLOCKED_RESOURCE_TYPES),
            blocked_url_suffixes=normalize_string_list(payload.get("blocked_url_suffixes"), lower=True) or list(DEFAULT_BLOCKED_URL_SUFFIXES),
        )


@dataclass
class SiteConfig:
    site_key: str
    site_host: str
    site_origin: str
    output_dir: Path
    seed_urls: list[str]
    chromium_executable_path: str
    log_level: str
    log_to_file: bool
    headless: bool
    aggressive_same_site_crawl: bool
    max_concurrency: int
    max_heavy_page_concurrency: int
    max_light_page_concurrency: int
    max_pages_per_session: int
    max_api_expansion_concurrency: int
    timeout_ms: int
    settle_ms: int
    heavy_page_settle_ms: int
    light_page_settle_ms: int
    response_grace_ms: int
    transient_page_retry_limit: int
    page_limit: int
    checkpoint_every_pages: int
    checkpoint_every_seconds: int
    write_full_outputs_on_checkpoint: bool
    visit_leaf_pages: bool
    enable_generic_interactions: bool
    max_interaction_clicks_per_page: int
    enable_cbpt_portal_ajax_expansion: bool
    max_cbpt_portal_ajax_requests_per_page: int
    max_api_pages_per_series: int
    enable_waf_slider_solver: bool
    max_waf_slider_attempts: int
    waf_slider_candidate_count: int
    playwright_driver_pool_size: int
    session_rebuild_retries: int
    session_failure_threshold: int
    session_cooldown_seconds: int
    proxy_servers: List[Dict[str, str]]
    proxy_session_count: int
    skip_failed_proxies: bool
    browser_launch_args: List[str]
    enable_request_blocking: bool
    blocked_resource_types: List[str]
    blocked_url_suffixes: List[str]


@dataclass
class CrawlerSession:
    index: int
    proxy_label: str
    proxy_entry: Optional[Dict[str, str]]
    browser: Optional[Browser]
    context: Optional[BrowserContext]
    api_context: Any
    api_mode: str = "request"
    active_pages: int = 0
    max_pages: int = 0
    consecutive_failures: int = 0
    rebuild_count: int = 0
    unhealthy_until: float = 0.0
    last_error: str = ""
    draining: bool = False
    pending_rebuild_reason: str = ""
    rebuild_lock: Any = field(default_factory=asyncio.Lock)
