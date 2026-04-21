#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import csv
import html as html_lib
import io
import json
import logging
import os
import re
import time
import traceback
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from playwright.async_api import (
    Browser,
    BrowserContext,
    Download,
    Page,
    Playwright,
    Response,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

DEFAULT_CONFIG_PATH = "config.json"
DEFAULT_INPUT_URLS_FILE = "input_urls.txt"
CRAWL_POLICY_VERSION = 3
AJCASS_HOST = "zgncjj.ajcass.com"
AJCASS_HOST_SUFFIX = ".ajcass.com"
AJCASS_SITE_CONTENT_API = "https://api.ajcass.com/api/JournalInfoApi/GetSiteContentPageList"
AJCASS_ISSUE_SEARCH_API = "https://api.ajcass.com/api/IssueContentApi/GetIssueNormalSearch"
AJCASS_ISSUE_SIMPLE_API = "https://api.ajcass.com/api/IssueContentApi/GetIssueSimpleSearch"
AJCASS_ROUTE_PATHS = {
    "/",
    "/index",
    "/detail",
    "/search",
    "/issue",
    "/issueDetail",
    "/enIndex",
    "/enIssue",
}
AJCASS_LEAF_PAGE_KINDS = {"detail", "issue_detail", "english_issue"}
AJCASS_FRAGMENT_PARAM_KEEP = {
    "/detail": {"channelId", "id", "title", "parentId", "type"},
    "/index": {"id", "title", "type"},
    "/search": {"author", "authors", "channelId", "curr", "issue", "keyword", "keywords", "page", "title", "unit", "year"},
    "/issue": {"id", "issue", "title", "year"},
    "/issueDetail": {"contentId", "issue", "title", "year"},
    "/enIndex": {"id", "title", "type"},
    "/enIssue": {"contentId", "title"},
}
AJCASS_SCRIPT_ROUTE_CANDIDATES = [
    "/index",
    "/detail",
    "/search",
    "/issueDetail",
    "/issue",
    "/enIndex",
    "/enIssue",
]
NON_HTML_SUFFIXES = {
    ".7z",
    ".avi",
    ".bmp",
    ".csv",
    ".css",
    ".doc",
    ".docx",
    ".eot",
    ".epub",
    ".gif",
    ".gz",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".m4a",
    ".m4v",
    ".md",
    ".mov",
    ".mp3",
    ".mp4",
    ".ods",
    ".odt",
    ".pdf",
    ".png",
    ".ppt",
    ".pptx",
    ".rar",
    ".rtf",
    ".svg",
    ".tar",
    ".tgz",
    ".ttf",
    ".ts",
    ".txt",
    ".wav",
    ".webm",
    ".webp",
    ".woff",
    ".woff2",
    ".xls",
    ".xlsx",
    ".xml",
    ".zip",
}
URL_REGEX = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
RELATIVE_URL_REGEX = re.compile(r"(?P<url>(?:/|\./|\.\./|\?)[^\s\"'<>]+)")
HASH_ROUTE_REGEX = re.compile(r"#/[A-Za-z0-9_./?=&%-]+")
HTML_ATTR_REGEX = re.compile(
    r"(?P<attr>href|src|action|data-href|data-url|data-src|poster|onclick)\s*=\s*(?P<quote>[\"'])(?P<value>.*?)(?P=quote)",
    re.IGNORECASE | re.DOTALL,
)
GENERIC_SCRIPT_ROUTE_REGEX = re.compile(
    r"(?:path|redirect|to)\s*:\s*[\"'](?P<route>/[A-Za-z0-9_./?=&%-]*)[\"']",
    re.IGNORECASE,
)
STATIC_ASSET_PREFIXES = (
    "/dist",
    "/src",
    "/assets",
    "/static",
    "/_nuxt",
    "/js",
    "/css",
    "/img",
    "/image",
    "/images",
    "/fonts",
    "/media",
    "/scripts",
)
BOYUAN_API_HOST = "uniapp.boyuancb.com"
BOYUAN_SITE_WEB_API_PREFIX = f"https://{BOYUAN_API_HOST}/api/SiteWebApi/"
BOYUAN_JOURNAL_INFO_API_PREFIX = f"https://{BOYUAN_API_HOST}/api/JournalInfoApi/"
JS_CALL_REGEX = re.compile(r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\((?P<args>.*)\)\s*;?\s*$", re.DOTALL)
JS_STRING_ARG_REGEX = re.compile(r"([\"'])(.*?)(?<!\\)\1", re.DOTALL)
AJCASS_ROUTE_REGEX = re.compile(
    r"(?:^|[\"'=])(?P<route>(?:/|#/)("
    r"index|detail|search|issueDetail|issue|enIndex|enIssue"
    r")[^\s\"'<>]*)",
    re.IGNORECASE,
)
TRACKING_QUERY_KEYS = {
    "from",
    "source",
    "spm",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}
UNSAFE_ACTION_PATH_SEGMENTS = {
    "delete",
    "del",
    "exit",
    "logoff",
    "logout",
    "quit",
    "remove",
    "signoff",
    "signout",
}
UNSAFE_ACTION_QUERY_PAIRS = {
    ("action", "delete"),
    ("action", "del"),
    ("action", "logout"),
    ("action", "logoff"),
    ("action", "quit"),
    ("action", "remove"),
    ("do", "logout"),
    ("method", "delete"),
    ("method", "logout"),
    ("op", "delete"),
    ("op", "logout"),
}
DEFAULT_BROWSER_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-gpu",
    "--disable-dev-shm-usage",
    "--incognito",
]
DEFAULT_BLOCKED_RESOURCE_TYPES = [
    "image",
    "media",
    "font",
    "ping",
]
DEFAULT_BLOCKED_URL_SUFFIXES = [
    ".bmp",
    ".eot",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".m4s",
    ".mp3",
    ".mp4",
    ".png",
    ".svg",
    ".ttf",
    ".wav",
    ".webm",
    ".webp",
    ".woff",
    ".woff2",
]
FORCE_OPEN_SHADOW_ROOTS_SCRIPT = """(function() {
    if (!Element.prototype._attachShadow) {
        Element.prototype._attachShadow = Element.prototype.attachShadow;
        Element.prototype.attachShadow = function () {
            return this._attachShadow({mode:'open'});
        };
    }
})();"""


@dataclass
class QueueItem:
    url: str
    depth: int
    discovered_from: str
    discovery_method: str


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
    max_concurrency: int = 8
    max_site_concurrency: int = 1
    page_timeout_ms: int = 20000
    settle_ms: int = 900
    max_pages_per_site: int = 0
    checkpoint_every_pages: int = 10
    checkpoint_every_seconds: int = 30
    write_full_outputs_on_checkpoint: bool = True
    skip_completed_sites: bool = True
    visit_leaf_pages: bool = True
    include_site_homepage_seed: bool = True
    enable_generic_interactions: bool = True
    max_interaction_clicks_per_page: int = 18
    enable_cbpt_portal_ajax_expansion: bool = True
    max_cbpt_portal_ajax_requests_per_page: int = 12
    max_api_pages_per_series: int = 0
    proxy_servers: List[Dict[str, str]] = field(default_factory=list)
    proxy_session_count: int = 0
    skip_failed_proxies: bool = True
    browser_launch_args: List[str] = field(default_factory=list)
    enable_request_blocking: bool = True
    blocked_resource_types: List[str] = field(default_factory=list)
    blocked_url_suffixes: List[str] = field(default_factory=list)

    @classmethod
    def from_file(cls, path: Union[str, Path]) -> "BatchConfig":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        base_dir = Path(path).resolve().parent
        return cls(
            input_urls_file=str(payload.get("input_urls_file", DEFAULT_INPUT_URLS_FILE)),
            output_root=str(payload.get("output_root", "crawl_output")),
            chromium_executable_path=resolve_optional_path(payload.get("chromium_executable_path", ""), base_dir),
            log_level=str(payload.get("log_level", "INFO")).upper(),
            log_to_file=bool(payload.get("log_to_file", True)),
            headless=bool(payload.get("headless", True)),
            max_concurrency=int(payload.get("max_concurrency", 8)),
            max_site_concurrency=max(1, int(payload.get("max_site_concurrency", 1))),
            page_timeout_ms=int(payload.get("page_timeout_ms", 20000)),
            settle_ms=int(payload.get("settle_ms", 900)),
            max_pages_per_site=int(payload.get("max_pages_per_site", 0)),
            checkpoint_every_pages=max(1, int(payload.get("checkpoint_every_pages", 10))),
            checkpoint_every_seconds=max(1, int(payload.get("checkpoint_every_seconds", 30))),
            write_full_outputs_on_checkpoint=bool(payload.get("write_full_outputs_on_checkpoint", True)),
            skip_completed_sites=bool(payload.get("skip_completed_sites", True)),
            visit_leaf_pages=bool(payload.get("visit_leaf_pages", True)),
            include_site_homepage_seed=bool(payload.get("include_site_homepage_seed", True)),
            enable_generic_interactions=bool(payload.get("enable_generic_interactions", True)),
            max_interaction_clicks_per_page=max(0, int(payload.get("max_interaction_clicks_per_page", 18))),
            enable_cbpt_portal_ajax_expansion=bool(payload.get("enable_cbpt_portal_ajax_expansion", True)),
            max_cbpt_portal_ajax_requests_per_page=max(0, int(payload.get("max_cbpt_portal_ajax_requests_per_page", 12))),
            max_api_pages_per_series=max(0, int(payload.get("max_api_pages_per_series", 0))),
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
    max_concurrency: int
    timeout_ms: int
    settle_ms: int
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
    browser: Browser
    context: BrowserContext
    api_context: Any


def atomic_write_text(path: Path, content: str) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)


def atomic_write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    atomic_write_text(path, buffer.getvalue())


def sanitize_site_key(site_key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", site_key.lower()).strip("_")


def resolve_optional_path(raw_value: Any, base_dir: Path) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    expanded = Path(os.path.expandvars(os.path.expanduser(value)))
    if not expanded.is_absolute():
        expanded = (base_dir / expanded).resolve()
    return str(expanded)


def normalize_string_list(raw_value: Any, lower: bool = False) -> List[str]:
    values = raw_value if isinstance(raw_value, list) else []
    results = []
    for item in values:
        text = str(item or "").strip()
        if not text:
            continue
        results.append(text.lower() if lower else text)
    return results


def load_proxy_servers(raw_value: Any) -> List[Dict[str, str]]:
    items = raw_value if isinstance(raw_value, list) else []
    proxies = []
    for item in items:
        if isinstance(item, str):
            server = item.strip()
            if not server:
                continue
            proxies.append({"server": server, "username": "", "password": "", "label": server})
            continue
        if isinstance(item, dict):
            server = str(item.get("server") or "").strip()
            if not server:
                continue
            proxies.append(
                {
                    "server": server,
                    "username": str(item.get("username") or "").strip(),
                    "password": str(item.get("password") or "").strip(),
                    "label": str(item.get("label") or server).strip(),
                }
            )
    return proxies


def build_playwright_proxy_settings(proxy_entry: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
    if not proxy_entry:
        return None
    server = str(proxy_entry.get("server") or "").strip()
    if not server:
        return None
    settings = {"server": server}
    username = str(proxy_entry.get("username") or "").strip()
    password = str(proxy_entry.get("password") or "").strip()
    if username:
        settings["username"] = username
    if password:
        settings["password"] = password
    return settings


def get_proxy_label(proxy_entry: Optional[Dict[str, str]]) -> str:
    if not proxy_entry:
        return "<direct>"
    label = str(proxy_entry.get("label") or proxy_entry.get("server") or "").strip()
    return label or "<direct>"


def normalize_log_level(raw_value: Any) -> int:
    value = str(raw_value or "INFO").upper()
    return getattr(logging, value, logging.INFO)


def reset_logger_handlers(logger: logging.Logger) -> None:
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass


def configure_logger(name: str, level_name: str, *, log_file: Optional[Path] = None) -> logging.Logger:
    level = normalize_log_level(level_name)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    reset_logger_handlers(logger)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def truncate_text(value: str, limit: int = 120) -> str:
    text = " ".join((value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def cbpt_query_params(url: str) -> Dict[str, str]:
    parts = urlsplit(url)
    return {
        key: value
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if value != ""
    }


def unescape_js_string(value: str) -> str:
    return (
        value.replace("\\\\", "\\")
        .replace("\\'", "'")
        .replace('\\"', '"')
        .strip()
    )


def parse_js_call(value: str) -> Tuple[Optional[str], List[str]]:
    match = JS_CALL_REGEX.match((value or "").strip())
    if not match:
        return None, []
    args = [unescape_js_string(item.group(2)) for item in JS_STRING_ARG_REGEX.finditer(match.group("args"))]
    return match.group("name"), args


def is_probably_unsafe_action_url(url: str) -> bool:
    parts = urlsplit(url)
    path_segments = [segment for segment in parts.path.lower().split("/") if segment]
    if any(segment in UNSAFE_ACTION_PATH_SEGMENTS for segment in path_segments):
        return True

    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        lowered_key = key.lower()
        lowered_value = value.lower()
        if (lowered_key, lowered_value) in UNSAFE_ACTION_QUERY_PAIRS:
            return True
        if lowered_key in {"logout", "logoff", "signout", "signoff", "delete", "remove"}:
            return lowered_value not in {"", "0", "false", "no"}
    return False


def checkpoint_matches_current_policy(checkpoint: Dict[str, Any], visit_leaf_pages: bool) -> bool:
    saved_version = int(checkpoint.get("crawl_policy_version", 0) or 0)
    saved_visit_leaf_pages = bool(checkpoint.get("visit_leaf_pages", False))
    return saved_version >= CRAWL_POLICY_VERSION and saved_visit_leaf_pages == visit_leaf_pages


def sort_query(query: str) -> str:
    params = [
        (key, value)
        for key, value in parse_qsl(query, keep_blank_values=True)
        if key not in TRACKING_QUERY_KEYS and value != ""
    ]
    return urlencode(sorted(params), doseq=True)


def is_ajcass_host(host: str) -> bool:
    lowered = host.lower()
    return lowered == "ajcass.com" or lowered.endswith(AJCASS_HOST_SUFFIX)


def normalize_seed_url(raw_url: str) -> Optional[str]:
    candidate = raw_url.strip()
    if not candidate or candidate.startswith("#"):
        return None
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", candidate):
        candidate = f"https://{candidate}"
    parts = urlsplit(candidate)
    if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
        return None
    host = parts.hostname.lower()
    netloc = host
    if parts.port and not ((parts.scheme == "https" and parts.port == 443) or (parts.scheme == "http" and parts.port == 80)):
        netloc = f"{host}:{parts.port}"
    path = parts.path or "/"
    query = sort_query(parts.query)
    return urlunsplit((parts.scheme.lower(), netloc, path, query, parts.fragment))


def load_seed_urls(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input URL file not found: {path}")
    urls: List[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        normalized = normalize_seed_url(line)
        if normalized:
            urls.append(normalized)
    return urls


def group_urls_by_site(urls: List[str], include_homepage_seed: bool) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for url in urls:
        parts = urlsplit(url)
        host = parts.hostname or ""
        if not host:
            continue
        site_key = host
        if parts.port and not ((parts.scheme == "https" and parts.port == 443) or (parts.scheme == "http" and parts.port == 80)):
            site_key = f"{host}:{parts.port}"
        if site_key not in grouped:
            origin = urlunsplit((parts.scheme, site_key, "", "", ""))
            grouped[site_key] = {
                "site_host": host,
                "site_origin": origin.rstrip("/"),
                "seed_urls": [],
            }
        grouped[site_key]["seed_urls"].append(url)

    for site_key, payload in grouped.items():
        seeds = sorted(set(payload["seed_urls"]))
        if include_homepage_seed:
            seeds.insert(0, f"{payload['site_origin']}/")
        payload["seed_urls"] = sorted(set(seeds))
    return grouped


class SiteCrawler:
    def __init__(self, config: SiteConfig, shared_playwright: Optional[Playwright] = None) -> None:
        self.config = config
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.site_host = config.site_host
        self.site_origin = config.site_origin.rstrip("/")
        self.site_family = self.detect_site_family()
        self.is_ajcass = self.site_family == "ajcass"
        self.logger = configure_logger(
            f"crawler.site.{sanitize_site_key(config.site_key)}",
            self.config.log_level,
            log_file=(self.config.output_dir / "crawl.log") if self.config.log_to_file else None,
        )

        self.shared_playwright = shared_playwright
        self.playwright: Optional[Playwright] = None
        self.owns_playwright = False
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.api_context = None
        self.sessions: List[CrawlerSession] = []
        self.session_index = 0

        self.frontier: Deque[QueueItem] = deque()
        self.active_queue_items: Dict[str, QueueItem] = {}
        self.discovered_urls: Dict[str, Dict[str, Any]] = {}
        self.visited_urls: Set[str] = set()
        self.queued_urls: Set[str] = set()
        self.edges: List[Discovery] = []
        self.visits: List[PageVisit] = []
        self.discovered_via_source: Set[Tuple[str, str, str]] = set()
        self.pending_discovered_nodes: List[Dict[str, Any]] = []

        self.expected_issue_search_urls: Set[str] = set()
        self.expected_issue_detail_urls: Set[str] = set()
        self.expected_static_detail_urls: Set[str] = set()
        self.expected_en_issue_urls: Set[str] = set()
        self.processed_api_requests: Set[str] = set()
        self.fetched_api_pages: Set[str] = set()
        self.processed_script_requests: Set[str] = set()
        self.ajcass_known_routes: Set[str] = set()
        self.ajcass_issue_route = "/issueDetail" if self.site_host == AJCASS_HOST else "/issue"
        if self.is_ajcass:
            if self.site_host == AJCASS_HOST:
                self.ajcass_known_routes.update({"/", "/detail", "/search", "/issueDetail", "/enIndex", "/enIssue"})
            else:
                self.ajcass_known_routes.update({"/", "/index", "/issue", "/enIndex", "/enIssue"})

        self.completed = False
        self.last_checkpoint_at = 0.0
        self.pages_since_checkpoint = 0

        self.checkpoint_path = self.config.output_dir / "checkpoint.json"
        self.summary_path = self.config.output_dir / "summary.json"
        self.nodes_path = self.config.output_dir / "nodes.jsonl"
        self.nodes_csv_path = self.config.output_dir / "nodes.csv"
        self.edges_path = self.config.output_dir / "edges.jsonl"
        self.edges_csv_path = self.config.output_dir / "edges.csv"
        self.visits_path = self.config.output_dir / "visits.jsonl"
        self.visits_csv_path = self.config.output_dir / "visits.csv"
        self.all_urls_path = self.config.output_dir / "all_discovered_urls.txt"
        self.all_urls_live_path = self.config.output_dir / "all_discovered_urls.live.txt"
        self.all_urls_live_tsv_path = self.config.output_dir / "all_discovered_urls.live.tsv"
        self.same_site_urls_path = self.config.output_dir / "same_site_urls.txt"
        self.external_urls_path = self.config.output_dir / "external_or_non_queueable_urls.txt"
        self.seed_urls_path = self.config.output_dir / "seed_urls.txt"

        self._load_or_initialize_state()
        self.logger.info(
            "Site crawler initialized site=%s family=%s output_dir=%s seeds=%s discovered=%s visited=%s frontier=%s",
            self.config.site_key,
            self.site_family,
            self.config.output_dir,
            len(self.config.seed_urls),
            len(self.discovered_urls),
            len(self.visited_urls),
            len(self.frontier),
        )

    async def __aenter__(self) -> "SiteCrawler":
        if self.shared_playwright is not None:
            self.playwright = self.shared_playwright
            self.owns_playwright = False
        else:
            self.playwright = await async_playwright().start()
            self.owns_playwright = True
        session_proxies = self.build_session_proxies()
        self.logger.info(
            "Launching crawler sessions count=%s proxies=%s headless=%s chromium_executable_path=%s",
            len(session_proxies),
            [get_proxy_label(item) for item in session_proxies],
            self.config.headless,
            self.config.chromium_executable_path or "<playwright-default>",
        )
        for index, proxy_entry in enumerate(session_proxies, start=1):
            proxy_label = get_proxy_label(proxy_entry)
            browser = None
            context = None
            api_context = None
            try:
                proxy_settings = build_playwright_proxy_settings(proxy_entry)
                launch_kwargs = self.build_launch_kwargs(proxy_settings)
                browser = await self.playwright.chromium.launch(**launch_kwargs)
                context = await browser.new_context(ignore_https_errors=True, accept_downloads=True)
                context.set_default_timeout(self.config.timeout_ms)
                await context.add_init_script(FORCE_OPEN_SHADOW_ROOTS_SCRIPT)
                api_context = await self.build_api_context(proxy_settings)
                self.sessions.append(
                    CrawlerSession(
                        index=index,
                        proxy_label=proxy_label,
                        browser=browser,
                        context=context,
                        api_context=api_context,
                    )
                )
                self.logger.info(
                    "Crawler session ready index=%s proxy=%s timeout_ms=%s settle_ms=%s",
                    index,
                    proxy_label,
                    self.config.timeout_ms,
                    self.config.settle_ms,
                )
            except Exception:
                self.logger.exception("Failed to initialize crawler session index=%s proxy=%s", index, proxy_label)
                if api_context is not None:
                    try:
                        await api_context.dispose()
                    except Exception:
                        pass
                if context is not None:
                    try:
                        await context.close()
                    except Exception:
                        pass
                if browser is not None:
                    try:
                        await browser.close()
                    except Exception:
                        pass
                if not self.config.skip_failed_proxies:
                    raise
        if not self.sessions:
            raise RuntimeError("No crawler sessions were initialized.")
        self.browser = self.sessions[0].browser
        self.context = self.sessions[0].context
        self.api_context = self.sessions[0].api_context
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        for session in self.sessions:
            if session.api_context is not None:
                await session.api_context.dispose()
            if session.context is not None:
                await session.context.close()
            if session.browser is not None:
                await session.browser.close()
        if self.playwright is not None and self.owns_playwright:
            await self.playwright.stop()
        self.sessions = []
        self.logger.info("Browser resources closed site=%s", self.config.site_key)

    def build_session_proxies(self) -> List[Optional[Dict[str, str]]]:
        if not self.config.proxy_servers:
            return [None]
        session_count = self.config.proxy_session_count or min(self.config.max_concurrency, len(self.config.proxy_servers))
        session_count = max(1, session_count)
        proxies = []
        site_offset = sum(ord(ch) for ch in self.config.site_key) % len(self.config.proxy_servers)
        for index in range(session_count):
            proxies.append(self.config.proxy_servers[(site_offset + index) % len(self.config.proxy_servers)])
        return proxies

    def build_launch_kwargs(self, proxy_settings: Optional[Dict[str, str]]) -> Dict[str, Any]:
        launch_kwargs = {
            "headless": self.config.headless,
            "args": list(self.config.browser_launch_args),
        }
        if self.config.chromium_executable_path:
            executable_path = Path(self.config.chromium_executable_path)
            if not executable_path.exists():
                raise FileNotFoundError(
                    "Configured chromium_executable_path does not exist: {0}".format(executable_path)
                )
            launch_kwargs["executable_path"] = str(executable_path)
        if proxy_settings:
            launch_kwargs["proxy"] = proxy_settings
        return launch_kwargs

    async def build_api_context(self, proxy_settings: Optional[Dict[str, str]]) -> Any:
        api_kwargs = {"ignore_https_errors": True}
        if proxy_settings:
            api_kwargs["proxy"] = proxy_settings
        try:
            return await self.playwright.request.new_context(**api_kwargs)
        except TypeError:
            if proxy_settings:
                self.logger.warning(
                    "Playwright request context does not accept proxy in this build; falling back to direct API context proxy=%s",
                    proxy_settings.get("server"),
                )
            return await self.playwright.request.new_context(ignore_https_errors=True)

    def get_next_session(self) -> CrawlerSession:
        if not self.sessions:
            raise RuntimeError("Crawler session pool is not initialized.")
        session = self.sessions[self.session_index % len(self.sessions)]
        self.session_index += 1
        return session

    def frontier_count(self) -> int:
        return len(self.frontier) + len(self.active_queue_items)

    def checkpoint_frontier_items(self) -> List[QueueItem]:
        combined: Dict[str, QueueItem] = {}
        for item in list(self.active_queue_items.values()) + list(self.frontier):
            combined[item.url] = item
        return list(combined.values())

    def flush_incremental_discovery_outputs(self) -> None:
        if not self.pending_discovered_nodes:
            return
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        url_lines = [node["url"] for node in self.pending_discovered_nodes]
        tsv_lines = [
            "\t".join(
                [
                    node["url"],
                    str(bool(node["same_site"])),
                    str(bool(node["queueable"])),
                    str(int(node["first_depth"])),
                    str(node["first_source"]),
                    str(node["first_method"]),
                    str(node["page_kind"]),
                ]
            )
            for node in self.pending_discovered_nodes
        ]
        with self.all_urls_live_path.open("a", encoding="utf-8", newline="") as handle:
            if url_lines:
                handle.write("\n".join(url_lines) + "\n")
        file_exists = self.all_urls_live_tsv_path.exists()
        with self.all_urls_live_tsv_path.open("a", encoding="utf-8", newline="") as handle:
            if not file_exists:
                handle.write("url\tsame_site\tqueueable\tfirst_depth\tfirst_source\tfirst_method\tpage_kind\n")
            if tsv_lines:
                handle.write("\n".join(tsv_lines) + "\n")
        self.logger.debug(
            "Flushed incremental discovered URLs count=%s live_txt=%s live_tsv=%s",
            len(self.pending_discovered_nodes),
            self.all_urls_live_path,
            self.all_urls_live_tsv_path,
        )
        self.pending_discovered_nodes = []

    async def handle_route(self, route) -> None:
        request = route.request
        request_url = request.url.lower()
        resource_type = (request.resource_type or "").lower()
        try:
            if self.config.enable_request_blocking:
                if resource_type in set(self.config.blocked_resource_types):
                    await route.abort()
                    return
                if any(request_url.endswith(suffix) for suffix in self.config.blocked_url_suffixes):
                    await route.abort()
                    return
            await route.continue_()
        except Exception:
            try:
                await route.continue_()
            except Exception:
                pass

    def detect_site_family(self) -> str:
        if is_ajcass_host(self.site_host):
            return "ajcass"
        if self.site_host.endswith(".cbpt.cnki.net"):
            return "cbpt_cnki"
        return "generic"

    def _load_or_initialize_state(self) -> None:
        if self.checkpoint_path.exists():
            payload = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
            self._load_from_checkpoint(payload)
            self._merge_new_seed_urls(self.config.seed_urls)
            self.logger.info(
                "Loaded checkpoint path=%s completed=%s discovered=%s visited=%s frontier=%s",
                self.checkpoint_path,
                self.completed,
                len(self.discovered_urls),
                len(self.visited_urls),
                len(self.frontier),
            )
            return
        self._initialize_from_seed_urls(self.config.seed_urls)
        self.logger.info("Initialized new crawl state from seed URLs count=%s", len(self.config.seed_urls))

    def _load_from_checkpoint(self, payload: Dict[str, Any]) -> None:
        original_frontier_count = len(payload.get("frontier", []))
        policy_matches = checkpoint_matches_current_policy(payload, self.config.visit_leaf_pages)
        self.frontier = deque(QueueItem(**item) for item in payload.get("frontier", []))
        self.active_queue_items = {}
        self.pending_discovered_nodes = []
        self.discovered_urls = {
            item["url"]: item for item in payload.get("discovered_urls", [])
        }
        self.visited_urls = set(payload.get("visited_urls", []))
        self.queued_urls = set(payload.get("queued_urls", []))
        self.edges = [Discovery(**item) for item in payload.get("edges", [])]
        self.visits = []
        for item in payload.get("visits", []):
            if isinstance(item, dict) and "proxy" not in item:
                item = dict(item)
                item["proxy"] = ""
            self.visits.append(PageVisit(**item))
        self.discovered_via_source = {
            (item[0], item[1], item[2]) for item in payload.get("discovered_via_source", [])
        }
        self.expected_issue_search_urls = set(payload.get("expected_issue_search_urls", []))
        self.expected_issue_detail_urls = set(payload.get("expected_issue_detail_urls", []))
        self.expected_static_detail_urls = set(payload.get("expected_static_detail_urls", []))
        self.expected_en_issue_urls = set(payload.get("expected_en_issue_urls", []))
        self.processed_api_requests = set(payload.get("processed_api_requests", []))
        self.fetched_api_pages = set(payload.get("fetched_api_pages", []))
        self.processed_script_requests = set(payload.get("processed_script_requests", []))
        self.ajcass_known_routes = set(payload.get("ajcass_known_routes", self.ajcass_known_routes))
        self.ajcass_issue_route = str(payload.get("ajcass_issue_route", self.ajcass_issue_route))
        self.completed = bool(payload.get("completed", False))
        if not policy_matches and self.completed:
            self.completed = False
            self.logger.info(
                "Checkpoint policy mismatch detected; marking site incomplete old_version=%s new_version=%s old_visit_leaf_pages=%s new_visit_leaf_pages=%s",
                payload.get("crawl_policy_version", 0),
                CRAWL_POLICY_VERSION,
                payload.get("visit_leaf_pages", False),
                self.config.visit_leaf_pages,
            )
        self._refresh_discovered_node_metadata()
        self.frontier = deque(
            item
            for item in self.frontier
            if item.url not in self.visited_urls and self.should_visit_url(item.url)
        )
        filtered_count = original_frontier_count - len(self.frontier)
        if filtered_count > 0:
            self.logger.info(
                "Filtered checkpoint frontier entries removed=%s remaining=%s",
                filtered_count,
                len(self.frontier),
            )
        requeued_count = self._requeue_discovered_urls_if_needed()
        if requeued_count > 0:
            self.logger.info("Requeued discovered URLs after checkpoint restore count=%s frontier=%s", requeued_count, len(self.frontier))

    def _initialize_from_seed_urls(self, seed_urls: List[str]) -> None:
        for seed_url in seed_urls:
            self.enqueue_url(seed_url, depth=0, source_url=seed_url, method="seed")
        self.save_checkpoint(force=True, completed=False)

    def _merge_new_seed_urls(self, seed_urls: List[str]) -> None:
        before_frontier = len(self.frontier)
        for seed_url in seed_urls:
            self.enqueue_url(seed_url, depth=0, source_url=seed_url, method="seed")
        added = len(self.frontier) - before_frontier
        if added > 0:
            self.logger.info("Merged seed URLs added_to_frontier=%s", added)

    def _refresh_discovered_node_metadata(self) -> None:
        for url, node in self.discovered_urls.items():
            same_site = self.is_same_site(url)
            queueable = self.is_queueable(url)
            node["same_site"] = same_site
            node["queueable"] = queueable
            node["page_kind"] = self.page_kind(url) if queueable else "resource"

    def _requeue_discovered_urls_if_needed(self) -> int:
        existing_frontier_urls = {item.url for item in self.frontier}
        requeued = 0
        for url, node in self.discovered_urls.items():
            if not node.get("queueable"):
                continue
            if not self.should_visit_url(url):
                continue
            if url in self.visited_urls or url in self.queued_urls or url in existing_frontier_urls:
                continue
            self.frontier.append(
                QueueItem(
                    url=url,
                    depth=int(node.get("first_depth", 0)),
                    discovered_from=str(node.get("first_source", url)),
                    discovery_method=str(node.get("first_method", "resume")),
                )
            )
            self.queued_urls.add(url)
            existing_frontier_urls.add(url)
            requeued += 1
        return requeued

    def normalize_url(self, raw_url: str, base_url: Optional[str] = None) -> Optional[str]:
        if not raw_url:
            return None
        candidate = raw_url.strip()
        if not candidate:
            return None
        lowered = candidate.lower()
        if lowered.startswith(("javascript:", "mailto:", "tel:", "data:")):
            return None

        if candidate.startswith("#/"):
            candidate = f"{self.site_origin}/{candidate}"
        elif self.is_ajcass and candidate.startswith(("/index", "/detail", "/search", "/issue", "/issueDetail", "/enIndex", "/enIssue")):
            candidate = f"{self.site_origin}/#{candidate}"
        elif base_url:
            candidate = urljoin(base_url, candidate)

        parts = urlsplit(candidate)
        if parts.scheme not in {"http", "https", "blob"}:
            return None
        if parts.scheme == "blob":
            return candidate
        if not parts.hostname:
            return None

        host = parts.hostname.lower()
        scheme = parts.scheme.lower()
        site_scheme = urlsplit(self.site_origin).scheme.lower()
        if host == self.site_host:
            scheme = site_scheme
        netloc = host
        if parts.port and not ((parts.scheme == "https" and parts.port == 443) or (parts.scheme == "http" and parts.port == 80)):
            netloc = f"{host}:{parts.port}"

        path = parts.path or "/"
        if host == self.site_host:
            path = re.sub(r"/{2,}", "/", path)
        query = sort_query(parts.query)
        fragment = parts.fragment
        if self.is_ajcass and host == self.site_host:
            fragment = self._normalize_ajcass_fragment(fragment)
        return urlunsplit((scheme, netloc, path, query, fragment))

    def _normalize_ajcass_fragment(self, fragment: str) -> str:
        raw = fragment[1:] if fragment.startswith("#") else fragment
        if not raw:
            return ""
        if raw == "/":
            return "/"
        if not raw.startswith("/"):
            return raw
        path, _, query = raw.partition("?")
        params = parse_qsl(query, keep_blank_values=True)
        keep = AJCASS_FRAGMENT_PARAM_KEEP.get(path)
        if keep is not None:
            params = [(key, value) for key, value in params if key in keep and value != ""]
        query_string = urlencode(sorted(params), doseq=True)
        return path if not query_string else f"{path}?{query_string}"

    def ajcass_route_from_url(self, url: str) -> str:
        normalized = self.normalize_url(url)
        if not normalized:
            return ""
        fragment = urlsplit(normalized).fragment
        if not fragment:
            return ""
        raw = fragment[1:] if fragment.startswith("#") else fragment
        if not raw.startswith("/"):
            return ""
        return raw.split("?", 1)[0]

    def remember_ajcass_route(self, raw_url: str) -> None:
        if not self.is_ajcass:
            return
        route = self.ajcass_route_from_url(raw_url)
        if not route:
            return
        self.ajcass_known_routes.add(route)
        if route in {"/issue", "/issueDetail"}:
            self.ajcass_issue_route = route

    def has_ajcass_route(self, route: str) -> bool:
        return route in self.ajcass_known_routes

    def is_cbpt_portal_url(self, url: str) -> bool:
        if self.site_family != "cbpt_cnki":
            return False
        normalized = self.normalize_url(url)
        if not normalized:
            return False
        parts = urlsplit(normalized)
        lowered_path = parts.path.lower()
        return parts.hostname == self.site_host and (
            lowered_path == "/portal"
            or lowered_path.startswith("/portal/")
            or "/portal/journal/portal/" in lowered_path
        )

    def build_cbpt_portal_url(self, path: str, query_params: Optional[List[Tuple[str, Any]]] = None) -> str:
        relative_path = path if path.startswith("/") else f"/{path}"
        url = f"{self.site_origin}{relative_path}"
        params = [(key, value) for key, value in (query_params or []) if value not in (None, "")]
        if not params:
            return url
        return f"{url}?{urlencode(params, doseq=True)}"

    def is_urlish_attribute_value(self, value: str) -> bool:
        candidate = (value or "").strip()
        if not candidate:
            return False
        lowered = candidate.lower()
        if lowered.startswith(("http://", "https://", "//", "/", "./", "../", "?", "#/", "data:", "blob:")):
            return True
        return any(token in candidate for token in ("/", ".", "?", "="))

    def is_probably_static_asset_path(self, path: str) -> bool:
        lowered = (path or "/").lower()
        if lowered in STATIC_ASSET_PREFIXES:
            return True
        return any(lowered.startswith(f"{prefix}/") for prefix in STATIC_ASSET_PREFIXES)

    def normalize_generic_route_candidate(self, route: str) -> str:
        candidate = (route or "").strip()
        if not candidate.startswith("/"):
            return ""
        path, separator, query = candidate.partition("?")
        segments = []
        for segment in path.split("/"):
            segment = segment.strip()
            if not segment:
                continue
            if segment in {"*", "(.*)"}:
                continue
            if segment.startswith(":"):
                continue
            segments.append(segment)
        normalized_path = "/" + "/".join(segments) if segments else "/"
        if self.is_probably_static_asset_path(normalized_path):
            return ""
        if normalized_path in {"/", "/404"}:
            return ""
        query_string = sort_query(query) if separator else ""
        return normalized_path if not query_string else f"{normalized_path}?{query_string}"

    def should_parse_script_response_url(self, response_url: str) -> bool:
        normalized = self.normalize_url(response_url)
        if not normalized:
            return False
        parts = urlsplit(normalized)
        if parts.hostname != self.site_host:
            return False
        lowered_path = parts.path.lower()
        return lowered_path.endswith(".js") or self.is_probably_static_asset_path(lowered_path)

    def extract_generic_spa_routes_from_script(self, script_text: str, source_url: str) -> List[Tuple[str, str]]:
        if self.is_ajcass:
            return []
        use_hash_routes = "#/" in source_url or any(
            token in script_text
            for token in (
                "location.hash",
                "hashchange",
                "mode:\"hash\"",
                "mode:'hash'",
                "#/",
            )
        )
        use_history_routes = not use_hash_routes and any(
            token in script_text
            for token in (
                "createWebHistory",
                "mode:\"history\"",
                "mode:'history'",
                "new VueRouter",
                "routes:[",
                "routes = [",
            )
        )
        if not use_hash_routes and not use_history_routes:
            return []
        found: List[Tuple[str, str]] = []
        seen: Set[str] = set()
        for match in GENERIC_SCRIPT_ROUTE_REGEX.finditer(script_text):
            route = self.normalize_generic_route_candidate(match.group("route"))
            if not route or route in seen:
                continue
            seen.add(route)
            if use_hash_routes:
                found.append((f"{self.site_origin}/#{route}", "response:script:route"))
            else:
                found.append((f"{self.site_origin}{route}", "response:script:route"))
        return found

    def extract_cbpt_portal_urls_from_onclick(self, onclick_value: str) -> List[str]:
        if self.site_family != "cbpt_cnki" or not self.site_host.endswith(".cbpt.cnki.net"):
            return []

        name, args = parse_js_call(onclick_value)
        if not name:
            return []

        if name == "goNewList" and len(args) >= 1:
            title = args[1] if len(args) > 1 else ""
            return [self.build_cbpt_portal_url(f"/portal/journal/portal/client/list/{args[0]}", [("title", title)])]
        if name == "goDownloadList" and len(args) >= 1:
            title = args[1] if len(args) > 1 else ""
            return [self.build_cbpt_portal_url(f"/portal/journal/portal/client/download/{args[0]}", [("title", title)])]
        if name == "goLinkpostList" and len(args) >= 1:
            title = args[1] if len(args) > 1 else ""
            return [self.build_cbpt_portal_url(f"/portal/journal/portal/client/linkpost/{args[0]}", [("title", title)])]
        if name == "guokanTurnPageList" and len(args) >= 4:
            return [
                self.build_cbpt_portal_url(
                    "/portal/journal/portal/client/guokan_list",
                    [
                        ("year", args[0]),
                        ("issue", args[1]),
                        ("yearId", args[2]),
                        ("issueId", args[3]),
                    ],
                )
            ]
        if name in {"getChineseHtmlUrl", "getSpecialPDFUrl"} and len(args) >= 1:
            return [
                self.build_cbpt_portal_url(
                    "/portal/journal/portal/journal/api/getChinesDownloadInfoByEnglishPaper",
                    [("contentId", args[0])],
                )
            ]
        if name == "gotoCNKINode" and len(args) >= 1:
            return [
                self.build_cbpt_portal_url(
                    "/portal/journal/portal/journal/api/gotoCNKINodeUrl",
                    [("id", args[0])],
                )
            ]
        if name == "tabPage" and len(args) >= 6:
            page_path = args[3].lstrip("/")
            if not page_path.startswith("portal/journal/portal/"):
                page_path = f"portal/journal/portal/{page_path}"
            return [
                self.build_cbpt_portal_url(
                    f"/{page_path}",
                    [
                        ("year", args[0]),
                        ("issue", args[1]),
                        ("pageNum", args[2]),
                        ("yearId", args[4]),
                        ("issueId", args[5]),
                    ],
                )
            ]
        if name == "lastNextIssue" and len(args) >= 5:
            direction = args[0]
            return [
                self.build_cbpt_portal_url(
                    f"/portal/journal/portal/journal/api/listPrePaperOrNextPaper/{direction}",
                    [
                        ("year", args[1]),
                        ("issue", args[2]),
                        ("yearId", args[3]),
                        ("issueId", args[4]),
                        ("pageNum", 1),
                        ("pageSize", 10),
                    ],
                )
            ]
        return []

    def extract_urls_from_html_fragment(self, html_text: str) -> List[str]:
        urls: List[str] = []
        for match in HTML_ATTR_REGEX.finditer(html_text or ""):
            attr = (match.group("attr") or "").lower()
            value = html_lib.unescape(match.group("value") or "").strip()
            if not value:
                continue
            urls.extend(self.extract_urls_from_string(value, allow_relative=True))
            if attr == "onclick":
                urls.extend(self.extract_cbpt_portal_urls_from_onclick(value))
            elif self.is_urlish_attribute_value(value):
                urls.append(value)
        urls.extend(self.extract_urls_from_string(html_text or "", allow_relative=False))
        return urls

    def cbpt_portal_ajax_action_from_onclick(self, onclick_value: str) -> Optional[PortalAjaxAction]:
        if self.site_family != "cbpt_cnki" or not self.site_host.endswith(".cbpt.cnki.net"):
            return None

        name, args = parse_js_call(onclick_value)
        if name == "tabPage" and len(args) >= 6:
            page_num = str(args[2]).strip()
            if page_num in {"", "1"}:
                return None
            page_path = args[3].lstrip("/")
            if not page_path.startswith("portal/journal/portal/"):
                page_path = f"portal/journal/portal/{page_path}"
            return PortalAjaxAction(
                url=self.build_cbpt_portal_url(f"/{page_path}"),
                payload={
                    "yearId": args[4],
                    "issueId": args[5],
                    "year": args[0],
                    "issue": args[1],
                    "pageNum": page_num,
                    "pageSize": 10,
                    "isSimple": "0",
                },
                method=f"portal:ajax:{name}:{page_num}",
            )
        if name == "lastNextIssue" and len(args) >= 5:
            direction = str(args[0]).strip().lower() or "next"
            return PortalAjaxAction(
                url=self.build_cbpt_portal_url(f"/portal/journal/portal/journal/api/listPrePaperOrNextPaper/{direction}"),
                payload={
                    "yearId": args[3],
                    "issueId": args[4],
                    "year": args[1],
                    "issue": args[2],
                    "pageNum": 1,
                    "pageSize": 10,
                    "isSimple": "0",
                },
                method=f"portal:ajax:{name}:{direction}",
            )
        return None

    async def discover_cbpt_portal_ajax_urls(self, page: Page, source_url: str, page_kind: str) -> List[Tuple[str, str]]:
        if (
            not self.config.enable_cbpt_portal_ajax_expansion
            or self.config.max_cbpt_portal_ajax_requests_per_page <= 0
            or not self.is_cbpt_portal_url(source_url)
            or page_kind not in {"cbpt_portal_index", "cbpt_portal_list", "cbpt_portal_news", "cbpt_portal_aux", "page"}
        ):
            return []

        onclick_values = await page.evaluate(
            """() => Array.from(document.querySelectorAll('[onclick]'))
                .map(el => (el.getAttribute('onclick') || '').trim())
                .filter(Boolean)"""
        )
        actions: List[PortalAjaxAction] = []
        seen: Set[str] = set()
        for onclick_value in onclick_values:
            if not isinstance(onclick_value, str):
                continue
            action = self.cbpt_portal_ajax_action_from_onclick(onclick_value)
            if action is None:
                continue
            action_key = f"{action.url}|{json.dumps(action.payload, ensure_ascii=False, sort_keys=True)}"
            if action_key in seen:
                continue
            seen.add(action_key)
            actions.append(action)

        if not actions:
            return []

        found: List[Tuple[str, str]] = []
        self.logger.debug(
            "CBPT portal ajax expansion start page_kind=%s source=%s actions=%s limit=%s",
            page_kind,
            source_url,
            len(actions),
            self.config.max_cbpt_portal_ajax_requests_per_page,
        )
        for action in actions[: self.config.max_cbpt_portal_ajax_requests_per_page]:
            found.append((action.url, action.method))
            try:
                html_text = await asyncio.wait_for(
                    page.evaluate(
                        """async ({url, payload}) => {
                            const response = await fetch(url, {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json; charset=UTF-8',
                                    'X-Requested-With': 'XMLHttpRequest'
                                },
                                credentials: 'same-origin',
                                body: JSON.stringify(payload)
                            });
                            return await response.text();
                        }""",
                        {"url": action.url, "payload": action.payload},
                    ),
                    timeout=max(5.0, self.config.timeout_ms / 1000.0),
                )
            except Exception as exc:
                self.logger.warning(
                    "CBPT portal ajax expansion failed kind=%s url=%s method=%s error=%s",
                    page_kind,
                    source_url,
                    action.method,
                    exc,
                )
                continue

            for discovered_url in self.extract_urls_from_html_fragment(html_text if isinstance(html_text, str) else ""):
                found.append((discovered_url, f"{action.method}:html"))
        self.logger.debug(
            "CBPT portal ajax expansion end page_kind=%s source=%s discoveries=%s",
            page_kind,
            source_url,
            len(found),
        )
        return found

    def page_kind(self, url: str) -> str:
        normalized = self.normalize_url(url)
        if not normalized:
            return "unknown"
        parts = urlsplit(normalized)
        if self.is_ajcass and parts.hostname == self.site_host:
            route = self.ajcass_route_from_url(normalized)
            if route in {"", "/", "/index"} and parts.path == "/":
                return "root"
            if route == "/search":
                return "issue_search"
            if route in {"/issueDetail", "/issue"}:
                return "issue_detail"
            if route == "/detail":
                return "detail"
            if route == "/enIndex":
                return "english_index"
            if route == "/enIssue":
                return "english_issue"
            if route.startswith("/"):
                return f"spa:{route[1:]}"
        if self.site_family == "cbpt_cnki" and parts.hostname == self.site_host:
            lowered_path = parts.path.lower()
            params = cbpt_query_params(normalized)
            if self.is_cbpt_portal_url(normalized):
                if lowered_path == "/portal" or lowered_path.endswith("/client/index"):
                    return "cbpt_portal_index"
                if "/portal/journal/portal/client/paper/" in lowered_path:
                    return "cbpt_portal_article"
                if "/portal/journal/portal/client/news/" in lowered_path:
                    return "cbpt_portal_news"
                if (
                    "/portal/journal/portal/journal/api/" in lowered_path
                    or "/portal/journal/portal/common/api/" in lowered_path
                    or lowered_path.endswith("/client/paperpage_list")
                ):
                    return "cbpt_portal_api"
                if any(
                    token in lowered_path
                    for token in (
                        "/portal/journal/portal/client/guokan_list",
                        "/portal/journal/portal/client/paper_list/",
                        "/portal/journal/portal/client/paperrank_list/",
                        "/portal/journal/portal/client/shoufa_list",
                        "/portal/journal/portal/client/list/",
                        "/portal/journal/portal/client/download/",
                        "/portal/journal/portal/client/linkpost/",
                    )
                ):
                    return "cbpt_portal_list"
                return "cbpt_portal_aux"
            if lowered_path.startswith("/api/"):
                return "cbpt_aux"
            if lowered_path == "/index.aspx" and params.get("t"):
                return "cbpt_aux"
            if lowered_path.endswith("/showvalidatecode.aspx") or lowered_path.endswith("/validatecode.aspx") or lowered_path.endswith("/error.aspx") or lowered_path.endswith("/quit.aspx"):
                return "cbpt_guard"
            if "/editor" in lowered_path:
                return "cbpt_aux"
            if lowered_path.endswith("/paperdigest.aspx"):
                return "cbpt_article"
            if lowered_path.endswith("/wktextcontent.aspx"):
                if params.get("contentID") or params.get("paperID"):
                    return "cbpt_article"
                if any(params.get(key) for key in ("colType", "tp", "yt", "st", "navigationContentID")):
                    return "cbpt_list"
                return "page"
            if lowered_path.endswith("/wklist.aspx"):
                if params.get("contentID"):
                    return "cbpt_article"
                return "cbpt_list"
            if lowered_path.endswith("/editora3n/index.aspx") or lowered_path.endswith("/wka3/error.aspx"):
                return "cbpt_aux"
            if lowered_path.endswith("/webpublication/index.aspx") and parts.query:
                return "page"
        if parts.path == "/" and not parts.query and not parts.fragment:
            return "root"
        return "page"

    def is_same_site(self, url: str) -> bool:
        normalized = self.normalize_url(url)
        if not normalized:
            return False
        parts = urlsplit(normalized)
        return parts.scheme in {"http", "https"} and parts.hostname == self.site_host

    def is_queueable(self, url: str) -> bool:
        normalized = self.normalize_url(url)
        if not normalized:
            return False
        parts = urlsplit(normalized)
        if parts.scheme not in {"http", "https"} or parts.hostname != self.site_host:
            return False
        if any(parts.path.lower().endswith(suffix) for suffix in NON_HTML_SUFFIXES):
            return False
        if self.is_probably_static_asset_path(parts.path):
            return False
        if is_probably_unsafe_action_url(normalized):
            return False
        if self.site_family == "cbpt_cnki":
            lowered_path = parts.path.lower()
            if (
                lowered_path.endswith("/downloadissueinfo.aspx")
                or lowered_path.endswith("/showvalidatecode.aspx")
                or lowered_path.endswith("/validatecode.aspx")
                or lowered_path.endswith("/quit.aspx")
                or lowered_path.endswith("/wkdownfilebylink.aspx")
                or lowered_path.endswith("/kbdownload.aspx")
            ):
                return False
            if lowered_path.startswith("/api/"):
                return False
            if (
                "/portal/journal/portal/journal/api/" in lowered_path
                or "/portal/journal/portal/common/api/" in lowered_path
                or lowered_path.endswith("/client/paperpage_list")
            ):
                return False
        if self.is_ajcass and parts.fragment:
            return self.ajcass_route_from_url(normalized).startswith("/")
        return True

    def should_visit_url(self, url: str) -> bool:
        if not self.is_queueable(url):
            return False
        if self.config.visit_leaf_pages:
            return True
        if self.is_ajcass and self.page_kind(url) in AJCASS_LEAF_PAGE_KINDS:
            return False
        if self.site_family == "cbpt_cnki" and self.page_kind(url) in {
            "cbpt_article",
            "cbpt_aux",
            "cbpt_guard",
            "cbpt_portal_article",
            "cbpt_portal_api",
            "cbpt_portal_aux",
        }:
            return False
        return True

    def build_ajcass_issue_url(
        self,
        *,
        content_id: Any,
        year: Any = None,
        issue: Any = None,
        title: Optional[str] = None,
        english: bool = False,
    ) -> Optional[str]:
        if not self.is_ajcass or content_id in (None, ""):
            return None

        params: list[tuple[str, Any]]
        if english:
            params = [("contentId", content_id)]
            if title:
                params.append(("title", title))
            return f"{self.site_origin}/#/enIssue?{urlencode(params, doseq=True)}"

        route = self.ajcass_issue_route or ("/issueDetail" if self.site_host == AJCASS_HOST else "/issue")
        if route == "/issueDetail":
            if year in (None, "") or issue in (None, ""):
                return None
            params = [("contentId", content_id), ("issue", issue), ("year", year)]
            if title:
                params.append(("title", title))
            return f"{self.site_origin}/#/issueDetail?{urlencode(params, doseq=True)}"

        params = [("id", content_id)]
        if issue not in (None, ""):
            params.append(("issue", issue))
        if title:
            params.append(("title", title))
        if year not in (None, ""):
            params.append(("year", year))
        return f"{self.site_origin}/#/issue?{urlencode(params, doseq=True)}"

    def register_url(self, raw_url: str, source_url: str, depth: int, method: str, note: str = "") -> Optional[str]:
        normalized = self.normalize_url(raw_url, base_url=source_url)
        if not normalized:
            return None
        self.remember_ajcass_route(normalized)

        same_site = self.is_same_site(normalized)
        queueable = self.is_queueable(normalized)
        page_kind = self.page_kind(normalized) if queueable else "resource"
        edge_key = (source_url, normalized, method)
        if edge_key not in self.discovered_via_source:
            self.discovered_via_source.add(edge_key)
            self.edges.append(
                Discovery(
                    source_url=source_url,
                    target_url=normalized,
                    depth=depth,
                    method=method,
                    same_site=same_site,
                    queueable=queueable,
                    note=note,
                )
            )

        is_new_node = normalized not in self.discovered_urls
        node = self.discovered_urls.setdefault(
            normalized,
            {
                "url": normalized,
                "same_site": same_site,
                "queueable": queueable,
                "first_depth": depth,
                "first_source": source_url,
                "first_method": method,
                "seen_count": 0,
                "page_kind": page_kind,
            },
        )
        node["seen_count"] += 1
        node["first_depth"] = min(node["first_depth"], depth)
        if is_new_node:
            self.pending_discovered_nodes.append(dict(node))
        if self.page_kind(source_url) == "english_index" and page_kind == "english_issue":
            self.expected_en_issue_urls.add(normalized)
        if page_kind == "issue_detail":
            self.expected_issue_detail_urls.add(normalized)
        elif page_kind == "detail":
            self.expected_static_detail_urls.add(normalized)
        return normalized

    def enqueue_url(self, raw_url: str, depth: int, source_url: str, method: str, note: str = "") -> Optional[str]:
        normalized = self.register_url(raw_url, source_url, depth, method, note=note)
        if not normalized:
            return None
        if self.should_visit_url(normalized) and normalized not in self.queued_urls and normalized not in self.visited_urls:
            self.frontier.append(
                QueueItem(
                    url=normalized,
                    depth=depth,
                    discovered_from=source_url,
                    discovery_method=method,
                )
            )
            self.queued_urls.add(normalized)
            self.logger.debug(
                "Queued URL depth=%s method=%s page_kind=%s url=%s",
                depth,
                method,
                self.page_kind(normalized),
                normalized,
            )
        return normalized

    async def settle_page(self, page: Page) -> None:
        try:
            await page.wait_for_load_state("networkidle", timeout=self.config.timeout_ms)
        except PlaywrightTimeoutError:
            pass
        await page.wait_for_timeout(self.config.settle_ms)

    async def extract_dom_urls(self, page: Page) -> list[str]:
        payload = await page.evaluate(
            """() => {
                const attrs = [];
                const seen = new Set();
                const pushAttr = (attr, value) => {
                    if (!value || typeof value !== 'string') return;
                    const trimmed = value.trim();
                    if (!trimmed) return;
                    const key = `${attr}::${trimmed}`;
                    if (seen.has(key)) return;
                    seen.add(key);
                    attrs.push({ attr, value: trimmed });
                };
                for (const el of Array.from(document.querySelectorAll('*'))) {
                    for (const attr of ['href', 'src', 'action', 'data-href', 'data-url', 'data-src', 'poster']) {
                        const value = el.getAttribute(attr);
                        if (value) pushAttr(attr, value);
                    }
                    const onclick = el.getAttribute('onclick');
                    if (onclick) pushAttr('onclick', onclick);
                }
                pushAttr('location', window.location.href);
                return {
                    attrs,
                    html: document.documentElement.outerHTML.slice(0, 2000000),
                };
            }"""
        )
        urls: list[str] = []
        for item in payload.get("attrs", []):
            if not isinstance(item, dict):
                continue
            value = item.get("value")
            if not isinstance(value, str):
                continue
            attr = str(item.get("attr") or "")
            urls.extend(self.extract_urls_from_string(value, allow_relative=True))
            if attr == "onclick":
                urls.extend(self.extract_cbpt_portal_urls_from_onclick(value))
            if attr in {"href", "src", "action", "data-href", "data-url", "data-src", "poster", "location"} and self.is_urlish_attribute_value(value):
                urls.append(value)
        html = payload.get("html")
        if isinstance(html, str):
            urls.extend(self.extract_urls_from_html_fragment(html))
        return urls

    async def probe_click_texts(self, page: Page, source_url: str, depth: int, labels: list[str]) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
        for label in labels:
            locator = page.get_by_text(label, exact=True).first
            try:
                if not await locator.is_visible():
                    continue
            except Exception:
                continue
            found.extend(await self.click_probe(page, locator, source_url, label, "click"))
        return found

    async def click_probe(self, page: Page, locator, source_url: str, label: str, method_prefix: str) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
        method_label = f"{method_prefix}:{label}" if label else method_prefix
        before = page.url
        popup_task = asyncio.create_task(page.wait_for_event("popup", timeout=2500))
        download_task = asyncio.create_task(page.wait_for_event("download", timeout=2500))
        try:
            try:
                await locator.scroll_into_view_if_needed(timeout=1500)
            except Exception:
                pass
            await locator.click(timeout=3000)
            await page.wait_for_timeout(1200)
            await self.settle_page(page)
        except Exception:
            for task in (popup_task, download_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(popup_task, download_task, return_exceptions=True)
            if page.url != source_url:
                try:
                    await page.goto(source_url, wait_until="domcontentloaded")
                    await self.settle_page(page)
                except Exception:
                    pass
            return found

        popup_result, download_result = await asyncio.gather(
            popup_task,
            download_task,
            return_exceptions=True,
        )
        popup_page = popup_result if isinstance(popup_result, Page) else None
        download_obj = download_result if isinstance(download_result, Download) else None

        after = page.url
        if after != before:
            self.remember_ajcass_route(after)
            found.append((after, method_label))

        if popup_page is not None:
            try:
                await popup_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            popup_url = popup_page.url
            if popup_url:
                found.append((popup_url, f"popup:{label}"))
            await popup_page.close()

        if download_obj is not None:
            download_url = download_obj.url
            if download_url and not download_url.startswith("blob:"):
                found.append((download_url, f"download:{label}"))

        for dom_url in await self.extract_dom_urls(page):
            found.append((dom_url, f"{method_label}:dom"))

        if page.url != source_url:
            try:
                await page.goto(source_url, wait_until="domcontentloaded")
                await self.settle_page(page)
            except Exception:
                pass
        return found

    async def probe_selector_clicks(self, page: Page, source_url: str, selector: str, method_prefix: str, limit: int) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
        locator = page.locator(selector)
        count = await locator.count()
        for index in range(min(count, limit)):
            current = locator.nth(index)
            try:
                if not await current.is_visible():
                    continue
            except Exception:
                continue
            label = ""
            try:
                label = (await current.inner_text()).strip()
            except Exception:
                pass
            if not label:
                label = f"{selector}[{index}]"
            found.extend(await self.click_probe(page, current, source_url, label, method_prefix))
        return found

    async def run_generic_interactions(self, page: Page, source_url: str, page_kind: str) -> list[tuple[str, str]]:
        if not self.config.enable_generic_interactions or self.config.max_interaction_clicks_per_page <= 0:
            return []

        plans: list[tuple[str, str, int]] = []
        if self.is_ajcass:
            plans.extend(
                [
                    (".issue-list", "selector:.issue-list", 8),
                    (".issueTitle", "selector:.issueTitle", 8),
                    (".title", "selector:.title", 8),
                    (".enTitle", "selector:.enTitle", 8),
                    (".nav > *", "selector:.nav", 8),
                    (".menu > *", "selector:.menu", 10),
                    (".menu-center > *", "selector:.menu-center", 10),
                    (".issue1 > *", "selector:.issue1", 10),
                    (".link-list > *", "selector:.link-list", 8),
                    (".list-title", "selector:.list-title", 4),
                    (".special-topic-title", "selector:.special-topic-title", 4),
                    (".el-carousel__arrow", "selector:.el-carousel__arrow", 4),
                    (".el-carousel__button", "selector:.el-carousel__button", 6),
                ]
            )
        elif self.is_cbpt_portal_url(source_url):
            if page_kind in {"cbpt_portal_index", "cbpt_portal_aux"}:
                plans.extend(
                    [
                        (".paperNav a, .moreBtn, .listZone_more", "selector:cbpt-portal-home", 12),
                        ("a[target='_blank']", "selector:cbpt-portal-blank", 8),
                    ]
                )
            if page_kind in {"cbpt_portal_index", "cbpt_portal_list", "cbpt_portal_news", "cbpt_portal_aux"}:
                plans.extend(
                    [
                        (".pageNum, .nextBtn, .endBtn, .lastBtn, .prevBtn, .moreNum", "selector:cbpt-portal-page", 12),
                        (".paperNav a, .moreBtn, .listZone_more, .nowPast", "selector:cbpt-portal-nav", 10),
                        (".simpM, .compM", "selector:cbpt-portal-view", 4),
                    ]
                )
        else:
            plans.extend(
                [
                    ("[class*='title'], [class*='author'], [class*='article'], [class*='issue'], [class*='card']", "selector:generic-content", 10),
                    (".swiper-slide, [class*='swiper-slide']", "selector:generic-swiper", 8),
                ]
            )

        plans.extend(
            [
                ("a[href='#'], a[href=''], a[href^='javascript']", "selector:a-dynamic", 6),
                ("[onclick]", "selector:[onclick]", 8),
                ("button", "selector:button", 6),
                ("[role='button']", "selector:[role=button]", 6),
                (".btn, .button", "selector:.btn", 6),
                (".tab, .tabs li, .nav-item, .menu-item", "selector:.tab", 8),
                (".pagination a, .pager a, .next, .prev, .more, [class*='more']", "selector:.pagination", 8),
            ]
        )

        remaining = self.config.max_interaction_clicks_per_page
        found: list[tuple[str, str]] = []
        for selector, method_prefix, selector_limit in plans:
            if remaining <= 0:
                break
            current_limit = min(selector_limit, remaining)
            found.extend(await self.probe_selector_clicks(page, source_url, selector, method_prefix, current_limit))
            remaining = max(0, remaining - current_limit)
        return found

    def clean_extracted_url_candidate(self, candidate: str) -> Optional[str]:
        value = candidate.strip()
        if not value:
            return None

        cleaned_chars: list[str] = []
        for ch in value:
            if ch.isspace() or ch in {'"', "'", "<", ">"} or ord(ch) > 127:
                break
            cleaned_chars.append(ch)

        cleaned = "".join(cleaned_chars).rstrip(").,;:!?]}")
        if cleaned.lower().startswith(("javascript:", "mailto:", "tel:", "data:", "blob:")):
            return None
        for entity in ("&quot;", "&#34;", "&#39;", "&apos;", "&gt;", "&lt;"):
            if entity in cleaned:
                cleaned = cleaned.split(entity, 1)[0]
        if any(ch in cleaned for ch in "{}[]|"):
            return None
        if cleaned.startswith("/") and len(cleaned) <= 4 and cleaned.strip("/").isdigit():
            return None
        if cleaned in {"", "/", "//", "?", "./", "../"}:
            return None
        return cleaned

    def extract_urls_from_string(self, value: str, *, allow_relative: bool) -> list[str]:
        candidate = value.strip()
        if not candidate:
            return []
        lowered_candidate = candidate.lower()
        if lowered_candidate.startswith(("javascript:", "mailto:", "tel:", "data:", "blob:")):
            return []

        results: list[str] = []
        for match in URL_REGEX.findall(candidate):
            cleaned = self.clean_extracted_url_candidate(match)
            if cleaned:
                results.append(cleaned)
        if allow_relative:
            for match in RELATIVE_URL_REGEX.finditer(candidate):
                cleaned = self.clean_extracted_url_candidate(match.group("url"))
                if cleaned:
                    results.append(cleaned)
            for match in HASH_ROUTE_REGEX.findall(candidate):
                cleaned = self.clean_extracted_url_candidate(match)
                if cleaned:
                    results.append(cleaned)
            if candidate.startswith(("//", "/", "./", "../", "?", "#/")):
                results.append(candidate)

        if self.is_ajcass:
            for match in AJCASS_ROUTE_REGEX.finditer(candidate):
                results.append(match.group("route"))
            if candidate.startswith(("/index", "/detail", "#/", "/search", "/issue", "/issueDetail", "/enIndex", "/enIssue")):
                results.append(candidate)
        return results

    def iter_string_urls(self, value: Any) -> list[str]:
        results: list[str] = []
        if isinstance(value, dict):
            for nested in value.values():
                results.extend(self.iter_string_urls(nested))
        elif isinstance(value, list):
            for nested in value:
                results.extend(self.iter_string_urls(nested))
        elif isinstance(value, str):
            results.extend(self.extract_urls_from_string(value, allow_relative=True))
        return results

    def parse_ajcass_issue_items(
        self,
        items: Optional[List[Dict[str, Any]]],
        method: str,
        *,
        english: bool = False,
    ) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        if not items:
            return found
        for item in items:
            content_id = item.get("contentId") or item.get("id")
            year = item.get("year")
            issue_no = item.get("issue")
            title = str(item.get("title") or item.get("enTitle") or "").strip()
            issue_url = self.build_ajcass_issue_url(
                content_id=content_id,
                year=year,
                issue=issue_no,
                title=title,
                english=english,
            )
            if issue_url:
                if english:
                    normalized = self.normalize_url(issue_url)
                    if normalized:
                        self.expected_en_issue_urls.add(normalized)
                else:
                    normalized = self.normalize_url(issue_url)
                    if normalized:
                        self.expected_issue_detail_urls.add(normalized)
                found.append((issue_url, method))

            for key in ("filePath", "filePath2", "otherPath"):
                value = str(item.get(key) or "").strip()
                if value:
                    found.append((value, f"{method}:{key}"))
        return found

    async def parse_paginated_site_content(self, source_url: str, depth: int, payload: Dict[str, Any], total_pages: int, api_context: Any) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        current = int(payload.get("curr", 1))
        page_numbers = list(range(current + 1, total_pages + 1))
        if self.config.max_api_pages_per_series:
            page_numbers = page_numbers[: self.config.max_api_pages_per_series]
        for page_num in page_numbers:
            next_payload = dict(payload)
            next_payload["curr"] = page_num
            fetch_key = f"{AJCASS_SITE_CONTENT_API}|{json.dumps(next_payload, ensure_ascii=False, sort_keys=True)}"
            if fetch_key in self.fetched_api_pages:
                continue
            self.fetched_api_pages.add(fetch_key)
            response = await api_context.post(AJCASS_SITE_CONTENT_API, data=next_payload)
            data = await response.json()
            found.extend(await self.parse_site_content_response(data, next_payload, source_url, depth, api_context, allow_pagination=False))
        return found

    async def parse_paginated_issue_search(self, source_url: str, depth: int, payload: Dict[str, Any], total_pages: int, api_context: Any) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        current = int(payload.get("curr", 1))
        page_numbers = list(range(current + 1, total_pages + 1))
        if self.config.max_api_pages_per_series:
            page_numbers = page_numbers[: self.config.max_api_pages_per_series]
        for page_num in page_numbers:
            next_payload = dict(payload)
            next_payload["curr"] = page_num
            fetch_key = f"{AJCASS_ISSUE_SEARCH_API}|{json.dumps(next_payload, ensure_ascii=False, sort_keys=True)}"
            if fetch_key in self.fetched_api_pages:
                continue
            self.fetched_api_pages.add(fetch_key)
            response = await api_context.post(AJCASS_ISSUE_SEARCH_API, data=next_payload)
            data = await response.json()
            found.extend(await self.parse_issue_search_response(data, next_payload, source_url, depth, api_context, allow_pagination=False))
        return found

    async def parse_paginated_issue_simple_search(self, source_url: str, depth: int, payload: Dict[str, Any], total_pages: int, api_context: Any) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        current = int(payload.get("curr", 1))
        page_numbers = list(range(current + 1, total_pages + 1))
        if self.config.max_api_pages_per_series:
            page_numbers = page_numbers[: self.config.max_api_pages_per_series]
        for page_num in page_numbers:
            next_payload = dict(payload)
            next_payload["curr"] = page_num
            fetch_key = f"{AJCASS_ISSUE_SIMPLE_API}|{json.dumps(next_payload, ensure_ascii=False, sort_keys=True)}"
            if fetch_key in self.fetched_api_pages:
                continue
            self.fetched_api_pages.add(fetch_key)
            response = await api_context.post(AJCASS_ISSUE_SIMPLE_API, data=next_payload)
            data = await response.json()
            found.extend(await self.parse_issue_simple_response(data, next_payload, source_url, depth, api_context, allow_pagination=False))
        return found

    async def parse_site_content_response(
        self,
        data: Dict[str, Any],
        payload: Dict[str, Any],
        source_url: str,
        depth: int,
        api_context: Any,
        allow_pagination: bool = True,
    ) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        channel_id = str(payload.get("channeID", ""))
        for item in data.get("data") or []:
            link_url = str(item.get("linkUrl") or "").strip()
            content_id = item.get("contentID")
            title_photo = str(item.get("titlePhoto") or "").strip()
            if title_photo:
                found.append((title_photo, "api:GetSiteContentPageList:titlePhoto"))

            if re.fullmatch(r"\d+", link_url) and self.has_ajcass_route("/detail"):
                detail_url = f"{self.site_origin}/#/detail?channelId={link_url}"
                normalized = self.normalize_url(detail_url)
                if normalized:
                    self.expected_static_detail_urls.add(normalized)
                found.append((detail_url, "api:GetSiteContentPageList:channel"))
                continue

            if link_url:
                if self.is_ajcass and link_url.startswith(("/index", "/detail", "/search", "/issue", "/issueDetail", "/enIndex", "/enIssue")):
                    link_url = f"{self.site_origin}/#{link_url}"
                found.append((link_url, "api:GetSiteContentPageList:linkUrl"))
                continue

            if content_id is not None and channel_id and self.has_ajcass_route("/detail"):
                detail_url = f"{self.site_origin}/#/detail?channelId={channel_id}&id={content_id}"
                normalized = self.normalize_url(detail_url)
                if normalized:
                    self.expected_static_detail_urls.add(normalized)
                found.append((detail_url, "api:GetSiteContentPageList:detail"))

        total_pages = int(data.get("totalpage") or 1)
        if allow_pagination and total_pages > int(payload.get("curr", 1)):
            found.extend(await self.parse_paginated_site_content(source_url, depth, payload, total_pages, api_context))
        return found

    async def parse_issue_search_response(
        self,
        data: Dict[str, Any],
        payload: Dict[str, Any],
        source_url: str,
        depth: int,
        api_context: Any,
        allow_pagination: bool = True,
    ) -> List[Tuple[str, str]]:
        found = self.parse_ajcass_issue_items(data.get("data") or [], "api:GetIssueNormalSearch:issueDetail")
        total_pages = int(data.get("totalpage") or 1)
        if allow_pagination and total_pages > int(payload.get("curr", 1)):
            found.extend(await self.parse_paginated_issue_search(source_url, depth, payload, total_pages, api_context))
        return found

    async def parse_issue_simple_response(
        self,
        data: Dict[str, Any],
        payload: Dict[str, Any],
        source_url: str,
        depth: int,
        api_context: Any,
        allow_pagination: bool = True,
    ) -> List[Tuple[str, str]]:
        found = self.parse_ajcass_issue_items(
            data.get("data") or [],
            "api:GetIssueSimpleSearch:enIssue",
            english=True,
        )
        total_pages = int(data.get("totalpage") or 1)
        if allow_pagination and total_pages > int(payload.get("curr", 1)):
            found.extend(await self.parse_paginated_issue_simple_search(source_url, depth, payload, total_pages, api_context))
        return found

    def parse_current_issue_tree(self, data: Dict[str, Any]) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []

        def walk_channels(channels: Optional[List[Dict[str, Any]]]) -> None:
            if not channels:
                return
            for channel in channels:
                found.extend(
                    self.parse_ajcass_issue_items(
                        channel.get("issueInfoList") or [],
                        "api:GetCurrentPeriodMutiChannel:issueDetail",
                    )
                )
                walk_channels(channel.get("channels"))

        issue_data = data.get("data") or {}
        title_photo = str(issue_data.get("titlePhoto") or "").strip()
        if title_photo:
            found.append((title_photo, "api:GetCurrentPeriodMutiChannel:titlePhoto"))
        year = issue_data.get("year")
        issue_no = issue_data.get("issue")
        if year and issue_no and self.has_ajcass_route("/search"):
            search_url = f"{self.site_origin}/#/search?issue={issue_no}&year={year}"
            normalized = self.normalize_url(search_url)
            if normalized:
                self.expected_issue_search_urls.add(normalized)
            found.append((search_url, "api:GetCurrentPeriodMutiChannel:currentIssue"))
        found.extend(
            self.parse_ajcass_issue_items(
                issue_data.get("issueInfoList") or [],
                "api:GetCurrentPeriod:issueDetail",
            )
        )
        walk_channels(issue_data.get("channels"))
        return found

    def parse_year_volume_tree(self, data: Dict[str, Any]) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        if not self.has_ajcass_route("/search"):
            return found
        for year_item in data.get("data") or []:
            year = year_item.get("year")
            for issue_item in year_item.get("issueLists") or []:
                issue_no = issue_item.get("issue")
                if year and issue_no:
                    search_url = f"{self.site_origin}/#/search?issue={issue_no}&year={year}"
                    normalized = self.normalize_url(search_url)
                    if normalized:
                        self.expected_issue_search_urls.add(normalized)
                    found.append((search_url, "api:GetYearVolumeTree:search"))
        return found

    def parse_content_info(self, data: Dict[str, Any]) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        payload = data.get("data") or {}
        for key in ("siteContentInfoResult", "issueContentInfoResult", "bmpVideoCourseResult"):
            item = payload.get(key)
            if not item:
                continue
            for field in ("filePath", "filePath2", "otherPath", "titlePhoto", "linkUrl"):
                value = str(item.get(field) or "").strip()
                if value:
                    found.append((value, f"api:GetContentInfo:{field}"))
            found.extend((url, "api:GetContentInfo:inline") for url in self.iter_string_urls(item))
        return found

    def is_boyuan_api_url(self, url: str) -> bool:
        lowered = (url or "").lower()
        return f"https://{BOYUAN_API_HOST}/api/" in lowered or f"http://{BOYUAN_API_HOST}/api/" in lowered

    def build_boyuan_browse_url(self, year: Any = None, issue: Any = None) -> str:
        params: List[Tuple[str, Any]] = []
        if year not in (None, ""):
            params.append(("year", year))
        if issue not in (None, ""):
            params.append(("issue", issue))
        base = f"{self.site_origin}/#/browse"
        if not params:
            return base
        return f"{base}?{urlencode(params, doseq=True)}"

    def build_boyuan_browse_detail_url(self, *, item_id: Any, year: Any = None, issue: Any = None) -> Optional[str]:
        if item_id in (None, ""):
            return None
        params: List[Tuple[str, Any]] = [("issuecid", item_id)]
        if year not in (None, ""):
            params.insert(0, ("year", year))
        if issue not in (None, ""):
            params.insert(1 if year not in (None, "") else 0, ("issue", issue))
        return f"{self.site_origin}/#/browse_details?{urlencode(params, doseq=True)}"

    def parse_boyuan_article_items(
        self,
        items: Optional[List[Dict[str, Any]]],
        method: str,
        *,
        year: Any = None,
        issue: Any = None,
    ) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        if not items:
            return found
        for item in items:
            item_year = item.get("year", year)
            item_issue = item.get("issue", issue)
            detail_url = self.build_boyuan_browse_detail_url(
                item_id=item.get("id") or item.get("issuecid") or item.get("issueCid"),
                year=item_year,
                issue=item_issue,
            )
            if detail_url:
                found.append((detail_url, method))
            for field in ("filePath", "pdfPath", "htmlPath", "otherPath", "titlePhoto"):
                value = str(item.get(field) or "").strip()
                if value:
                    found.append((value, f"{method}:{field}"))
        return found

    async def parse_boyuan_journal_year_response(
        self,
        data: Dict[str, Any],
        journal_id: Any,
        api_context: Any,
        gap_year: Any,
    ) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        if journal_id in (None, ""):
            return found
        for item in data.get("data") or []:
            year = item.get("year")
            if year in (None, ""):
                continue
            found.append((self.build_boyuan_browse_url(year=year), "api:GetJournalYear:browse"))
            fetch_url = f"{BOYUAN_SITE_WEB_API_PREFIX}GetThatYearIssueList?journalId={journal_id}&year={year}"
            fetch_key = f"boyuan:GetThatYearIssueList|{fetch_url}"
            if fetch_key in self.fetched_api_pages:
                continue
            self.fetched_api_pages.add(fetch_key)
            try:
                response = await api_context.get(fetch_url)
                payload = await response.json()
            except Exception as exc:
                self.logger.warning("Failed to expand Boyuan year issues journal_id=%s year=%s error=%s", journal_id, year, exc)
                continue
            found.extend(await self.parse_boyuan_issue_list_response(payload, year=year, api_context=api_context))
        return found

    async def parse_boyuan_gap_year_response(
        self,
        data: Dict[str, Any],
        request_params: Dict[str, Any],
        api_context: Any,
    ) -> List[Tuple[str, str]]:
        journal_id = request_params.get("journalId")
        if journal_id in (None, ""):
            return []
        gap_size = request_params.get("gapYear", 10)
        found: List[Tuple[str, str]] = []
        seen_groups: Set[Any] = set()
        for item in data.get("data") or []:
            group_year = item.get("year")
            if group_year in (None, "") or group_year in seen_groups:
                continue
            seen_groups.add(group_year)
            fetch_url = f"{BOYUAN_SITE_WEB_API_PREFIX}GetJournalYear?journalId={journal_id}&year={group_year}&gapYear={gap_size}"
            fetch_key = f"boyuan:GetJournalYear|{fetch_url}"
            if fetch_key in self.fetched_api_pages:
                continue
            self.fetched_api_pages.add(fetch_key)
            try:
                response = await api_context.get(fetch_url)
                payload = await response.json()
            except Exception as exc:
                self.logger.warning("Failed to expand Boyuan gap year journal_id=%s group_year=%s error=%s", journal_id, group_year, exc)
                continue
            found.extend(await self.parse_boyuan_journal_year_response(payload, journal_id=journal_id, api_context=api_context, gap_year=gap_size))
        return found

    async def parse_boyuan_issue_list_response(
        self,
        data: Dict[str, Any],
        *,
        year: Any,
        api_context: Any,
    ) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        issues = data.get("data") or []
        for item in issues:
            issue = item.get("issue")
            if issue in (None, ""):
                continue
            found.append((self.build_boyuan_browse_url(year=year, issue=issue), "api:GetThatYearIssueList:browse"))
            title_photo = str(item.get("titlePhoto") or "").strip()
            if title_photo:
                found.append((title_photo, "api:GetThatYearIssueList:titlePhoto"))
        return found

    async def parse_paginated_boyuan_back_issue(
        self,
        source_url: str,
        payload: Dict[str, Any],
        total_pages: int,
        api_context: Any,
    ) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []
        current = int(payload.get("curr", 1))
        page_numbers = list(range(current + 1, total_pages + 1))
        if self.config.max_api_pages_per_series:
            page_numbers = page_numbers[: self.config.max_api_pages_per_series]
        for page_num in page_numbers:
            next_payload = dict(payload)
            next_payload["curr"] = page_num
            fetch_key = f"boyuan:GetBackIssueBrowsing|{json.dumps(next_payload, ensure_ascii=False, sort_keys=True)}"
            if fetch_key in self.fetched_api_pages:
                continue
            self.fetched_api_pages.add(fetch_key)
            try:
                response = await api_context.post(f"{BOYUAN_SITE_WEB_API_PREFIX}GetBackIssueBrowsing", data=next_payload)
                data = await response.json()
            except Exception as exc:
                self.logger.warning("Failed to expand Boyuan back issue source=%s page=%s error=%s", source_url, page_num, exc)
                continue
            found.extend(
                await self.parse_boyuan_back_issue_response(
                    data,
                    payload=next_payload,
                    source_url=source_url,
                    api_context=api_context,
                    allow_pagination=False,
                )
            )
        return found

    async def parse_boyuan_back_issue_response(
        self,
        data: Dict[str, Any],
        *,
        payload: Dict[str, Any],
        source_url: str,
        api_context: Any,
        allow_pagination: bool = True,
    ) -> List[Tuple[str, str]]:
        year = payload.get("year")
        issue = payload.get("issue")
        found = self.parse_boyuan_article_items(data.get("data") or [], "api:GetBackIssueBrowsing:detail", year=year, issue=issue)
        total_pages = int(data.get("totalpage") or 1)
        if allow_pagination and total_pages > int(payload.get("curr", 1)):
            found.extend(await self.parse_paginated_boyuan_back_issue(source_url, payload, total_pages, api_context))
        return found

    async def parse_boyuan_json_response(
        self,
        data: Dict[str, Any],
        response: Response,
        source_url: str,
        api_context: Any,
    ) -> List[Tuple[str, str]]:
        url = response.url
        request = response.request
        params = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
        if "GetJournalGapYear" in url:
            return await self.parse_boyuan_gap_year_response(data, params, api_context)
        if "GetJournalYear" in url:
            return await self.parse_boyuan_journal_year_response(
                data,
                journal_id=params.get("journalId"),
                api_context=api_context,
                gap_year=params.get("gapYear"),
            )
        if "GetThatYearIssueList" in url:
            return await self.parse_boyuan_issue_list_response(data, year=params.get("year"), api_context=api_context)
        if "GetJournalIssueList" in url:
            return [
                (self.build_boyuan_browse_url(year=params.get("year"), issue=item.get("issue")), "api:GetJournalIssueList:browse")
                for item in (data.get("data") or [])
                if item.get("issue") not in (None, "")
            ]
        if "GetBackIssueBrowsing" in url:
            try:
                payload = request.post_data_json or {}
            except Exception:
                payload = {}
            return await self.parse_boyuan_back_issue_response(data, payload=payload, source_url=source_url, api_context=api_context)
        if "GetJournalArticleList" in url:
            try:
                payload = request.post_data_json or {}
            except Exception:
                payload = {}
            return self.parse_boyuan_article_items(
                data.get("data") or [],
                "api:GetJournalArticleList:detail",
                year=payload.get("year"),
                issue=payload.get("issue"),
            )
        return []

    async def parse_script_response(self, response: Response, source_url: str) -> List[Tuple[str, str]]:
        if response.url in self.processed_script_requests:
            return []
        if not self.is_ajcass and not self.should_parse_script_response_url(response.url):
            return []
        self.processed_script_requests.add(response.url)
        try:
            text = await response.text()
        except Exception as exc:
            self.logger.warning("Failed to read script response url=%s error=%s", response.url, exc)
            return []

        found: List[Tuple[str, str]] = []
        if self.is_ajcass:
            for route in AJCASS_SCRIPT_ROUTE_CANDIDATES:
                if f'"{route}"' in text or f"'{route}'" in text:
                    self.ajcass_known_routes.add(route)
                    found.append((f"{self.site_origin}/#{route}", "response:script:route"))
        else:
            found.extend(self.extract_generic_spa_routes_from_script(text, source_url))
        return found

    async def parse_json_response(self, response: Response, source_url: str, depth: int, page_kind: str, api_context: Any) -> List[Tuple[str, str]]:
        url = response.url
        request = response.request
        api_key = f"{url}|{request.method}|{request.post_data or ''}"
        if api_key in self.processed_api_requests:
            return []
        self.processed_api_requests.add(api_key)

        try:
            data = await response.json()
        except Exception as exc:
            self.logger.warning("Failed to parse JSON response url=%s error=%s", url, exc)
            return []

        found: List[Tuple[str, str]] = []
        if self.is_ajcass:
            if "GetYearVolumeTree" in url:
                found.extend(self.parse_year_volume_tree(data))
            elif "GetCurrentPeriodMutiChannel" in url or "GetCurrentPeriod?" in url:
                found.extend(self.parse_current_issue_tree(data))
            elif "GetThatYearIssueList" in url:
                if self.has_ajcass_route("/search"):
                    for item in data.get("data") or []:
                        year = item.get("year")
                        issue_no = item.get("issue")
                        if year and issue_no:
                            search_url = f"{self.site_origin}/#/search?issue={issue_no}&year={year}"
                            normalized = self.normalize_url(search_url)
                            if normalized:
                                self.expected_issue_search_urls.add(normalized)
                            found.append((search_url, "api:GetThatYearIssueList:search"))
            elif "GetSiteContentPageList" in url:
                payload = {}
                try:
                    payload = request.post_data_json or {}
                except Exception:
                    payload = {}
                found.extend(await self.parse_site_content_response(data, payload, source_url, depth, api_context))
            elif "GetIssueNormalSearch" in url:
                payload = {}
                try:
                    payload = request.post_data_json or {}
                except Exception:
                    payload = {}
                found.extend(await self.parse_issue_search_response(data, payload, source_url, depth, api_context))
            elif "GetIssueSimpleSearch" in url:
                payload = {}
                try:
                    payload = request.post_data_json or {}
                except Exception:
                    payload = {}
                found.extend(await self.parse_issue_simple_response(data, payload, source_url, depth, api_context))
            elif "GetIssueinfoList" in url:
                found.extend(self.parse_ajcass_issue_items(data.get("data") or [], "api:GetIssueinfoList:issueDetail"))
            elif "GetContentInfo" in url:
                found.extend(self.parse_content_info(data))
        elif self.is_boyuan_api_url(url):
            found.extend(await self.parse_boyuan_json_response(data, response, source_url, api_context))
        found.extend((url_candidate, "response:json") for url_candidate in self.iter_string_urls(data))
        return found

    async def parse_response(self, response: Response, source_url: str, depth: int, page_kind: str, api_context: Any) -> List[Tuple[str, str]]:
        content_type = response.headers.get("content-type", "").lower()
        if "application/json" in content_type or "+json" in content_type:
            return await self.parse_json_response(response, source_url, depth, page_kind, api_context)
        if "javascript" in content_type or response.url.lower().endswith(".js"):
            return await self.parse_script_response(response, source_url)
        return []

    async def process_page(self, item: QueueItem) -> None:
        session = self.get_next_session()
        page = await session.context.new_page()
        if self.config.enable_request_blocking:
            await page.route("**/*", self.handle_route)
        response_tasks: list[tuple[str, asyncio.Task[list[tuple[str, str]]]]] = []
        discoveries: list[tuple[str, str]] = []
        visit = PageVisit(
            requested_url=item.url,
            final_url=item.url,
            depth=item.depth,
            page_kind=self.page_kind(item.url),
            proxy=session.proxy_label,
            title="",
            ok=False,
            started_at=time.time(),
        )
        self.logger.info(
            "Visit start depth=%s kind=%s proxy=%s from=%s method=%s url=%s",
            item.depth,
            visit.page_kind,
            visit.proxy,
            item.discovered_from,
            item.discovery_method,
            item.url,
        )

        def on_response(response: Response) -> None:
            interesting = (
                response.request.resource_type in {"document", "xhr", "fetch"}
                or "application/json" in response.headers.get("content-type", "").lower()
                or any(response.url.lower().endswith(suffix) for suffix in NON_HTML_SUFFIXES)
                or (response.request.resource_type == "script" and self.should_parse_script_response_url(response.url))
            )
            if not interesting:
                return
            discoveries.append((response.url, f"response:{response.request.resource_type}"))
            response_tasks.append(
                (
                    response.url,
                    asyncio.create_task(
                        self.parse_response(
                            response=response,
                            source_url=item.url,
                            depth=item.depth + 1,
                            page_kind=self.page_kind(item.url),
                            api_context=session.api_context,
                        )
                    ),
                )
            )

        page.on("response", on_response)
        try:
            await page.goto(item.url, wait_until="domcontentloaded")
            await self.settle_page(page)

            visit.final_url = page.url
            visit.page_kind = self.page_kind(page.url)
            try:
                visit.title = await page.title()
            except Exception as exc:
                visit.final_url = page.url
                visit.page_kind = self.page_kind(page.url)
                visit.title = ""
                self.logger.debug(
                    "Failed to read page title depth=%s requested=%s final=%s error=%s",
                    item.depth,
                    item.url,
                    visit.final_url,
                    exc,
                )
            self.remember_ajcass_route(page.url)

            for dom_url in await self.extract_dom_urls(page):
                discoveries.append((dom_url, "dom"))

            if self.is_ajcass:
                click_targets: list[str] = []
                if visit.page_kind == "root":
                    click_targets = [
                        "English",
                        "\u4f5c\u8005\u6295\u7a3f",
                        "\u4f5c\u8005\u67e5\u7a3f",
                        "\u4e13\u5bb6\u5ba1\u7a3f",
                        "\u7f16\u8f91\u529e\u516c",
                    ]
                elif visit.page_kind == "english_index":
                    click_targets = ["JSTOR", "About Us", "Contact Us", "Submission & Review"]

                if click_targets:
                    discoveries.extend(await self.probe_click_texts(page, page.url, item.depth + 1, click_targets))
                if visit.page_kind == "english_index":
                    discoveries.extend(
                        await self.probe_selector_clicks(
                            page=page,
                            source_url=page.url,
                            selector=".enTitle",
                            method_prefix="click:.enTitle",
                            limit=12,
                        )
                    )
            elif self.is_cbpt_portal_url(page.url):
                discoveries.extend(await self.discover_cbpt_portal_ajax_urls(page, page.url, visit.page_kind))
            discoveries.extend(await self.run_generic_interactions(page, page.url, visit.page_kind))

            if response_tasks:
                response_results = await asyncio.gather(
                    *(task for _, task in response_tasks),
                    return_exceptions=True,
                )
                for (response_url, _), result in zip(response_tasks, response_results):
                    if isinstance(result, list):
                        discoveries.extend(result)
                    elif isinstance(result, Exception):
                        self.logger.warning(
                            "Response parse task failed response_url=%s error=%s",
                            response_url,
                            result,
                            exc_info=(type(result), result, result.__traceback__),
                        )

            for raw_url, method in discoveries:
                self.enqueue_url(raw_url, item.depth + 1, page.url, method)

            visit.ok = True
            visit.discoveries = len(discoveries)
        except Exception:
            visit.error = traceback.format_exc()
            self.logger.exception(
                "Visit failed depth=%s url=%s error=%s",
                item.depth,
                item.url,
                truncate_text(visit.error, 500),
            )
        finally:
            visit.finished_at = time.time()
            self.visits.append(visit)
            self.visited_urls.add(item.url)
            if visit.ok:
                self.logger.info(
                    "Visit ok depth=%s final_kind=%s proxy=%s discoveries=%s duration_ms=%s requested=%s final=%s title=%s",
                    item.depth,
                    visit.page_kind,
                    visit.proxy,
                    visit.discoveries,
                    visit.duration_ms,
                    item.url,
                    visit.final_url,
                    truncate_text(visit.title),
                )
            try:
                await page.close()
            except Exception:
                self.logger.debug("Failed to close page requested=%s final=%s", item.url, visit.final_url, exc_info=True)

    async def crawl(self) -> dict[str, Any]:
        processed_pages = 0
        hit_page_limit = False
        active_tasks: Dict[asyncio.Task[None], QueueItem] = {}
        self.logger.info(
            "Crawl start site=%s frontier=%s visited=%s discovered=%s page_limit=%s concurrency=%s",
            self.config.site_key,
            self.frontier_count(),
            len(self.visited_urls),
            len(self.discovered_urls),
            self.config.page_limit,
            self.config.max_concurrency,
        )
        while self.frontier or active_tasks:
            while self.frontier and len(active_tasks) < self.config.max_concurrency:
                if self.config.page_limit and processed_pages >= self.config.page_limit:
                    if not hit_page_limit:
                        hit_page_limit = True
                        self.logger.info(
                            "Hit page limit site=%s page_limit=%s processed_pages=%s frontier_remaining=%s active=%s",
                            self.config.site_key,
                            self.config.page_limit,
                            processed_pages,
                            len(self.frontier),
                            len(active_tasks),
                        )
                    break
                item = self.frontier.popleft()
                self.active_queue_items[item.url] = item
                active_tasks[asyncio.create_task(self.process_page(item))] = item
                processed_pages += 1
                self.logger.debug(
                    "Dispatched URL site=%s processed=%s active=%s frontier=%s url=%s",
                    self.config.site_key,
                    processed_pages,
                    len(active_tasks),
                    len(self.frontier),
                    item.url,
                )

            if not active_tasks:
                break

            visits_before = len(self.visits)
            done, _ = await asyncio.wait(active_tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                item = active_tasks.pop(task)
                self.active_queue_items.pop(item.url, None)
                try:
                    await task
                except Exception:
                    self.logger.exception("Crawler worker crashed site=%s url=%s", self.config.site_key, item.url)
            completed_count = len(done)
            self.pages_since_checkpoint += completed_count
            new_visits = self.visits[visits_before:]
            completed_ok = sum(1 for visit in new_visits if visit.ok)
            completed_failed = len(new_visits) - completed_ok
            self.logger.info(
                "Worker tick site=%s finished=%s ok=%s failed=%s active=%s frontier=%s visited=%s discovered=%s",
                self.config.site_key,
                completed_count,
                completed_ok,
                completed_failed,
                len(active_tasks),
                len(self.frontier),
                len(self.visited_urls),
                len(self.discovered_urls),
            )
            self.save_checkpoint()

        self.completed = not self.frontier and not self.active_queue_items and not hit_page_limit
        summary = self.build_summary()
        self.write_outputs(summary, include_detail_files=True)
        self.save_checkpoint(force=True, completed=self.completed, include_detail_files=False)
        self.logger.info(
            "Crawl finished site=%s completed=%s discovered=%s queueable=%s visited=%s failed=%s summary=%s",
            self.config.site_key,
            self.completed,
            summary["counts"]["discovered_urls"],
            summary["counts"]["queueable_urls"],
            summary["counts"]["visited_pages"],
            summary["counts"]["visit_failed"],
            self.summary_path,
        )
        return summary

    def build_summary(self) -> dict[str, Any]:
        def sample(items: list[str], limit: int = 20) -> list[str]:
            return items[:limit]

        page_kind_counter = Counter(
            node["page_kind"] for node in self.discovered_urls.values() if node["queueable"]
        )
        visit_ok = sum(1 for visit in self.visits if visit.ok)
        visit_failed = len(self.visits) - visit_ok
        queueable_discovered = {url for url, node in self.discovered_urls.items() if node["queueable"]}
        should_visit_urls = {url for url in queueable_discovered if self.should_visit_url(url)}
        skipped_leaf_urls = sorted(queueable_discovered - should_visit_urls)
        unvisited_queueable = sorted(should_visit_urls - self.visited_urls)

        missing_issue_search = sorted(self.expected_issue_search_urls - queueable_discovered)
        missing_issue_detail = sorted(self.expected_issue_detail_urls - queueable_discovered)
        missing_static_detail = sorted(self.expected_static_detail_urls - queueable_discovered)
        missing_en_issue = sorted(self.expected_en_issue_urls - queueable_discovered)
        proxy_session_count = len(self.sessions) if self.sessions else len(self.build_session_proxies())

        return {
            "site_key": self.config.site_key,
            "site_host": self.site_host,
            "site_origin": self.site_origin,
            "site_family": self.site_family,
            "crawl_policy_version": CRAWL_POLICY_VERSION,
            "visit_leaf_pages": self.config.visit_leaf_pages,
            "seed_urls": self.config.seed_urls,
            "completed": self.completed,
            "generated_at": int(time.time()),
            "counts": {
                "discovered_urls": len(self.discovered_urls),
                "queueable_urls": len(queueable_discovered),
                "same_site_urls": sum(1 for node in self.discovered_urls.values() if node["same_site"]),
                "external_or_non_queueable_urls": sum(1 for node in self.discovered_urls.values() if not node["queueable"]),
                "edges": len(self.edges),
                "visited_pages": len(self.visits),
                "visit_ok": visit_ok,
                "visit_failed": visit_failed,
            },
            "page_kinds": dict(page_kind_counter),
            "site_features": {
                "ajcass_known_routes": sorted(self.ajcass_known_routes),
                "ajcass_issue_route": self.ajcass_issue_route if self.is_ajcass else "",
                "proxy_servers_count": len(self.config.proxy_servers),
                "proxy_session_count": proxy_session_count,
                "skip_failed_proxies": self.config.skip_failed_proxies,
                "max_concurrency": self.config.max_concurrency,
                "write_full_outputs_on_checkpoint": self.config.write_full_outputs_on_checkpoint,
                "enable_cbpt_portal_ajax_expansion": self.config.enable_cbpt_portal_ajax_expansion,
                "max_cbpt_portal_ajax_requests_per_page": self.config.max_cbpt_portal_ajax_requests_per_page,
            },
            "verification": {
                "expected_issue_search_urls": len(self.expected_issue_search_urls),
                "missing_issue_search_urls_count": len(missing_issue_search),
                "missing_issue_search_urls_sample": sample(missing_issue_search),
                "expected_issue_detail_urls": len(self.expected_issue_detail_urls),
                "missing_issue_detail_urls_count": len(missing_issue_detail),
                "missing_issue_detail_urls_sample": sample(missing_issue_detail),
                "expected_static_detail_urls": len(self.expected_static_detail_urls),
                "missing_static_detail_urls_count": len(missing_static_detail),
                "missing_static_detail_urls_sample": sample(missing_static_detail),
                "expected_en_issue_urls": len(self.expected_en_issue_urls),
                "missing_en_issue_urls_count": len(missing_en_issue),
                "missing_en_issue_urls_sample": sample(missing_en_issue),
                "unvisited_queueable_urls_count": len(unvisited_queueable),
                "unvisited_queueable_urls_sample": sample(unvisited_queueable),
                "intentionally_skipped_leaf_urls_count": len(skipped_leaf_urls),
                "intentionally_skipped_leaf_urls_sample": sample(skipped_leaf_urls),
                "frontier_queue_count": len(self.frontier),
                "active_pages_count": len(self.active_queue_items),
                "remaining_frontier_count": self.frontier_count(),
            },
        }

    def write_outputs(self, summary: dict[str, Any], include_detail_files: bool = True) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

        atomic_write_text(self.summary_path, json.dumps(summary, ensure_ascii=False, indent=2))
        atomic_write_text(self.seed_urls_path, "\n".join(self.config.seed_urls) + ("\n" if self.config.seed_urls else ""))

        if not include_detail_files:
            return

        node_lines = [
            json.dumps(self.discovered_urls[url], ensure_ascii=False)
            for url in sorted(self.discovered_urls)
        ]
        atomic_write_text(self.nodes_path, "\n".join(node_lines) + ("\n" if node_lines else ""))
        atomic_write_csv(
            self.nodes_csv_path,
            [self.discovered_urls[url] for url in sorted(self.discovered_urls)],
            ["url", "same_site", "queueable", "first_depth", "first_source", "first_method", "seen_count", "page_kind"],
        )

        edge_lines = [json.dumps(asdict(edge), ensure_ascii=False) for edge in self.edges]
        atomic_write_text(self.edges_path, "\n".join(edge_lines) + ("\n" if edge_lines else ""))
        atomic_write_csv(
            self.edges_csv_path,
            [asdict(edge) for edge in self.edges],
            ["source_url", "target_url", "depth", "method", "same_site", "queueable", "note"],
        )

        visit_lines: list[str] = []
        visit_rows: list[dict[str, Any]] = []
        for visit in self.visits:
            record = asdict(visit)
            record["duration_ms"] = visit.duration_ms
            visit_lines.append(json.dumps(record, ensure_ascii=False))
            visit_rows.append(record)
        atomic_write_text(self.visits_path, "\n".join(visit_lines) + ("\n" if visit_lines else ""))
        atomic_write_csv(
            self.visits_csv_path,
            visit_rows,
            [
                "requested_url",
                "final_url",
                "depth",
                "page_kind",
                "proxy",
                "title",
                "ok",
                "error",
                "discoveries",
                "started_at",
                "finished_at",
                "duration_ms",
            ],
        )

        all_urls = sorted(self.discovered_urls)
        same_site_urls = sorted(url for url, node in self.discovered_urls.items() if node["same_site"])
        external_urls = sorted(url for url, node in self.discovered_urls.items() if not node["queueable"])

        atomic_write_text(self.all_urls_path, "\n".join(all_urls) + ("\n" if all_urls else ""))
        atomic_write_text(self.same_site_urls_path, "\n".join(same_site_urls) + ("\n" if same_site_urls else ""))
        atomic_write_text(self.external_urls_path, "\n".join(external_urls) + ("\n" if external_urls else ""))

    def save_checkpoint(
        self,
        force: bool = False,
        completed: Optional[bool] = None,
        include_detail_files: Optional[bool] = None,
    ) -> None:
        if completed is not None:
            self.completed = completed
        now = time.time()
        if not force:
            if self.pages_since_checkpoint < self.config.checkpoint_every_pages and (now - self.last_checkpoint_at) < self.config.checkpoint_every_seconds:
                return

        summary = self.build_summary()
        if include_detail_files is None:
            include_detail_files = self.config.write_full_outputs_on_checkpoint
        self.write_outputs(summary, include_detail_files=include_detail_files)
        payload = {
            "site_key": self.config.site_key,
            "site_host": self.site_host,
            "site_origin": self.site_origin,
            "crawl_policy_version": CRAWL_POLICY_VERSION,
            "visit_leaf_pages": self.config.visit_leaf_pages,
            "seed_urls": self.config.seed_urls,
            "completed": self.completed,
            "frontier": [asdict(item) for item in self.checkpoint_frontier_items()],
            "discovered_urls": [self.discovered_urls[url] for url in sorted(self.discovered_urls)],
            "visited_urls": sorted(self.visited_urls),
            "queued_urls": sorted(self.queued_urls),
            "edges": [asdict(edge) for edge in self.edges],
            "visits": [asdict(visit) for visit in self.visits],
            "discovered_via_source": [list(item) for item in sorted(self.discovered_via_source)],
            "expected_issue_search_urls": sorted(self.expected_issue_search_urls),
            "expected_issue_detail_urls": sorted(self.expected_issue_detail_urls),
            "expected_static_detail_urls": sorted(self.expected_static_detail_urls),
            "expected_en_issue_urls": sorted(self.expected_en_issue_urls),
            "processed_api_requests": sorted(self.processed_api_requests),
            "fetched_api_pages": sorted(self.fetched_api_pages),
            "processed_script_requests": sorted(self.processed_script_requests),
            "ajcass_known_routes": sorted(self.ajcass_known_routes),
            "ajcass_issue_route": self.ajcass_issue_route,
        }
        atomic_write_text(self.checkpoint_path, json.dumps(payload, ensure_ascii=False, indent=2))
        self.flush_incremental_discovery_outputs()
        self.pages_since_checkpoint = 0
        self.last_checkpoint_at = now
        self.logger.info(
            "Checkpoint saved completed=%s discovered=%s visited=%s frontier=%s active=%s detail_files=%s path=%s",
            self.completed,
            len(self.discovered_urls),
            len(self.visited_urls),
            len(self.frontier),
            len(self.active_queue_items),
            include_detail_files,
            self.checkpoint_path,
        )


class BatchRunner:
    def __init__(self, config_path: Union[str, Path] = DEFAULT_CONFIG_PATH) -> None:
        self.config_path = Path(config_path)
        self.batch_config = BatchConfig.from_file(self.config_path)
        self.output_root = Path(self.batch_config.output_root)
        if not self.output_root.is_absolute():
            self.output_root = self.config_path.parent / self.output_root
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.logger = configure_logger(
            "crawler.batch",
            self.batch_config.log_level,
            log_file=(self.output_root / "batch.log") if self.batch_config.log_to_file else None,
        )
        self.logger.info(
            "Batch runner initialized config=%s output_root=%s log_level=%s chromium_executable_path=%s max_site_concurrency=%s write_full_outputs_on_checkpoint=%s",
            self.config_path,
            self.output_root,
            self.batch_config.log_level,
            self.batch_config.chromium_executable_path or "<playwright-default>",
            self.batch_config.max_site_concurrency,
            self.batch_config.write_full_outputs_on_checkpoint,
        )

    def build_site_configs(self) -> list[SiteConfig]:
        input_path = Path(self.batch_config.input_urls_file)
        if not input_path.is_absolute():
            input_path = self.config_path.parent / input_path
        urls = load_seed_urls(input_path)
        grouped = group_urls_by_site(urls, include_homepage_seed=self.batch_config.include_site_homepage_seed)
        site_configs: list[SiteConfig] = []
        for site_key in sorted(grouped):
            payload = grouped[site_key]
            folder_name = sanitize_site_key(site_key)
            site_configs.append(
                SiteConfig(
                    site_key=site_key,
                    site_host=payload["site_host"],
                    site_origin=payload["site_origin"],
                    output_dir=self.output_root / folder_name,
                    seed_urls=payload["seed_urls"],
                    chromium_executable_path=self.batch_config.chromium_executable_path,
                    log_level=self.batch_config.log_level,
                    log_to_file=self.batch_config.log_to_file,
                    headless=self.batch_config.headless,
                    max_concurrency=self.batch_config.max_concurrency,
                    timeout_ms=self.batch_config.page_timeout_ms,
                    settle_ms=self.batch_config.settle_ms,
                    page_limit=self.batch_config.max_pages_per_site,
                    checkpoint_every_pages=self.batch_config.checkpoint_every_pages,
                    checkpoint_every_seconds=self.batch_config.checkpoint_every_seconds,
                    write_full_outputs_on_checkpoint=self.batch_config.write_full_outputs_on_checkpoint,
                    visit_leaf_pages=self.batch_config.visit_leaf_pages,
                    enable_generic_interactions=self.batch_config.enable_generic_interactions,
                    max_interaction_clicks_per_page=self.batch_config.max_interaction_clicks_per_page,
                    enable_cbpt_portal_ajax_expansion=self.batch_config.enable_cbpt_portal_ajax_expansion,
                    max_cbpt_portal_ajax_requests_per_page=self.batch_config.max_cbpt_portal_ajax_requests_per_page,
                    max_api_pages_per_series=self.batch_config.max_api_pages_per_series,
                    proxy_servers=self.batch_config.proxy_servers,
                    proxy_session_count=self.batch_config.proxy_session_count,
                    skip_failed_proxies=self.batch_config.skip_failed_proxies,
                    browser_launch_args=self.batch_config.browser_launch_args,
                    enable_request_blocking=self.batch_config.enable_request_blocking,
                    blocked_resource_types=self.batch_config.blocked_resource_types,
                    blocked_url_suffixes=self.batch_config.blocked_url_suffixes,
                )
            )
        self.logger.info("Built site configs count=%s input_path=%s", len(site_configs), input_path)
        return site_configs

    def _load_completed_site_summary_if_skippable(self, site_config: SiteConfig) -> Optional[dict[str, Any]]:
        checkpoint_path = site_config.output_dir / "checkpoint.json"
        if not checkpoint_path.exists():
            return None
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        policy_matches = checkpoint_matches_current_policy(checkpoint, self.batch_config.visit_leaf_pages)
        if checkpoint.get("completed") and self.batch_config.skip_completed_sites and policy_matches:
            summary_path = site_config.output_dir / "summary.json"
            self.logger.info(
                "Skipping completed site site=%s checkpoint=%s",
                site_config.site_key,
                checkpoint_path,
            )
            if summary_path.exists():
                return json.loads(summary_path.read_text(encoding="utf-8"))
            return {
                "site_key": site_config.site_key,
                "site_host": site_config.site_host,
                "site_origin": site_config.site_origin,
                "seed_urls": site_config.seed_urls,
                "completed": True,
            }
        if checkpoint.get("completed") and self.batch_config.skip_completed_sites and not policy_matches:
            self.logger.info(
                "Checkpoint policy changed; resuming site site=%s checkpoint=%s old_version=%s new_version=%s old_visit_leaf_pages=%s new_visit_leaf_pages=%s",
                site_config.site_key,
                checkpoint_path,
                checkpoint.get("crawl_policy_version", 0),
                CRAWL_POLICY_VERSION,
                checkpoint.get("visit_leaf_pages", False),
                self.batch_config.visit_leaf_pages,
            )
        return None

    async def run_site(self, site_config: SiteConfig, shared_playwright: Optional[Playwright] = None) -> dict[str, Any]:
        site_family = "ajcass" if is_ajcass_host(site_config.site_host) else "cbpt_cnki" if site_config.site_host.endswith(".cbpt.cnki.net") else "generic"
        try:
            self.logger.info(
                "Starting site crawl site=%s family=%s seeds=%s output_dir=%s",
                site_config.site_key,
                site_family,
                len(site_config.seed_urls),
                site_config.output_dir,
            )
            async with SiteCrawler(site_config, shared_playwright=shared_playwright) as crawler:
                summary = await crawler.crawl()
                self.logger.info(
                    "Site crawl finished site=%s completed=%s discovered=%s visited=%s failed=%s",
                    site_config.site_key,
                    summary.get("completed", False),
                    summary.get("counts", {}).get("discovered_urls", 0),
                    summary.get("counts", {}).get("visited_pages", 0),
                    summary.get("counts", {}).get("visit_failed", 0),
                )
                return summary
        except Exception:
            error_summary = {
                "site_key": site_config.site_key,
                "site_host": site_config.site_host,
                "site_origin": site_config.site_origin,
                "seed_urls": site_config.seed_urls,
                "completed": False,
                "error": traceback.format_exc(),
            }
            atomic_write_text(site_config.output_dir / "run_error.txt", error_summary["error"])
            atomic_write_text(site_config.output_dir / "summary.json", json.dumps(error_summary, ensure_ascii=False, indent=2))
            self.logger.exception("Site crawl failed site=%s output_dir=%s", site_config.site_key, site_config.output_dir)
            return error_summary

    async def run(self) -> dict[str, Any]:
        site_configs = self.build_site_configs()
        batch_results: list[dict[str, Any]] = []
        self.logger.info(
            "Batch run start sites=%s skip_completed=%s input_urls_file=%s max_site_concurrency=%s",
            len(site_configs),
            self.batch_config.skip_completed_sites,
            self.batch_config.input_urls_file,
            self.batch_config.max_site_concurrency,
        )
        runnable_sites: list[SiteConfig] = []
        for site_config in site_configs:
            skipped_summary = self._load_completed_site_summary_if_skippable(site_config)
            if skipped_summary is not None:
                batch_results.append(skipped_summary)
                continue
            runnable_sites.append(site_config)

        self.logger.info(
            "Batch site scheduling runnable=%s skipped=%s max_site_concurrency=%s",
            len(runnable_sites),
            len(batch_results),
            self.batch_config.max_site_concurrency,
        )
        shared_playwright = None
        active_tasks: Dict[asyncio.Task[dict[str, Any]], SiteConfig] = {}
        try:
            if runnable_sites:
                shared_playwright = await async_playwright().start()
            pending_sites = deque(runnable_sites)
            while pending_sites or active_tasks:
                while pending_sites and len(active_tasks) < self.batch_config.max_site_concurrency:
                    site_config = pending_sites.popleft()
                    task = asyncio.create_task(self.run_site(site_config, shared_playwright=shared_playwright))
                    active_tasks[task] = site_config
                    self.logger.info(
                        "Site task dispatched site=%s active_sites=%s pending_sites=%s",
                        site_config.site_key,
                        len(active_tasks),
                        len(pending_sites),
                    )
                if not active_tasks:
                    break
                done, _ = await asyncio.wait(active_tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    site_config = active_tasks.pop(task)
                    try:
                        batch_results.append(await task)
                    except Exception:
                        self.logger.exception("Site task crashed unexpectedly site=%s", site_config.site_key)
                    self.logger.info(
                        "Site task settled site=%s active_sites=%s pending_sites=%s batch_results=%s",
                        site_config.site_key,
                        len(active_tasks),
                        len(pending_sites),
                        len(batch_results),
                    )
        finally:
            if shared_playwright is not None:
                await shared_playwright.stop()

        batch_summary = self._write_global_outputs(batch_results)
        self.logger.info(
            "Batch run finished completed_sites=%s total_sites=%s summary=%s",
            batch_summary.get("sites_completed", 0),
            batch_summary.get("sites_total", 0),
            self.output_root / "batch_summary.json",
        )
        return batch_summary

    def _write_global_outputs(self, batch_results: list[dict[str, Any]]) -> dict[str, Any]:
        all_links_lines = ["site_folder\tsite_host\turl\tsame_site\tqueueable\tpage_kind\tfirst_depth"]
        all_urls: set[str] = set()
        sites: list[dict[str, Any]] = []
        all_link_rows: list[dict[str, Any]] = []

        for site_dir in sorted(path for path in self.output_root.iterdir() if path.is_dir()):
            summary_path = site_dir / "summary.json"
            nodes_path = site_dir / "nodes.jsonl"
            if not summary_path.exists():
                continue
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            site_host = str(summary.get("site_host", ""))
            site_family = str(summary.get("site_family", ""))
            if not site_family:
                if is_ajcass_host(site_host):
                    site_family = "ajcass"
                elif site_host.endswith(".cbpt.cnki.net"):
                    site_family = "cbpt_cnki"
                elif site_host:
                    site_family = "generic"
            counts = summary.get("counts", {})
            sites.append(
                {
                    "site_folder": site_dir.name,
                    "site_host": site_host,
                    "site_family": site_family,
                    "completed": summary.get("completed", False),
                    "discovered_urls": counts.get("discovered_urls", 0),
                    "queueable_urls": counts.get("queueable_urls", 0),
                    "same_site_urls": counts.get("same_site_urls", 0),
                    "external_or_non_queueable_urls": counts.get("external_or_non_queueable_urls", 0),
                    "edges": counts.get("edges", 0),
                    "visited_pages": counts.get("visited_pages", 0),
                    "visit_ok": counts.get("visit_ok", 0),
                    "visit_failed": counts.get("visit_failed", 0),
                }
            )
            if not nodes_path.exists():
                continue
            for line in nodes_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                item = json.loads(line)
                all_urls.add(str(item.get("url", "")))
                all_link_rows.append(
                    {
                        "site_folder": site_dir.name,
                        "site_host": site_host,
                        "site_family": site_family,
                        "url": str(item.get("url", "")),
                        "same_site": str(item.get("same_site", False)),
                        "queueable": str(item.get("queueable", False)),
                        "page_kind": str(item.get("page_kind", "")),
                        "first_depth": str(item.get("first_depth", "")),
                    }
                )
                all_links_lines.append(
                    "\t".join(
                        [
                            site_dir.name,
                            str(summary.get("site_host", "")),
                            str(item.get("url", "")),
                            str(item.get("same_site", False)),
                            str(item.get("queueable", False)),
                            str(item.get("page_kind", "")),
                            str(item.get("first_depth", "")),
                        ]
                    )
                )

        atomic_write_text(self.output_root / "all_discovered_urls.txt", "\n".join(sorted(url for url in all_urls if url)) + ("\n" if all_urls else ""))
        atomic_write_text(self.output_root / "all_discovered_urls.tsv", "\n".join(all_links_lines) + "\n")
        atomic_write_csv(
            self.output_root / "all_discovered_urls.csv",
            all_link_rows,
            ["site_folder", "site_host", "site_family", "url", "same_site", "queueable", "page_kind", "first_depth"],
        )
        batch_summary = {
            "generated_at": int(time.time()),
            "config_path": str(self.config_path),
            "sites_total": len(sites),
            "sites_completed": sum(1 for item in sites if item.get("completed")),
            "sites": sites,
        }
        atomic_write_text(self.output_root / "batch_summary.json", json.dumps(batch_summary, ensure_ascii=False, indent=2))
        atomic_write_csv(
            self.output_root / "sites_summary.csv",
            sites,
            [
                "site_folder",
                "site_host",
                "site_family",
                "completed",
                "discovered_urls",
                "queueable_urls",
                "same_site_urls",
                "external_or_non_queueable_urls",
                "edges",
                "visited_pages",
                "visit_ok",
                "visit_failed",
            ],
        )
        self.logger.info(
            "Global outputs written batch_summary=%s urls_csv=%s sites_csv=%s",
            self.output_root / "batch_summary.json",
            self.output_root / "all_discovered_urls.csv",
            self.output_root / "sites_summary.csv",
        )
        return batch_summary


async def async_main(config_path: str = DEFAULT_CONFIG_PATH) -> int:
    resolved_config_path = os.environ.get("CRAWLER_CONFIG_PATH", config_path)
    runner = BatchRunner(config_path=resolved_config_path)
    summary = await runner.run()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def main(config_path: str = DEFAULT_CONFIG_PATH) -> int:
    return asyncio.run(async_main(config_path=config_path))


if __name__ == "__main__":
    raise SystemExit(main())
