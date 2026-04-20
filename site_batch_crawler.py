#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import re
import time
import traceback
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
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
    title: str
    ok: bool
    error: str = ""
    discoveries: int = 0
    started_at: float = 0.0
    finished_at: float = 0.0

    @property
    def duration_ms(self) -> int:
        return int((self.finished_at - self.started_at) * 1000)


@dataclass
class BatchConfig:
    input_urls_file: str
    output_root: str
    chromium_executable_path: str = ""
    headless: bool = True
    max_concurrency: int = 8
    page_timeout_ms: int = 20000
    settle_ms: int = 900
    max_pages_per_site: int = 0
    checkpoint_every_pages: int = 10
    checkpoint_every_seconds: int = 30
    skip_completed_sites: bool = True
    visit_leaf_pages: bool = False
    include_site_homepage_seed: bool = True
    enable_generic_interactions: bool = True
    max_interaction_clicks_per_page: int = 18
    max_api_pages_per_series: int = 0

    @classmethod
    def from_file(cls, path: str | Path) -> "BatchConfig":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        base_dir = Path(path).resolve().parent
        return cls(
            input_urls_file=str(payload.get("input_urls_file", DEFAULT_INPUT_URLS_FILE)),
            output_root=str(payload.get("output_root", "crawl_output")),
            chromium_executable_path=resolve_optional_path(payload.get("chromium_executable_path", ""), base_dir),
            headless=bool(payload.get("headless", True)),
            max_concurrency=int(payload.get("max_concurrency", 8)),
            page_timeout_ms=int(payload.get("page_timeout_ms", 20000)),
            settle_ms=int(payload.get("settle_ms", 900)),
            max_pages_per_site=int(payload.get("max_pages_per_site", 0)),
            checkpoint_every_pages=max(1, int(payload.get("checkpoint_every_pages", 10))),
            checkpoint_every_seconds=max(1, int(payload.get("checkpoint_every_seconds", 30))),
            skip_completed_sites=bool(payload.get("skip_completed_sites", True)),
            visit_leaf_pages=bool(payload.get("visit_leaf_pages", False)),
            include_site_homepage_seed=bool(payload.get("include_site_homepage_seed", True)),
            enable_generic_interactions=bool(payload.get("enable_generic_interactions", True)),
            max_interaction_clicks_per_page=max(0, int(payload.get("max_interaction_clicks_per_page", 18))),
            max_api_pages_per_series=max(0, int(payload.get("max_api_pages_per_series", 0))),
        )


@dataclass
class SiteConfig:
    site_key: str
    site_host: str
    site_origin: str
    output_dir: Path
    seed_urls: list[str]
    chromium_executable_path: str
    headless: bool
    max_concurrency: int
    timeout_ms: int
    settle_ms: int
    page_limit: int
    checkpoint_every_pages: int
    checkpoint_every_seconds: int
    visit_leaf_pages: bool
    enable_generic_interactions: bool
    max_interaction_clicks_per_page: int
    max_api_pages_per_series: int


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


def sort_query(query: str) -> str:
    params = [
        (key, value)
        for key, value in parse_qsl(query, keep_blank_values=True)
        if key not in TRACKING_QUERY_KEYS
    ]
    return urlencode(sorted(params), doseq=True)


def is_ajcass_host(host: str) -> bool:
    lowered = host.lower()
    return lowered == "ajcass.com" or lowered.endswith(AJCASS_HOST_SUFFIX)


def normalize_seed_url(raw_url: str) -> str | None:
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


def load_seed_urls(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input URL file not found: {path}")
    urls: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        normalized = normalize_seed_url(line)
        if normalized:
            urls.append(normalized)
    return urls


def group_urls_by_site(urls: list[str], include_homepage_seed: bool) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
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
    def __init__(self, config: SiteConfig) -> None:
        self.config = config
        self.site_host = config.site_host
        self.site_origin = config.site_origin.rstrip("/")
        self.site_family = self.detect_site_family()
        self.is_ajcass = self.site_family == "ajcass"

        self.playwright: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.api_context = None

        self.frontier: list[QueueItem] = []
        self.discovered_urls: dict[str, dict[str, Any]] = {}
        self.visited_urls: set[str] = set()
        self.queued_urls: set[str] = set()
        self.edges: list[Discovery] = []
        self.visits: list[PageVisit] = []
        self.discovered_via_source: set[tuple[str, str, str]] = set()

        self.expected_issue_search_urls: set[str] = set()
        self.expected_issue_detail_urls: set[str] = set()
        self.expected_static_detail_urls: set[str] = set()
        self.expected_en_issue_urls: set[str] = set()
        self.processed_api_requests: set[str] = set()
        self.fetched_api_pages: set[str] = set()
        self.processed_script_requests: set[str] = set()
        self.ajcass_known_routes: set[str] = set()
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
        self.same_site_urls_path = self.config.output_dir / "same_site_urls.txt"
        self.external_urls_path = self.config.output_dir / "external_or_non_queueable_urls.txt"
        self.seed_urls_path = self.config.output_dir / "seed_urls.txt"

        self._load_or_initialize_state()

    async def __aenter__(self) -> "SiteCrawler":
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.playwright = await async_playwright().start()
        launch_kwargs: dict[str, Any] = {"headless": self.config.headless}
        if self.config.chromium_executable_path:
            executable_path = Path(self.config.chromium_executable_path)
            if not executable_path.exists():
                raise FileNotFoundError(
                    f"Configured chromium_executable_path does not exist: {executable_path}"
                )
            launch_kwargs["executable_path"] = str(executable_path)
        self.browser = await self.playwright.chromium.launch(**launch_kwargs)
        self.context = await self.browser.new_context(ignore_https_errors=True, accept_downloads=True)
        self.context.set_default_timeout(self.config.timeout_ms)
        self.api_context = await self.playwright.request.new_context(ignore_https_errors=True)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.api_context is not None:
            await self.api_context.dispose()
        if self.context is not None:
            await self.context.close()
        if self.browser is not None:
            await self.browser.close()
        if self.playwright is not None:
            await self.playwright.stop()

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
            return
        self._initialize_from_seed_urls(self.config.seed_urls)

    def _load_from_checkpoint(self, payload: dict[str, Any]) -> None:
        self.frontier = [QueueItem(**item) for item in payload.get("frontier", [])]
        self.discovered_urls = {
            item["url"]: item for item in payload.get("discovered_urls", [])
        }
        self.visited_urls = set(payload.get("visited_urls", []))
        self.queued_urls = set(payload.get("queued_urls", []))
        self.edges = [Discovery(**item) for item in payload.get("edges", [])]
        self.visits = [PageVisit(**item) for item in payload.get("visits", [])]
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
        self.frontier = [
            item
            for item in self.frontier
            if item.url not in self.visited_urls and self.should_visit_url(item.url)
        ]

    def _initialize_from_seed_urls(self, seed_urls: list[str]) -> None:
        for seed_url in seed_urls:
            self.enqueue_url(seed_url, depth=0, source_url=seed_url, method="seed")
        self.save_checkpoint(force=True, completed=False)

    def _merge_new_seed_urls(self, seed_urls: list[str]) -> None:
        for seed_url in seed_urls:
            self.enqueue_url(seed_url, depth=0, source_url=seed_url, method="seed")

    def normalize_url(self, raw_url: str, base_url: str | None = None) -> str | None:
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
            if lowered_path.endswith("/wktextcontent.aspx") or lowered_path.endswith("/paperdigest.aspx"):
                return "cbpt_article"
            if lowered_path.endswith("/wklist.aspx"):
                params = dict(parse_qsl(parts.query, keep_blank_values=True))
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
        if self.site_family == "cbpt_cnki" and parts.path.lower().endswith("/downloadissueinfo.aspx"):
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
        if self.site_family == "cbpt_cnki" and self.page_kind(url) in {"cbpt_article", "cbpt_aux"}:
            return False
        return True

    def build_ajcass_issue_url(
        self,
        *,
        content_id: Any,
        year: Any = None,
        issue: Any = None,
        title: str | None = None,
        english: bool = False,
    ) -> str | None:
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

    def register_url(self, raw_url: str, source_url: str, depth: int, method: str, note: str = "") -> str | None:
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
        if self.page_kind(source_url) == "english_index" and page_kind == "english_issue":
            self.expected_en_issue_urls.add(normalized)
        if page_kind == "issue_detail":
            self.expected_issue_detail_urls.add(normalized)
        elif page_kind == "detail":
            self.expected_static_detail_urls.add(normalized)
        return normalized

    def enqueue_url(self, raw_url: str, depth: int, source_url: str, method: str, note: str = "") -> str | None:
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
            if attr in {"href", "src", "action", "data-href", "data-url", "data-src", "poster", "location"}:
                urls.append(value)
        html = payload.get("html")
        if isinstance(html, str):
            urls.extend(self.extract_urls_from_string(html, allow_relative=False))
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

    def clean_extracted_url_candidate(self, candidate: str) -> str | None:
        value = candidate.strip()
        if not value:
            return None

        cleaned_chars: list[str] = []
        for ch in value:
            if ch.isspace() or ch in {'"', "'", "<", ">"} or ord(ch) > 127:
                break
            cleaned_chars.append(ch)

        cleaned = "".join(cleaned_chars).rstrip(").,;:!?]}")
        for entity in ("&quot;", "&#34;", "&#39;", "&apos;", "&gt;", "&lt;"):
            if entity in cleaned:
                cleaned = cleaned.split(entity, 1)[0]
        if cleaned in {"", "/", "//", "?", "./", "../"}:
            return None
        return cleaned

    def extract_urls_from_string(self, value: str, *, allow_relative: bool) -> list[str]:
        candidate = value.strip()
        if not candidate:
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
            if candidate.startswith(("//", "/", "./", "../", "?")):
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
        items: list[dict[str, Any]] | None,
        method: str,
        *,
        english: bool = False,
    ) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
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

    async def parse_paginated_site_content(self, source_url: str, depth: int, payload: dict[str, Any], total_pages: int) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
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
            response = await self.api_context.post(AJCASS_SITE_CONTENT_API, data=next_payload)
            data = await response.json()
            found.extend(await self.parse_site_content_response(data, next_payload, source_url, depth, allow_pagination=False))
        return found

    async def parse_paginated_issue_search(self, source_url: str, depth: int, payload: dict[str, Any], total_pages: int) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
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
            response = await self.api_context.post(AJCASS_ISSUE_SEARCH_API, data=next_payload)
            data = await response.json()
            found.extend(await self.parse_issue_search_response(data, next_payload, source_url, depth, allow_pagination=False))
        return found

    async def parse_paginated_issue_simple_search(self, source_url: str, depth: int, payload: dict[str, Any], total_pages: int) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
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
            response = await self.api_context.post(AJCASS_ISSUE_SIMPLE_API, data=next_payload)
            data = await response.json()
            found.extend(await self.parse_issue_simple_response(data, next_payload, source_url, depth, allow_pagination=False))
        return found

    async def parse_site_content_response(
        self,
        data: dict[str, Any],
        payload: dict[str, Any],
        source_url: str,
        depth: int,
        allow_pagination: bool = True,
    ) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
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
            found.extend(await self.parse_paginated_site_content(source_url, depth, payload, total_pages))
        return found

    async def parse_issue_search_response(
        self,
        data: dict[str, Any],
        payload: dict[str, Any],
        source_url: str,
        depth: int,
        allow_pagination: bool = True,
    ) -> list[tuple[str, str]]:
        found = self.parse_ajcass_issue_items(data.get("data") or [], "api:GetIssueNormalSearch:issueDetail")
        total_pages = int(data.get("totalpage") or 1)
        if allow_pagination and total_pages > int(payload.get("curr", 1)):
            found.extend(await self.parse_paginated_issue_search(source_url, depth, payload, total_pages))
        return found

    async def parse_issue_simple_response(
        self,
        data: dict[str, Any],
        payload: dict[str, Any],
        source_url: str,
        depth: int,
        allow_pagination: bool = True,
    ) -> list[tuple[str, str]]:
        found = self.parse_ajcass_issue_items(
            data.get("data") or [],
            "api:GetIssueSimpleSearch:enIssue",
            english=True,
        )
        total_pages = int(data.get("totalpage") or 1)
        if allow_pagination and total_pages > int(payload.get("curr", 1)):
            found.extend(await self.parse_paginated_issue_simple_search(source_url, depth, payload, total_pages))
        return found

    def parse_current_issue_tree(self, data: dict[str, Any]) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []

        def walk_channels(channels: list[dict[str, Any]] | None) -> None:
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

    def parse_year_volume_tree(self, data: dict[str, Any]) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
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

    def parse_content_info(self, data: dict[str, Any]) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
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

    async def parse_script_response(self, response: Response) -> list[tuple[str, str]]:
        if not self.is_ajcass or response.url in self.processed_script_requests:
            return []
        self.processed_script_requests.add(response.url)
        try:
            text = await response.text()
        except Exception:
            return []

        found: list[tuple[str, str]] = []
        for route in AJCASS_SCRIPT_ROUTE_CANDIDATES:
            if f'"{route}"' in text or f"'{route}'" in text:
                self.ajcass_known_routes.add(route)
                found.append((f"{self.site_origin}/#{route}", "response:script:route"))
        return found

    async def parse_json_response(self, response: Response, source_url: str, depth: int, page_kind: str) -> list[tuple[str, str]]:
        url = response.url
        request = response.request
        api_key = f"{url}|{request.method}|{request.post_data or ''}"
        if api_key in self.processed_api_requests:
            return []
        self.processed_api_requests.add(api_key)

        try:
            data = await response.json()
        except Exception:
            return []

        found: list[tuple[str, str]] = []
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
                found.extend(await self.parse_site_content_response(data, payload, source_url, depth))
            elif "GetIssueNormalSearch" in url:
                payload = {}
                try:
                    payload = request.post_data_json or {}
                except Exception:
                    payload = {}
                found.extend(await self.parse_issue_search_response(data, payload, source_url, depth))
            elif "GetIssueSimpleSearch" in url:
                payload = {}
                try:
                    payload = request.post_data_json or {}
                except Exception:
                    payload = {}
                found.extend(await self.parse_issue_simple_response(data, payload, source_url, depth))
            elif "GetIssueinfoList" in url:
                found.extend(self.parse_ajcass_issue_items(data.get("data") or [], "api:GetIssueinfoList:issueDetail"))
            elif "GetContentInfo" in url:
                found.extend(self.parse_content_info(data))
        found.extend((url_candidate, "response:json") for url_candidate in self.iter_string_urls(data))
        return found

    async def parse_response(self, response: Response, source_url: str, depth: int, page_kind: str) -> list[tuple[str, str]]:
        content_type = response.headers.get("content-type", "").lower()
        if "application/json" in content_type or "+json" in content_type:
            return await self.parse_json_response(response, source_url, depth, page_kind)
        if self.is_ajcass and ("javascript" in content_type or response.url.lower().endswith(".js")):
            return await self.parse_script_response(response)
        return []

    async def process_page(self, item: QueueItem) -> None:
        if self.context is None:
            raise RuntimeError("Browser context is not initialized.")

        page = await self.context.new_page()
        response_tasks: list[asyncio.Task[list[tuple[str, str]]]] = []
        discoveries: list[tuple[str, str]] = []
        visit = PageVisit(
            requested_url=item.url,
            final_url=item.url,
            depth=item.depth,
            page_kind=self.page_kind(item.url),
            title="",
            ok=False,
            started_at=time.time(),
        )

        def on_response(response: Response) -> None:
            interesting = (
                response.request.resource_type in {"document", "xhr", "fetch"}
                or "application/json" in response.headers.get("content-type", "").lower()
                or any(response.url.lower().endswith(suffix) for suffix in NON_HTML_SUFFIXES)
                or (self.is_ajcass and response.request.resource_type == "script")
            )
            if not interesting:
                return
            discoveries.append((response.url, f"response:{response.request.resource_type}"))
            response_tasks.append(
                asyncio.create_task(
                    self.parse_response(
                        response=response,
                        source_url=item.url,
                        depth=item.depth + 1,
                        page_kind=self.page_kind(item.url),
                    )
                )
            )

        page.on("response", on_response)
        try:
            await page.goto(item.url, wait_until="domcontentloaded")
            await self.settle_page(page)

            visit.final_url = page.url
            visit.page_kind = self.page_kind(page.url)
            visit.title = await page.title()
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
            discoveries.extend(await self.run_generic_interactions(page, page.url, visit.page_kind))

            if response_tasks:
                response_results = await asyncio.gather(*response_tasks, return_exceptions=True)
                for result in response_results:
                    if isinstance(result, list):
                        discoveries.extend(result)

            for raw_url, method in discoveries:
                self.enqueue_url(raw_url, item.depth + 1, page.url, method)

            visit.ok = True
            visit.discoveries = len(discoveries)
        except Exception:
            visit.error = traceback.format_exc()
        finally:
            visit.finished_at = time.time()
            self.visits.append(visit)
            self.visited_urls.add(item.url)
            await page.close()

    async def crawl(self) -> dict[str, Any]:
        processed_pages = 0
        hit_page_limit = False
        while self.frontier:
            if self.config.page_limit and processed_pages >= self.config.page_limit:
                hit_page_limit = True
                break
            batch = self.frontier[: self.config.max_concurrency]
            self.frontier = self.frontier[self.config.max_concurrency :]
            await asyncio.gather(*(self.process_page(item) for item in batch))
            processed_pages += len(batch)
            self.pages_since_checkpoint += len(batch)
            self.save_checkpoint()

        self.completed = not self.frontier and not hit_page_limit
        summary = self.build_summary()
        self.write_outputs(summary)
        self.save_checkpoint(force=True, completed=self.completed)
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

        return {
            "site_key": self.config.site_key,
            "site_host": self.site_host,
            "site_origin": self.site_origin,
            "site_family": self.site_family,
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
                "remaining_frontier_count": len(self.frontier),
            },
        }

    def write_outputs(self, summary: dict[str, Any]) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

        atomic_write_text(self.summary_path, json.dumps(summary, ensure_ascii=False, indent=2))

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
        atomic_write_text(self.seed_urls_path, "\n".join(self.config.seed_urls) + ("\n" if self.config.seed_urls else ""))

    def save_checkpoint(self, force: bool = False, completed: bool | None = None) -> None:
        if completed is not None:
            self.completed = completed
        now = time.time()
        if not force:
            if self.pages_since_checkpoint < self.config.checkpoint_every_pages and (now - self.last_checkpoint_at) < self.config.checkpoint_every_seconds:
                return

        summary = self.build_summary()
        self.write_outputs(summary)
        payload = {
            "site_key": self.config.site_key,
            "site_host": self.site_host,
            "site_origin": self.site_origin,
            "seed_urls": self.config.seed_urls,
            "completed": self.completed,
            "frontier": [asdict(item) for item in self.frontier],
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
        self.pages_since_checkpoint = 0
        self.last_checkpoint_at = now


class BatchRunner:
    def __init__(self, config_path: str | Path = DEFAULT_CONFIG_PATH) -> None:
        self.config_path = Path(config_path)
        self.batch_config = BatchConfig.from_file(self.config_path)
        self.output_root = Path(self.batch_config.output_root)
        if not self.output_root.is_absolute():
            self.output_root = self.config_path.parent / self.output_root
        self.output_root.mkdir(parents=True, exist_ok=True)

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
                    headless=self.batch_config.headless,
                    max_concurrency=self.batch_config.max_concurrency,
                    timeout_ms=self.batch_config.page_timeout_ms,
                    settle_ms=self.batch_config.settle_ms,
                    page_limit=self.batch_config.max_pages_per_site,
                    checkpoint_every_pages=self.batch_config.checkpoint_every_pages,
                    checkpoint_every_seconds=self.batch_config.checkpoint_every_seconds,
                    visit_leaf_pages=self.batch_config.visit_leaf_pages,
                    enable_generic_interactions=self.batch_config.enable_generic_interactions,
                    max_interaction_clicks_per_page=self.batch_config.max_interaction_clicks_per_page,
                    max_api_pages_per_series=self.batch_config.max_api_pages_per_series,
                )
            )
        return site_configs

    async def run(self) -> dict[str, Any]:
        site_configs = self.build_site_configs()
        batch_results: list[dict[str, Any]] = []

        for site_config in site_configs:
            checkpoint_path = site_config.output_dir / "checkpoint.json"
            if checkpoint_path.exists():
                checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
                if checkpoint.get("completed") and self.batch_config.skip_completed_sites:
                    summary_path = site_config.output_dir / "summary.json"
                    if summary_path.exists():
                        batch_results.append(json.loads(summary_path.read_text(encoding="utf-8")))
                    continue

            try:
                async with SiteCrawler(site_config) as crawler:
                    summary = await crawler.crawl()
                    batch_results.append(summary)
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
                batch_results.append(error_summary)

        batch_summary = self._write_global_outputs(batch_results)
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
        return batch_summary


async def async_main(config_path: str = DEFAULT_CONFIG_PATH) -> int:
    runner = BatchRunner(config_path=config_path)
    summary = await runner.run()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def main(config_path: str = DEFAULT_CONFIG_PATH) -> int:
    return asyncio.run(async_main(config_path=config_path))


if __name__ == "__main__":
    raise SystemExit(main())
