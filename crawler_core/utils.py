from __future__ import annotations

import csv
import io
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .constants import *

def normalize_query_key(key: str) -> str:
    normalized = str(key or "")
    while normalized.lower().startswith("amp;"):
        normalized = normalized[4:]
    return normalized

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


def get_path_segments(path: str) -> List[str]:
    return [segment for segment in (path or "").lower().split("/") if segment]


def get_path_filename(path: str) -> str:
    segments = get_path_segments(path)
    return segments[-1] if segments else ""


def is_html_tag_like_path(path: str) -> bool:
    cleaned = (path or "").strip()
    if not cleaned:
        return False
    if cleaned.startswith("#"):
        cleaned = cleaned[1:]
    if cleaned.startswith("//"):
        return False
    while cleaned.startswith(("./", "../")):
        cleaned = cleaned[2:] if cleaned.startswith("./") else cleaned[3:]
    if cleaned.startswith("/"):
        cleaned = cleaned[1:]
    cleaned = cleaned.split("?", 1)[0].split("#", 1)[0].strip("/")
    if not cleaned or "/" in cleaned:
        return False
    return cleaned.lower() in HTML_TAGLIKE_PATH_SEGMENTS


def is_probably_non_navigational_endpoint(url: str) -> bool:
    parts = urlsplit(url)
    lowered_path = parts.path.lower()
    filename = get_path_filename(lowered_path)
    lowered_query = parts.query.lower()
    if any(lowered_path.startswith(prefix) for prefix in LOW_PRIORITY_NAVIGATION_PREFIXES):
        return True
    if filename in NON_NAVIGATIONAL_ENDPOINT_FILENAMES:
        return True
    if filename in {"404.htm", "404.html", "404.aspx", "500.htm", "500.html", "500.aspx"}:
        return True
    if "aspxerrorpath=" in lowered_query:
        return True
    return False


def is_probably_low_priority_navigation_url(url: str) -> bool:
    parts = urlsplit(url)
    lowered_path = parts.path.lower()
    filename = get_path_filename(lowered_path)
    segments = get_path_segments(lowered_path)
    if any(lowered_path.startswith(prefix) for prefix in LOW_PRIORITY_NAVIGATION_PREFIXES):
        return True
    if filename in LOW_PRIORITY_NAVIGATION_FILENAMES:
        return True
    if any(segment in LOW_PRIORITY_NAVIGATION_SEGMENTS for segment in segments):
        return True
    return False


def checkpoint_matches_current_policy(checkpoint: Dict[str, Any], visit_leaf_pages: bool) -> bool:
    saved_version = int(checkpoint.get("crawl_policy_version", 0) or 0)
    saved_visit_leaf_pages = bool(checkpoint.get("visit_leaf_pages", False))
    return saved_version >= CRAWL_POLICY_VERSION and saved_visit_leaf_pages == visit_leaf_pages


def sort_query(query: str) -> str:
    params = [
        (normalize_query_key(key), value)
        for key, value in parse_qsl(query, keep_blank_values=True)
        if normalize_query_key(key) not in TRACKING_QUERY_KEYS and value != ""
    ]
    params = [(key, value) for key, value in params if key]
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
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        normalized = normalize_seed_url(line)
        if normalized:
            urls.append(normalized)
    return urls


def normalize_seed_url_set(urls: List[str]) -> Set[str]:
    normalized: Set[str] = set()
    for url in urls:
        value = normalize_seed_url(url)
        if value:
            normalized.add(value)
    return normalized


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
