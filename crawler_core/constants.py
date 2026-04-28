from __future__ import annotations

import re

DEFAULT_CONFIG_PATH = "config.json"
DEFAULT_INPUT_URLS_FILE = "input_urls.txt"
CRAWL_POLICY_VERSION = 5
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
HTML_TAGLIKE_PATH_SEGMENTS = {
    "a",
    "b",
    "br",
    "dd",
    "div",
    "dl",
    "dt",
    "em",
    "font",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "i",
    "img",
    "li",
    "ol",
    "p",
    "small",
    "span",
    "strong",
    "sub",
    "sup",
    "table",
    "tbody",
    "td",
    "th",
    "thead",
    "tr",
    "u",
    "ul",
}
NON_NAVIGATIONAL_ENDPOINT_FILENAMES = {
    "articledownloadcontrol.do",
    "existscnctstinarticle.do",
    "exportcitation.do",
    "getdianjishu.jsp",
    "getdianjirichhtmlshu.jsp",
    "getxiazaishu.jsp",
    "showalertinfo.do",
    "waf_slider_captcha",
    "waf_slider_verify.html",
}
LOW_PRIORITY_NAVIGATION_FILENAMES = {
    "component.do",
    "css.aspx",
    "login.aspx",
    "register_note.aspx",
}
LOW_PRIORITY_NAVIGATION_SEGMENTS = {
    "auditor",
    "login",
    "register",
    "signin",
    "signup",
}
LOW_PRIORITY_NAVIGATION_PREFIXES = (
    "/uploadfile",
)
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
    ".csv",
    ".doc",
    ".docx",
    ".epub",
    ".eot",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".m4s",
    ".mp3",
    ".mp4",
    ".pdf",
    ".png",
    ".ppt",
    ".pptx",
    ".rar",
    ".svg",
    ".tar",
    ".ttf",
    ".wav",
    ".webm",
    ".webp",
    ".woff",
    ".woff2",
    ".xls",
    ".xlsx",
    ".zip",
]
FORCE_OPEN_SHADOW_ROOTS_SCRIPT = """(function() {
    if (!Element.prototype._attachShadow) {
        Element.prototype._attachShadow = Element.prototype.attachShadow;
        Element.prototype.attachShadow = function () {
            return this._attachShadow({mode:'open'});
        };
    }
})();"""
