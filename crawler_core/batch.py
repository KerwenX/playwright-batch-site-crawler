from __future__ import annotations

import asyncio
import json
import time
import traceback
from collections import deque
from pathlib import Path
from typing import Any, Dict, Optional, Union

from playwright.async_api import Playwright, async_playwright

from .constants import *
from .models import *
from .site import SiteCrawler
from .utils import *

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
        checkpoint_seed_urls = normalize_seed_url_set(checkpoint.get("seed_urls", []))
        current_seed_urls = normalize_seed_url_set(site_config.seed_urls)
        new_seed_urls = sorted(current_seed_urls - checkpoint_seed_urls)
        if checkpoint.get("completed") and self.batch_config.skip_completed_sites and policy_matches and not new_seed_urls:
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
        if checkpoint.get("completed") and self.batch_config.skip_completed_sites and policy_matches and new_seed_urls:
            self.logger.info(
                "Completed checkpoint has new seed URLs; resuming site site=%s checkpoint=%s new_seeds=%s",
                site_config.site_key,
                checkpoint_path,
                new_seed_urls,
            )
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
