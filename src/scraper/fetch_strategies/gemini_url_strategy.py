"""Gemini-URL fetch strategy.

Fetches nothing: it writes placeholder artifacts and defers price extraction to
the parse side, which reads the URL directly. Currently not wired into any
selection path -- kept for reuse.
"""
import html
import json
import logging
import time
from typing import Optional

from logger import logger

from .base_fetch_strategy import BaseFetchStrategy


class GeminiUrlFetchStrategy(BaseFetchStrategy):
    @staticmethod
    def _write_placeholder_result_files(
        *,
        url: str,
        output_dir: str,
        browser_session_id: str,
    ) -> dict:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        safe_name = (
            url.replace("https://", "").replace("http://", "").replace("/", "_")[:50]
        )
        base_name = f"{safe_name}_{timestamp}"

        placeholder_text = (
            "Fetch skipped by gemini_url strategy. "
            "Price extraction is deferred to parser via URL-based Gemini call."
        )
        text_path = f"{output_dir}/{base_name}.txt"
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(placeholder_text)

        placeholder_html = (
            "<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>{html.escape(url)}</title></head><body><pre>"
            f"{html.escape(placeholder_text)}</pre></body></html>"
        )
        html_path = f"{output_dir}/{base_name}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(placeholder_html)

        metadata = {
            "url": url,
            "timestamp": timestamp,
            "browser_session": browser_session_id,
            "title": f"Gemini deferred fetch for {url}",
            "text_length": len(placeholder_text),
            "html_length": len(placeholder_html),
            "element_count": None,
            "reliability_score": None,
            "wait_time": None,
            "fetch_attempts": 0,
            "scraping_strategy_used": "gemini_url",
            "fetch_strategy": "gemini_url",
            "source_endpoint": "gemini_url_deferred_parser",
        }
        metadata_path = f"{output_dir}/{base_name}_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        logger.info("  💾 Placeholder fetch artifacts for gemini_url strategy.")
        return {
            "url": url,
            "status": "success",
            "html": html_path,
            "text": text_path,
            "metadata": metadata_path,
            "reliability": None,
            "size": len(placeholder_html),
        }

    def fetch_batch(
        self,
        urls: list[str],
        output_dir: str,
    ) -> list[dict]:
        results = []
        for url in urls:
            logger.info(
                "🧠 Skipping page fetch for gemini_url strategy; parsing will use URL directly."
            )
            results.append(
                self._write_placeholder_result_files(
                    url=url,
                    output_dir=output_dir,
                    browser_session_id=time.strftime("%Y%m%d_%H%M%S"),
                )
            )
        return results
