from collections import deque
import html
import json
import logging
import time
from pathlib import Path
from typing import Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from .base import FetchStrategy


class JinaFetchStrategy(FetchStrategy):
    def __init__(self, rate_limit_rpm: int = 20, timeout_seconds: int = 30):
        self.rate_limit_rpm = max(1, int(rate_limit_rpm))
        self.timeout_seconds = max(5, int(timeout_seconds))
        self._request_timestamps = deque()

    def _wait_for_rate_limit(self, logger: Optional[logging.Logger] = None):
        active_logger = logger or logging.getLogger("price_fox")
        while True:
            now = time.time()
            while self._request_timestamps and now - self._request_timestamps[0] >= 60:
                self._request_timestamps.popleft()
            if len(self._request_timestamps) < self.rate_limit_rpm:
                return
            wait_seconds = 60 - (now - self._request_timestamps[0])
            wait_seconds = max(0.1, wait_seconds)
            active_logger.info(
                f"⏳ Jina strategy rate limit reached ({self.rate_limit_rpm}/min). "
                f"Sleeping {wait_seconds:.1f}s..."
            )
            time.sleep(wait_seconds)

    @staticmethod
    def _build_jina_reader_url(url: str) -> str:
        encoded_url = urllib_parse.quote(url, safe=":/?&=#%")
        return f"https://r.jina.ai/{encoded_url}"

    def _fetch_markdown(self, url: str, logger: Optional[logging.Logger] = None) -> str:
        active_logger = logger or logging.getLogger("price_fox")
        reader_url = self._build_jina_reader_url(url)
        retry_delays = [2, 5, 10]

        for attempt in range(len(retry_delays) + 1):
            self._wait_for_rate_limit(active_logger)
            request = urllib_request.Request(
                reader_url,
                headers={
                    "User-Agent": "price-fox-service/1.0",
                    "Accept": "text/plain, text/markdown;q=0.9, */*;q=0.5",
                },
                method="GET",
            )
            self._request_timestamps.append(time.time())
            try:
                with urllib_request.urlopen(
                    request, timeout=self.timeout_seconds
                ) as response:
                    payload = response.read()
                    return payload.decode("utf-8", errors="replace")
            except urllib_error.HTTPError as exc:
                retryable = exc.code in {429, 500, 502, 503, 504}
                if attempt < len(retry_delays) and retryable:
                    delay = retry_delays[attempt]
                    active_logger.warning(
                        f"  ⚠️ Jina HTTP {exc.code} for '{url}'. Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"Jina HTTP {exc.code}: {exc.reason}") from exc
            except urllib_error.URLError as exc:
                if attempt < len(retry_delays):
                    delay = retry_delays[attempt]
                    active_logger.warning(
                        f"  ⚠️ Jina network error for '{url}': {exc.reason}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"Jina network error: {exc.reason}") from exc

        raise RuntimeError("Unexpected Jina fetch retry flow exit")

    @staticmethod
    def _write_result_files(
        *,
        url: str,
        output_dir: str,
        browser_session_id: str,
        markdown_content: str,
        logger: Optional[logging.Logger] = None,
    ) -> dict:
        active_logger = logger or logging.getLogger("price_fox")
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        safe_name = (
            url.replace("https://", "").replace("http://", "").replace("/", "_")[:50]
        )
        base_name = f"{safe_name}_{timestamp}"

        text_path = f"{output_dir}/{base_name}.txt"
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)

        rendered_html = (
            "<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>{html.escape(url)}</title></head><body><pre>"
            f"{html.escape(markdown_content)}</pre></body></html>"
        )
        html_path = f"{output_dir}/{base_name}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(rendered_html)

        metadata = {
            "url": url,
            "timestamp": timestamp,
            "browser_session": browser_session_id,
            "title": f"Jina Reader content for {url}",
            "text_length": len(markdown_content),
            "html_length": len(rendered_html),
            "element_count": None,
            "reliability_score": None,
            "wait_time": None,
            "fetch_attempts": 1,
            "scraping_strategy_used": "jina",
            "fetch_strategy": "jina",
            "source_endpoint": "https://r.jina.ai/",
        }
        metadata_path = f"{output_dir}/{base_name}_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        active_logger.info(f"  💾 Text: {len(markdown_content):,} chars")

        return {
            "url": url,
            "status": "success",
            "html": html_path,
            "text": text_path,
            "metadata": metadata_path,
            "reliability": None,
            "size": len(markdown_content),
        }

    def fetch_batch(
        self,
        urls: list[str],
        output_dir: str,
        logger: Optional[logging.Logger] = None,
    ) -> list[dict]:
        active_logger = logger or logging.getLogger("price_fox")
        Path(output_dir).mkdir(exist_ok=True)
        browser_session_id = time.strftime("%Y%m%d_%H%M%S")
        results = []

        active_logger.info(f"{'=' * 70}")
        active_logger.info("🚀 BATCH SCRAPING - JINA READER MODE")
        active_logger.info(f"{'=' * 70}")
        active_logger.info(f"URLs to process: {len(urls)}")
        active_logger.info(f"Output directory: {output_dir}")
        active_logger.info(
            "Jina Reader endpoint: https://r.jina.ai/ (no API key, unauthenticated)"
        )
        active_logger.info(
            f"Local rate limiter: {self.rate_limit_rpm} requests per minute"
        )

        for i, url in enumerate(urls, 1):
            active_logger.info(f"📄 Processing {i}/{len(urls)}")
            try:
                markdown_content = self._fetch_markdown(url=url, logger=active_logger)
                if not markdown_content.strip():
                    raise RuntimeError("Received empty content from Jina Reader")
                result = self._write_result_files(
                    url=url,
                    output_dir=output_dir,
                    browser_session_id=browser_session_id,
                    markdown_content=markdown_content,
                    logger=active_logger,
                )
            except Exception as exc:
                active_logger.error(f"  ❌ Error: {exc}")
                result = {
                    "url": url,
                    "status": "failed",
                    "error": str(exc),
                }
            results.append(result)

        return results
