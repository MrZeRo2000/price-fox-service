from pathlib import Path

from cfg import Configuration
from models import ScrapeSession
from .fetch_strategies import (
    FetchStrategy,
    JinaFetchStrategy,
    PlaywrightFetchStrategy,
)


class Fetcher:
    def __init__(self, configuration: Configuration, scrape_session: ScrapeSession):
        self.configuration = configuration
        self.scrape_session = scrape_session
        self._strategy_settings = self._load_strategy_settings()

    def _prepare_output_path(self) -> Path:
        base_data_root = Path(self.configuration.data_path)
        base_data_root.mkdir(parents=True, exist_ok=True)
        scrape_root = base_data_root / "scrape"
        scrape_root.mkdir(parents=True, exist_ok=True)
        session_folder_name = self.scrape_session.fetch_start_datetime.strftime(
            DATA_SESSION_FOLDER_DATETIME_FORMAT
        )
        session_data_root = scrape_root / session_folder_name
        session_data_root.mkdir(parents=True, exist_ok=True)
        return session_data_root

    @staticmethod
    def _product_url_output_dir(data_root: Path, product_id: int, url_id: int) -> Path:
        output_dir = data_root / str(product_id) / str(url_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    @staticmethod
    def _place_result_into_product_url_folder(result: dict, output_dir: Path) -> dict:
        if result.get("status") != "success":
            return result

        html_target = output_dir / "page.html"
        text_target = output_dir / "page.txt"
        metadata_target = output_dir / "metadata.json"

        shutil.move(result["html"], html_target)
        shutil.move(result["text"], text_target)
        shutil.move(result["metadata"], metadata_target)

        result["html"] = str(html_target)
        result["text"] = str(text_target)
        result["metadata"] = str(metadata_target)
        return result

    def _build_fetch_strategy(self) -> tuple[str, FetchStrategy]:
        strategy_name = (self.configuration.fetch_strategy or "playwright").lower()
        if strategy_name == "jina":
            return strategy_name, JinaFetchStrategy(
                rate_limit_rpm=self.configuration.jina_rate_limit_rpm
            )
        return "playwright", PlaywrightFetchStrategy()

    def execute(self):
        self.scrape_session.fetch_start_datetime = datetime.today()
        data_root = self._prepare_output_path()
        url_by_id = {
            url.url_id: str(url.url) for url in self.configuration.product_catalog_data.urls
        }
        jobs = [
            {
                "product_id": product.id,
                "url_id": url_id,
                "url": url_by_id[url_id],
            }
            for product in self.configuration.product_catalog_data.products
            for url_id in product.url_ids
        ]

        if not jobs:
            self.scrape_session.fetch_end_datetime = datetime.today()
            return []

        urls = [job["url"] for job in jobs]
        strategy_name, fetch_strategy = self._build_fetch_strategy()
        self.configuration.logger.info(
            f"Using fetch strategy: {strategy_name} "
            f"(jina_rate_limit_rpm={self.configuration.jina_rate_limit_rpm})"
        )
        raw_results = fetch_strategy.fetch_batch(
            urls=urls,
            output_dir=str(data_root),
            logger=self.configuration.logger,
        )

        all_results = []
        for job, result in zip(jobs, raw_results):
            output_dir = self._product_url_output_dir(
                data_root=data_root,
                product_id=job["product_id"],
                url_id=job["url_id"],
            )
            placed_result = self._place_result_into_product_url_folder(result, output_dir)
            all_results.append(
                {
                    "product_id": job["product_id"],
                    "url_id": job["url_id"],
                    "result": placed_result,
                }
            )

        for item in data_root.iterdir():
            if item.is_file():
                item.unlink()

        self.scrape_session.fetch_end_datetime = datetime.today()
        return all_results

    def execiute(self):
        """
        Backward-compatible misspelled alias.
        """
        return self.execute()
from abc import ABC, abstractmethod
from collections import deque
import hashlib
import html
import json
import logging
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

from cfg import Configuration
from models import ScrapeSession
from repositories import PriceStrategyRepository
from session.constants import DATA_SESSION_FOLDER_DATETIME_FORMAT


class FetchStrategy(ABC):
    @abstractmethod
    def fetch_batch(
        self,
        urls: list[str],
        output_dir: str,
        logger: Optional[logging.Logger] = None,
    ) -> list[dict]:
        raise NotImplementedError


class PlaywrightFetchStrategy(FetchStrategy):
    def fetch_batch(
        self,
        urls: list[str],
        output_dir: str,
        logger: Optional[logging.Logger] = None,
    ) -> list[dict]:
        return Fetcher.batch_scrape_optimized(
            urls=urls,
            output_dir=output_dir,
            logger=logger,
        )


class GeminiUrlFetchStrategy(FetchStrategy):
    @staticmethod
    def _write_placeholder_result_files(
        *,
        url: str,
        output_dir: str,
        browser_session_id: str,
        logger: Optional[logging.Logger] = None,
    ) -> dict:
        active_logger = logger or logging.getLogger("price_fox")
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

        active_logger.info("  💾 Placeholder fetch artifacts for gemini_url strategy.")
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
        logger: Optional[logging.Logger] = None,
    ) -> list[dict]:
        active_logger = logger or logging.getLogger("price_fox")
        results = []
        for url in urls:
            active_logger.info(
                "🧠 Skipping page fetch for gemini_url strategy; parsing will use URL directly."
            )
            results.append(
                self._write_placeholder_result_files(
                    url=url,
                    output_dir=output_dir,
                    browser_session_id=time.strftime("%Y%m%d_%H%M%S"),
                    logger=active_logger,
                )
            )
        return results


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
        scraping_strategy_used: str = "jina",
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
            "scraping_strategy_used": scraping_strategy_used,
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


class Fetcher:
    def __init__(self, configuration: Configuration, scrape_session: ScrapeSession):
        self.configuration = configuration
        self.scrape_session = scrape_session
        self._strategy_settings = self._load_strategy_settings()

    @staticmethod
    def content_stable_wait(page, max_wait=120, logger: Optional[logging.Logger] = None):
        """
        Maximum reliability for content only - ignores images
        (same as before - keeping it for completeness)
        """
        active_logger = logger or logging.getLogger("price_fox")
        active_logger.info("🔒 Waiting for content stability...")
        start_time = time.time()
        checks = {}

        def remaining_ms() -> int:
            remaining_seconds = max_wait - (time.time() - start_time)
            return max(0, int(remaining_seconds * 1000))

        # Wait for network idle multiple times
        for attempt in range(3):
            timeout_ms = min(15000, remaining_ms())
            if timeout_ms <= 0:
                checks[f"networkidle_{attempt}"] = False
                break
            try:
                page.wait_for_load_state("networkidle", timeout=timeout_ms)
                checks[f"networkidle_{attempt}"] = True
                active_logger.info(f"  ✓ Network idle (check {attempt + 1}/3)")
                time.sleep(2)
            except Exception:
                checks[f"networkidle_{attempt}"] = False

        # Content stabilization
        active_logger.info("  Checking content stability...")
        stable_count = 0
        required_stable = 5
        last_hash = ""

        for _ in range(50):
            if remaining_ms() <= 0:
                break
            content_signature = page.evaluate(
                """
                () => {
                    const text = document.body.innerText;
                    const elements = document.querySelectorAll('*').length;
                    const html_length = document.body.innerHTML.length;
                    return `${text.length}:${elements}:${html_length}`;
                }
            """
            )

            current_hash = hashlib.md5(content_signature.encode()).hexdigest()

            if current_hash == last_hash:
                stable_count += 1
                if stable_count >= required_stable:
                    checks["content_stable"] = True
                    active_logger.info(f"  ✓ Content stable ({content_signature})")
                    break
            else:
                stable_count = 0

            last_hash = current_hash
            if remaining_ms() <= 0:
                break
            time.sleep(1)
        else:
            checks["content_stable"] = False

        if "content_stable" not in checks:
            checks["content_stable"] = False

        # Scroll to trigger lazy content
        for pos in [0.33, 0.66, 1.0, 0]:
            if remaining_ms() <= 0:
                break
            page.evaluate(
                f"""
                () => {{
                    const height = Math.max(
                        document.body.scrollHeight,
                        document.documentElement.scrollHeight
                    );
                    window.scrollTo(0, height * {pos});
                }}
            """
            )
            time.sleep(1.5)
            try:
                timeout_ms = min(4000, remaining_ms())
                if timeout_ms > 0:
                    page.wait_for_load_state("networkidle", timeout=timeout_ms)
            except Exception:
                pass

        checks["lazy_triggered"] = True

        # Final verification
        if remaining_ms() > 0:
            time.sleep(min(2, remaining_ms() / 1000))

        snapshot = page.evaluate(
            """
            () => {
                const text = document.body.innerText;
                return {
                    textLength: text.length,
                    elementCount: document.querySelectorAll('*').length,
                    htmlLength: document.body.innerHTML.length
                };
            }
        """
        )

        elapsed = time.time() - start_time
        passed = sum(1 for v in checks.values() if v)
        total = len(checks)

        checks["timed_out"] = elapsed >= max_wait

        active_logger.info(f"  ⏱️  Wait time: {elapsed:.1f}s")
        active_logger.info(f"  ✅ Reliability: {passed}/{total} ({passed / total * 100:.1f}%)")

        return {
            "elapsed": elapsed,
            "checks": checks,
            "success_rate": passed / total,
            "snapshot": snapshot,
        }

    @staticmethod
    def save_single_page(
        page,
        url,
        output_dir,
        browser_session_id,
        scraping_strategy_used: str = "playwright",
        logger: Optional[logging.Logger] = None,
    ):
        """
        Saves a single page using an existing page instance
        """
        active_logger = logger or logging.getLogger("price_fox")
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        safe_name = (
            url.replace("https://", "").replace("http://", "").replace("/", "_")[:50]
        )
        base_name = f"{safe_name}_{timestamp}"
        retry_intervals_seconds = [2, 5, 10]
        max_retries = len(retry_intervals_seconds)

        active_logger.info(f"{'=' * 70}")
        active_logger.info(f"🌐 URL: {url}")
        active_logger.info(f"{'=' * 70}")

        try:
            html_content = ""
            text_content = ""
            wait_result = None
            attempt_count = 0
            started_at = time.time()
            max_url_runtime_seconds = 180

            for attempt in range(max_retries + 1):
                if time.time() - started_at > max_url_runtime_seconds:
                    raise RuntimeError(
                        f"URL processing timeout exceeded {max_url_runtime_seconds}s"
                    )
                attempt_count = attempt + 1
                active_logger.info(
                    f"Loading page... (attempt {attempt_count}/{max_retries + 1})"
                )
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                consent_accepted = Fetcher._try_accept_cookie_consent(
                    page, logger=active_logger
                )
                if not consent_accepted:
                    consent_accepted = Fetcher._disable_cookie_dialog_overlay(
                        page, logger=active_logger
                    )
                if consent_accepted:
                    # Some anti-bot setups release full content only after consent is stored.
                    try:
                        page.wait_for_load_state("networkidle", timeout=15000)
                    except Exception:
                        pass
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)

                if Fetcher._is_itbox_url(url):
                    challenge_cleared = Fetcher._wait_out_itbox_cloudflare_challenge(
                        page=page,
                        url=url,
                        logger=active_logger,
                    )
                    if not challenge_cleared:
                        raise RuntimeError(
                            "Cloudflare challenge persisted on itbox.ua; "
                            "browser fetch could not continue"
                        )

                Fetcher._dismiss_blocking_modals(page, logger=active_logger)

                # Quick pre-check to avoid long waits on known blocked pages.
                quick_html = page.content()
                quick_text = page.evaluate("() => document.body.innerText")
                if Fetcher._has_access_denied_content(quick_text, quick_html):
                    if attempt < max_retries:
                        retry_delay = retry_intervals_seconds[attempt]
                        active_logger.warning(
                            "  ⚠️ Access denied detected immediately after navigation. "
                            f"Retrying in {retry_delay}s..."
                        )
                        time.sleep(retry_delay)
                        continue
                    raise RuntimeError(
                        f"Access denied content persisted after {max_retries} retries "
                        f"(attempts={max_retries + 1})"
                    )

                # Wait for content stability
                wait_result = Fetcher.content_stable_wait(
                    page, max_wait=45, logger=active_logger
                )

                # Extract content
                active_logger.info("📦 Extracting content...")
                Fetcher._dismiss_blocking_modals(page, logger=active_logger)
                html_content = page.content()
                text_content = page.evaluate("() => document.body.innerText")
                text_length = len(text_content.strip())

                is_access_denied = Fetcher._has_access_denied_content(
                    text_content, html_content
                )
                if is_access_denied:
                    if attempt < max_retries:
                        retry_delay = retry_intervals_seconds[attempt]
                        active_logger.warning(
                            "  ⚠️ Access denied content detected. "
                            f"Retrying in {retry_delay}s after cookie-consent attempt..."
                        )
                        Fetcher._try_accept_cookie_consent(page, logger=active_logger)
                        Fetcher._disable_cookie_dialog_overlay(page, logger=active_logger)
                        time.sleep(retry_delay)
                        continue
                    raise RuntimeError(
                        f"Access denied content persisted after {max_retries} retries "
                        f"(attempts={max_retries + 1})"
                    )

                if text_length > 0:
                    active_logger.info(
                        f"  ✅ Extracted non-empty text ({text_length:,} chars)"
                    )
                    break

                if attempt < max_retries:
                    retry_delay = retry_intervals_seconds[attempt]
                    active_logger.warning(
                        f"  ⚠️ Empty text content (text_length=0). "
                        f"Retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                    continue

                raise RuntimeError(
                    f"Empty text content after {max_retries} retries "
                    f"(attempts={max_retries + 1})"
                )

            html_path = f"{output_dir}/{base_name}.html"
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            active_logger.info(f"  💾 HTML: {len(html_content):,} bytes")

            text_path = f"{output_dir}/{base_name}.txt"
            with open(text_path, "w", encoding="utf-8") as f:
                f.write(text_content)
            active_logger.info(f"  💾 Text: {len(text_content):,} chars")

            # 3. Metadata
            metadata = {
                "url": url,
                "timestamp": timestamp,
                "browser_session": browser_session_id,
                "title": page.title(),
                "text_length": len(text_content),
                "html_length": len(html_content),
                "element_count": wait_result["snapshot"]["elementCount"],
                "reliability_score": wait_result["success_rate"],
                "wait_time": wait_result["elapsed"],
                "fetch_attempts": attempt_count,
                "scraping_strategy_used": scraping_strategy_used,
            }

            metadata_path = f"{output_dir}/{base_name}_metadata.json"
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            active_logger.info(
                f"  ✅ Success! Reliability: {wait_result['success_rate'] * 100:.1f}%"
            )

            return {
                "url": url,
                "status": "success",
                "html": html_path,
                "text": text_path,
                "metadata": metadata_path,
                "reliability": wait_result["success_rate"],
                "size": len(html_content),
            }

        except Exception as e:
            active_logger.error(f"  ❌ Error: {e}")
            return {
                "url": url,
                "status": "failed",
                "error": str(e),
            }

    @staticmethod
    def _is_itbox_url(url: str) -> bool:
        host = (urlparse(url).hostname or "").lower()
        return host.endswith("itbox.ua")

    @staticmethod
    def _has_cloudflare_challenge_content(text_content: str, html_content: str) -> bool:
        visible_text = (text_content or "").lower()
        html_lower = (html_content or "").lower()

        strong_text_markers = (
            "verify you are not a bot",
            "this website uses a security service to protect against malicious bots",
            "checking your browser before accessing",
            "just a moment...",
            "ray id:",
            "attention required! | cloudflare",
        )
        if any(marker in visible_text for marker in strong_text_markers):
            return True

        html_markers = (
            "cf-browser-verification",
            "cf-challenge",
            "cf_turnstile",
            "challenges.cloudflare.com",
            "__cf_chl_",
            "data-ray=",
        )
        if any(marker in html_lower for marker in html_markers):
            return True

        weak_text_markers = (
            "performance and security by cloudflare",
            "cloudflare",
        )
        weak_hits = sum(marker in visible_text for marker in weak_text_markers)
        return weak_hits >= 2

    @staticmethod
    def _has_readable_page_content(text_content: str) -> bool:
        normalized = " ".join((text_content or "").split()).lower()
        if len(normalized) < 300:
            return False
        if len(normalized) > 1400:
            return True

        product_markers = (
            "купить",
            "ціна",
            "цена",
            "грн",
            "₴",
            "характеристики",
            "описание",
            "опис",
            "доставка",
            "в наличии",
            "в наявності",
        )
        marker_hits = sum(marker in normalized for marker in product_markers)
        return marker_hits >= 2

    @staticmethod
    def _wait_out_itbox_cloudflare_challenge(
        page,
        url: str,
        logger: Optional[logging.Logger] = None,
        max_wait_seconds: int = 30,
    ) -> bool:
        active_logger = logger or logging.getLogger("price_fox")
        if not Fetcher._is_itbox_url(url):
            return False

        started_at = time.time()
        check_interval_seconds = 3
        challenge_seen = False
        next_interaction_at = started_at
        first_check_text = page.evaluate("() => document.body.innerText")
        first_check_html = page.content()
        is_challenged = Fetcher._has_cloudflare_challenge_content(
            first_check_text, first_check_html
        )
        if not is_challenged:
            return True

        challenge_seen = True
        active_logger.warning(
            "  ⚠️ itbox.ua Cloudflare challenge detected. Waiting for automatic clearance..."
        )

        while time.time() - started_at < max_wait_seconds:
            now = time.time()
            # Excessive interaction can restart Cloudflare checks. Keep this sparse.
            if now >= next_interaction_at:
                Fetcher._try_interact_with_cloudflare_widget(page, logger=active_logger)
                next_interaction_at = now + 18
            # Human-like interaction and idle wait can help JS challenges complete.
            try:
                page.mouse.move(320, 260)
                time.sleep(0.2)
                page.mouse.move(760, 420)
            except Exception:
                pass
            try:
                page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            time.sleep(check_interval_seconds)

            current_text = page.evaluate("() => document.body.innerText")
            current_html = page.content()
            if not Fetcher._has_cloudflare_challenge_content(current_text, current_html):
                active_logger.info(
                    "  ✓ itbox.ua Cloudflare challenge appears cleared in current tab."
                )
                return True

        if challenge_seen:
            active_logger.warning(
                "  ⚠️ itbox.ua Cloudflare challenge did not clear within timeout."
            )
        return False

    @staticmethod
    def _try_interact_with_cloudflare_widget(
        page, logger: Optional[logging.Logger] = None
    ) -> bool:
        active_logger = logger or logging.getLogger("price_fox")
        selectors = (
            "label.ctp-checkbox-label",
            "input[type='checkbox']",
            "[role='checkbox']",
            ".ctp-checkbox",
            "button[type='submit']",
            "button:has-text('Verify')",
            "button:has-text('Підтвердити')",
        )

        for frame in page.frames:
            frame_url = (frame.url or "").lower()
            frame_name = (frame.name or "").lower()
            if (
                "cloudflare" not in frame_url
                and "challenge" not in frame_url
                and "turnstile" not in frame_url
                and "cloudflare" not in frame_name
            ):
                continue

            for selector in selectors:
                try:
                    locator = frame.locator(selector).first
                    if locator.is_visible(timeout=500):
                        locator.click(timeout=2000, force=True)
                        active_logger.info(
                            f"  ✓ Attempted Cloudflare widget interaction via: {selector}"
                        )
                        time.sleep(0.8)
                        return True
                except Exception:
                    continue

        return False

    @staticmethod
    def _has_access_denied_content(text_content: str, html_content: str) -> bool:
        combined = f"{text_content}\n{html_content}".lower()
        denial_markers = (
            "you don't have permission to access",
            "access denied",
            "error 403",
            "forbidden",
            "request blocked",
            "blocked by security policy",
        )
        has_access_denied = any(marker in combined for marker in denial_markers)
        if has_access_denied:
            return True
        return Fetcher._has_cloudflare_challenge_content(text_content, html_content)

    @staticmethod
    def _try_accept_cookie_consent(page, logger: Optional[logging.Logger] = None) -> bool:
        active_logger = logger or logging.getLogger("price_fox")
        active_logger.info("🍪 Checking for cookie consent dialog...")

        button_text_candidates = [
            "Accept All Cookies",
            "Accept all",
            "Accept",
            "I agree",
            "Allow all",
            "Прийняти всі",
            "Прийняти все",
            "Погоджуюсь",
            "Согласен",
            "Принять все",
            "Прийняти",
        ]
        css_candidates = [
            "#onetrust-accept-btn-handler",
            "button#onetrust-accept-btn-handler",
            "button[aria-label*='accept' i]",
            "button[id*='accept' i]",
            "button[class*='accept' i]",
            "button[data-testid*='accept' i]",
            "button[data-test*='accept' i]",
            "[role='button'][aria-label*='accept' i]",
            "button:has-text('Accept')",
            "button:has-text('Прийняти')",
            "button:has-text('Согласен')",
        ]

        for selector in css_candidates:
            try:
                locator = page.locator(selector).first
                if locator.is_visible(timeout=1000):
                    locator.click(timeout=3000)
                    time.sleep(1.5)
                    active_logger.info(f"  ✓ Accepted cookies via selector: {selector}")
                    return True
            except Exception:
                continue

        for text in button_text_candidates:
            try:
                locator = page.get_by_role("button", name=text, exact=False).first
                if locator.is_visible(timeout=1000):
                    locator.click(timeout=3000)
                    time.sleep(1.5)
                    active_logger.info(f"  ✓ Accepted cookies via button text: {text}")
                    return True
            except Exception:
                continue

        # Some consent managers render inside iframes.
        for frame in page.frames:
            for selector in css_candidates:
                try:
                    locator = frame.locator(selector).first
                    if locator.is_visible(timeout=1000):
                        locator.click(timeout=3000)
                        time.sleep(1.5)
                        active_logger.info(
                            f"  ✓ Accepted cookies via iframe selector: {selector}"
                        )
                        return True
                except Exception:
                    continue
            for text in button_text_candidates:
                try:
                    locator = frame.get_by_role("button", name=text, exact=False).first
                    if locator.is_visible(timeout=1000):
                        locator.click(timeout=3000)
                        time.sleep(1.5)
                        active_logger.info(
                            f"  ✓ Accepted cookies via iframe button text: {text}"
                        )
                        return True
                except Exception:
                    continue

        active_logger.info("  ℹ️ Cookie consent button was not found.")
        return False

    @staticmethod
    def _disable_cookie_dialog_overlay(
        page, logger: Optional[logging.Logger] = None
    ) -> bool:
        active_logger = logger or logging.getLogger("price_fox")

        # Fallback when consent dialog has non-clickable markup; remove OneTrust nodes
        # and set the common "alert closed" cookie to avoid immediate re-render.
        try:
            script = """
                () => {
                    let removed = false;
                    const selectors = [
                        "#onetrust-banner-sdk",
                        "#onetrust-consent-sdk",
                        "#onetrust-pc-sdk",
                        ".onetrust-pc-dark-filter",
                        ".ot-sdk-container",
                        "[id*='onetrust' i]",
                        "[class*='onetrust' i]",
                        "[id*='ot-sdk' i]",
                        "[class*='ot-sdk' i]",
                    ];
                    for (const selector of selectors) {
                        for (const el of document.querySelectorAll(selector)) {
                            el.remove();
                            removed = true;
                        }
                    }

                    const now = new Date().toUTCString();
                    document.cookie = `OptanonAlertBoxClosed=${now}; path=/; max-age=31536000`;
                    document.body.style.overflow = "auto";
                    document.documentElement.style.overflow = "auto";
                    return removed;
                }
                """

            try:
                removed_any = bool(page.evaluate(script))
            except Exception:
                removed_any = False

            for frame in page.frames:
                try:
                    frame_removed = bool(frame.evaluate(script))
                    removed_any = removed_any or frame_removed
                except Exception:
                    continue

            if removed_any:
                active_logger.info("  ✓ Disabled OneTrust cookie dialog overlay.")
            return bool(removed_any)
        except Exception:
            return False

    @staticmethod
    def _dismiss_blocking_modals(
        page, logger: Optional[logging.Logger] = None
    ) -> bool:
        active_logger = logger or logging.getLogger("price_fox")
        active_logger.info("🧹 Checking for blocking modal windows...")

        modal_selectors = (
            "[aria-modal='true']",
            "[role='dialog']",
            "[class*='modal' i]",
            "[id*='modal' i]",
            "[class*='popup' i]",
            "[id*='popup' i]",
            "[class*='overlay' i]",
            "[class*='backdrop' i]",
            "[id*='overlay' i]",
            "[class*='fancybox' i]",
            "[class*='city' i][class*='select' i]",
        )
        close_selectors = (
            "button[aria-label*='close' i]",
            "button[title*='close' i]",
            "button[class*='close' i]",
            "button[id*='close' i]",
            "[role='button'][aria-label*='close' i]",
            "[data-testid*='close' i]",
            "[class*='close' i]",
            "[id*='close' i]",
            ".modal-close",
            ".popup-close",
            ".js-popup-close",
            ".fancybox-close",
            ".fancybox-button--close",
            ".mfp-close",
        )
        close_texts = (
            "Close",
            "Dismiss",
            "No thanks",
            "Not now",
            "Skip",
            "Закрити",
            "Закрыть",
            "Не зараз",
            "Ні, дякую",
            "Пізніше",
            "Позже",
            "Пропустить",
        )

        dismissed = False

        for selector in close_selectors:
            try:
                locator = page.locator(selector).first
                if locator.is_visible(timeout=1000):
                    locator.click(timeout=2000, force=True)
                    dismissed = True
            except Exception:
                continue

        for text in close_texts:
            try:
                button = page.get_by_role("button", name=text, exact=False).first
                if button.is_visible(timeout=700):
                    button.click(timeout=1500, force=True)
                    dismissed = True
            except Exception:
                continue

        for selector in modal_selectors:
            try:
                modal = page.locator(selector).first
                if modal.is_visible(timeout=500):
                    modal.press("Escape", timeout=500)
                    dismissed = True
            except Exception:
                continue

        # Last-resort JS cleanup for full-screen overlays that trap scrolling/clicks.
        # Keep this conservative by targeting dialog-like and overlay-like elements only.
        try:
            removed_any = bool(
                page.evaluate(
                    """
                    () => {
                        const looksBlocking = (el) => {
                            if (!(el instanceof HTMLElement)) return false;
                            const style = window.getComputedStyle(el);
                            if (!style) return false;

                            const isVisible = style.display !== "none"
                                && style.visibility !== "hidden"
                                && parseFloat(style.opacity || "1") > 0.05;
                            if (!isVisible) return false;

                            const pos = style.position;
                            const isOverlayPosition = pos === "fixed" || pos === "sticky";
                            if (!isOverlayPosition) return false;

                            const z = Number.parseInt(style.zIndex || "0", 10);
                            const highZ = Number.isFinite(z) && z >= 100;
                            if (!highZ) return false;

                            const rect = el.getBoundingClientRect();
                            const viewportArea = window.innerWidth * window.innerHeight;
                            const area = Math.max(0, rect.width) * Math.max(0, rect.height);
                            const coversViewport = viewportArea > 0 && area / viewportArea > 0.30;
                            if (!coversViewport) return false;

                            const attrs = `${el.id} ${el.className} ${el.getAttribute("role") || ""} ${el.getAttribute("aria-modal") || ""}`.toLowerCase();
                            return (
                                attrs.includes("modal")
                                || attrs.includes("popup")
                                || attrs.includes("overlay")
                                || attrs.includes("backdrop")
                                || attrs.includes("dialog")
                                || attrs.includes("cookie")
                                || attrs.includes("consent")
                                || attrs.includes("city")
                            );
                        };

                        let removed = false;
                        for (const el of document.querySelectorAll("div, section, aside, dialog")) {
                            if (looksBlocking(el)) {
                                el.remove();
                                removed = true;
                            }
                        }

                        if (removed) {
                            document.body.style.overflow = "auto";
                            document.documentElement.style.overflow = "auto";
                        }
                        return removed;
                    }
                    """
                )
            )
            dismissed = dismissed or removed_any
        except Exception:
            pass

        if dismissed:
            active_logger.info("  ✓ Dismissed one or more blocking modal windows.")
        else:
            active_logger.info("  ℹ️ No blocking modal windows detected.")
        return dismissed

    @staticmethod
    def _needs_antibot_fallback(url: str, result: dict) -> bool:
        if result.get("status") != "failed":
            return False
        error = str(result.get("error", "")).lower()
        host = (urlparse(url).hostname or "").lower()
        is_access_error = (
            "access denied" in error
            or "you don't have permission" in error
            or "forbidden" in error
            or "blocked" in error
            or "cloudflare" in error
            or "verify you are not a bot" in error
            or "security service" in error
        )
        return is_access_error or host.endswith("watsons.ua") or host.endswith("itbox.ua")

    @staticmethod
    def _run_antibot_fallback_fetch(
        playwright,
        url: str,
        output_dir: str,
        browser_session_id: str,
        logger: Optional[logging.Logger] = None,
    ) -> dict:
        active_logger = logger or logging.getLogger("price_fox")
        active_logger.warning("🛡️ Running anti-bot fallback browser session...")
        antibot_headless = False

        browser = None
        context = None
        try:
            browser = playwright.chromium.launch(
                headless=antibot_headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--start-minimized",
                ],
            )
            context = browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="uk-UA",
                timezone_id="Europe/Kyiv",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()
            page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )
            page.set_default_timeout(90000)
            return Fetcher.save_single_page(
                page=page,
                url=url,
                output_dir=output_dir,
                browser_session_id=f"{browser_session_id}_antibot",
                scraping_strategy_used="playwright_antibot",
                logger=active_logger,
            )
        except Exception as exc:
            active_logger.error(f"  ❌ Anti-bot fallback failed: {exc}")
            return {
                "url": url,
                "status": "failed",
                "error": f"Anti-bot fallback failed: {exc}",
            }
        finally:
            try:
                if context is not None:
                    context.close()
            except Exception:
                pass
            try:
                if browser is not None:
                    browser.close()
            except Exception:
                pass

    @staticmethod
    def _run_jina_fallback_fetch(
        url: str,
        output_dir: str,
        browser_session_id: str,
        logger: Optional[logging.Logger] = None,
    ) -> dict:
        active_logger = logger or logging.getLogger("price_fox")
        active_logger.warning("🛰️ Running Jina fallback fetch...")
        fallback_rpm = 20

        try:
            jina = JinaFetchStrategy(rate_limit_rpm=fallback_rpm, timeout_seconds=45)
            markdown_content = jina._fetch_markdown(url=url, logger=active_logger)
            if not markdown_content.strip():
                raise RuntimeError("Received empty content from Jina fallback")
            if Fetcher._has_cloudflare_challenge_content(
                markdown_content, markdown_content
            ):
                raise RuntimeError(
                    "Jina fallback returned Cloudflare challenge text instead of product page"
                )
            return JinaFetchStrategy._write_result_files(
                url=url,
                output_dir=output_dir,
                browser_session_id=f"{browser_session_id}_jina",
                markdown_content=markdown_content,
                scraping_strategy_used="jina_fallback",
                logger=active_logger,
            )
        except Exception as exc:
            active_logger.error(f"  ❌ Jina fallback failed: {exc}")
            return {
                "url": url,
                "status": "failed",
                "error": f"Jina fallback failed: {exc}",
            }

    @staticmethod
    def _run_itbox_persistent_chrome_fallback_fetch(
        playwright,
        url: str,
        output_dir: str,
        browser_session_id: str,
        logger: Optional[logging.Logger] = None,
    ) -> dict:
        active_logger = logger or logging.getLogger("price_fox")
        active_logger.warning(
            "🧩 Running itbox persistent Chrome fallback (profile-based)..."
        )

        profile_dir = ".pricefox-itbox-chrome-profile"
        challenge_wait_seconds = 120
        headless = False
        manual_solve = True
        context = None
        try:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                channel="chrome",
                headless=headless,
                viewport={"width": 1366, "height": 768},
                locale="uk-UA",
                timezone_id="Europe/Kyiv",
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--start-minimized",
                ],
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )
            page.set_default_timeout(120000)
            page.goto(url, wait_until="domcontentloaded", timeout=90000)

            started_at = time.time()
            challenge_seen = False
            next_interaction_at = started_at
            next_manual_notice_at = started_at
            while time.time() - started_at < challenge_wait_seconds:
                now = time.time()
                if now >= next_interaction_at:
                    Fetcher._try_interact_with_cloudflare_widget(
                        page=page, logger=active_logger
                    )
                    next_interaction_at = now + 18
                Fetcher._dismiss_blocking_modals(page, logger=active_logger)
                current_text = page.evaluate("() => document.body.innerText")
                current_html = page.content()
                if (
                    Fetcher._has_readable_page_content(current_text)
                    and not Fetcher._has_access_denied_content(current_text, current_html)
                ):
                    active_logger.info(
                        "  ✓ Product-like readable content detected in persistent tab."
                    )
                    return Fetcher.save_single_page(
                        page=page,
                        url=url,
                        output_dir=output_dir,
                        browser_session_id=f"{browser_session_id}_itbox_chrome",
                        scraping_strategy_used="itbox_persistent_chrome",
                        logger=active_logger,
                    )
                still_challenge = Fetcher._has_cloudflare_challenge_content(
                    current_text, current_html
                )
                if not still_challenge:
                    active_logger.info(
                        "  ✓ Persistent Chrome fallback passed Cloudflare challenge."
                    )
                    return Fetcher.save_single_page(
                        page=page,
                        url=url,
                        output_dir=output_dir,
                        browser_session_id=f"{browser_session_id}_itbox_chrome",
                        scraping_strategy_used="itbox_persistent_chrome",
                        logger=active_logger,
                    )

                challenge_seen = True
                if manual_solve and not headless and now >= next_manual_notice_at:
                    active_logger.warning(
                        "  ⚠️ Cloudflare challenge is visible. "
                        "Please solve it in the opened Chrome window; waiting..."
                    )
                    next_manual_notice_at = now + 20
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                time.sleep(4)

            if challenge_seen:
                return {
                    "url": url,
                    "status": "failed",
                    "error": (
                        "Persistent Chrome fallback timed out while waiting for "
                        "Cloudflare challenge clearance"
                    ),
                }
            return {
                "url": url,
                "status": "failed",
                "error": "Persistent Chrome fallback failed unexpectedly",
            }
        except Exception as exc:
            active_logger.error(f"  ❌ Persistent Chrome fallback failed: {exc}")
            return {
                "url": url,
                "status": "failed",
                "error": f"Persistent Chrome fallback failed: {exc}",
            }
        finally:
            try:
                if context is not None:
                    context.close()
            except Exception:
                pass

    @staticmethod
    def batch_scrape_optimized(
        urls, output_dir="batch_scrapes", delay_between_pages=3, logger: Optional[logging.Logger] = None
    ):
        """
        OPTIMIZED: Reuses browser instance for all URLs
        """
        active_logger = logger or logging.getLogger("price_fox")
        Path(output_dir).mkdir(exist_ok=True)

        browser_session_id = time.strftime("%Y%m%d_%H%M%S")
        results = []

        active_logger.info(f"{'=' * 70}")
        active_logger.info("🚀 BATCH SCRAPING - OPTIMIZED MODE")
        active_logger.info(f"{'=' * 70}")
        active_logger.info(f"URLs to process: {len(urls)}")
        active_logger.info(f"Output directory: {output_dir}")
        active_logger.info(f"Delay between pages: {delay_between_pages}s")
        active_logger.info(f"Browser session ID: {browser_session_id}")
        active_logger.info(f"{'=' * 70}")

        with sync_playwright() as p:
            # Create browser ONCE
            active_logger.info("🔧 Launching browser...")
            browser = p.chromium.launch(headless=True)

            # Create context with realistic settings
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            )

            # Create page ONCE
            page = context.new_page()
            page.set_default_timeout(120000)

            active_logger.info("✓ Browser ready")

            # Process all URLs with the same browser
            start_time = time.time()

            for i, url in enumerate(urls, 1):
                active_logger.info(f"📄 Processing {i}/{len(urls)}")

                result = Fetcher.save_single_page(
                    page,
                    url,
                    output_dir,
                    browser_session_id,
                    scraping_strategy_used="playwright",
                    logger=active_logger,
                )
                if Fetcher._needs_antibot_fallback(url, result):
                    active_logger.warning(
                        "  ⚠️ Primary fetch failed/blocked; trying anti-bot fallback."
                    )
                    fallback_result = Fetcher._run_antibot_fallback_fetch(
                        playwright=p,
                        url=url,
                        output_dir=output_dir,
                        browser_session_id=browser_session_id,
                        logger=active_logger,
                    )
                    if fallback_result.get("status") == "success":
                        active_logger.info("  ✓ Anti-bot fallback succeeded.")
                        result = fallback_result
                    else:
                        active_logger.warning(
                            "  ⚠️ Anti-bot fallback did not resolve blocking."
                        )
                        if Fetcher._is_itbox_url(url):
                            active_logger.warning(
                                "  ⚠️ Trying itbox.ua persistent Chrome fallback."
                            )
                            chrome_fallback_result = (
                                Fetcher._run_itbox_persistent_chrome_fallback_fetch(
                                    playwright=p,
                                    url=url,
                                    output_dir=output_dir,
                                    browser_session_id=browser_session_id,
                                    logger=active_logger,
                                )
                            )
                            if chrome_fallback_result.get("status") == "success":
                                active_logger.info(
                                    "  ✓ Persistent Chrome fallback succeeded."
                                )
                                result = chrome_fallback_result
                            else:
                                active_logger.warning(
                                    "  ⚠️ Persistent Chrome fallback did not resolve blocking."
                                )
                                active_logger.warning(
                                    "  ⚠️ Trying itbox.ua Jina fallback after browser blocking."
                                )
                                jina_fallback_result = Fetcher._run_jina_fallback_fetch(
                                    url=url,
                                    output_dir=output_dir,
                                    browser_session_id=browser_session_id,
                                    logger=active_logger,
                                )
                                if jina_fallback_result.get("status") == "success":
                                    active_logger.info("  ✓ Jina fallback succeeded.")
                                    result = jina_fallback_result
                                else:
                                    active_logger.warning(
                                        "  ⚠️ Jina fallback did not resolve blocking."
                                    )
                results.append(result)

                # Delay between pages (be nice to servers)
                if i < len(urls):
                    active_logger.info(
                        f"⏳ Waiting {delay_between_pages}s before next page..."
                    )
                    time.sleep(delay_between_pages)

            # Close browser ONCE at the end
            active_logger.info("🔧 Closing browser...")
            browser.close()

            total_time = time.time() - start_time

        # Save summary
        summary = {
            "session_id": browser_session_id,
            "total_urls": len(urls),
            "successful": sum(1 for r in results if r["status"] == "success"),
            "failed": sum(1 for r in results if r["status"] == "failed"),
            "total_time": total_time,
            "avg_time_per_url": total_time / len(urls),
            "results": results,
        }

        summary_path = f"{output_dir}/batch_summary_{browser_session_id}.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        active_logger.info(f"{'=' * 70}")
        active_logger.info("✅ BATCH COMPLETE")
        active_logger.info(f"{'=' * 70}")
        active_logger.info(f"Total URLs: {summary['total_urls']}")
        active_logger.info(f"Successful: {summary['successful']}")
        active_logger.info(f"Failed: {summary['failed']}")
        active_logger.info(f"Total time: {total_time:.1f}s")
        active_logger.info(f"Avg per URL: {summary['avg_time_per_url']:.1f}s")
        active_logger.info(f"Summary: {summary_path}")
        active_logger.info(f"{'=' * 70}")

        return results

    def _prepare_output_path(self) -> Path:
        """
        Create and return this run's timestamped session output root.
        """
        base_data_root = Path(self.configuration.data_path)
        base_data_root.mkdir(parents=True, exist_ok=True)
        scrape_root = base_data_root / "scrape"
        scrape_root.mkdir(parents=True, exist_ok=True)
        session_folder_name = self.scrape_session.fetch_start_datetime.strftime(
            DATA_SESSION_FOLDER_DATETIME_FORMAT
        )
        session_data_root = scrape_root / session_folder_name
        session_data_root.mkdir(parents=True, exist_ok=True)
        return session_data_root

    @staticmethod
    def _product_url_output_dir(data_root: Path, product_id: int, url_id: int) -> Path:
        output_dir = data_root / str(product_id) / str(url_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    @staticmethod
    def _place_result_into_product_url_folder(result: dict, output_dir: Path) -> dict:
        if result.get("status") != "success":
            return result

        html_target = output_dir / "page.html"
        text_target = output_dir / "page.txt"
        metadata_target = output_dir / "metadata.json"

        shutil.move(result["html"], html_target)
        shutil.move(result["text"], text_target)
        shutil.move(result["metadata"], metadata_target)

        result["html"] = str(html_target)
        result["text"] = str(text_target)
        result["metadata"] = str(metadata_target)
        return result

    @staticmethod
    def _normalize_fetch_strategy_name(strategy_name: Optional[str]) -> str:
        normalized = (strategy_name or "").strip().lower().replace("-", "_")
        if normalized == "jina":
            return "jina"
        if normalized in {"gemini", "gemini_url"}:
            return "gemini_url"
        return "playwright"

    @staticmethod
    def _normalize_host(url: Optional[str]) -> str:
        if not url:
            return ""
        return (urlparse(url).hostname or "").strip().lower()

    @staticmethod
    def _to_positive_int(value: Optional[str], fallback: int) -> int:
        try:
            parsed = int(str(value).strip())
        except Exception:
            return fallback
        return parsed if parsed > 0 else fallback

    def _load_strategy_settings(self) -> dict[str, str]:
        db_path = self.configuration.product_catalog_db_path
        if not db_path:
            return {}
        try:
            repository = PriceStrategyRepository(db_path)
            return repository.load_settings()
        except Exception as exc:
            self.configuration.logger.warning(
                f"Unable to load fetch strategy settings from DB '{db_path}': {exc}"
            )
            return {}

    def _jina_rate_limit_rpm(self) -> int:
        raw_value = self._strategy_settings.get("jina_rate_limit_rpm")
        return self._to_positive_int(raw_value, fallback=20)

    def _load_site_fetch_strategy_overrides(self) -> dict[str, str]:
        db_path = self.configuration.product_catalog_db_path
        if not db_path:
            return {}
        try:
            repository = PriceStrategyRepository(db_path)
            raw_mapping = repository.load_domain_strategy_overrides()
        except Exception as exc:
            self.configuration.logger.warning(
                f"Unable to load fetch strategy domains from DB '{db_path}': {exc}"
            )
            return {}

        normalized: dict[str, str] = {}
        for domain, strategy_name in raw_mapping.items():
            domain_key = str(domain or "").strip().lower()
            if not domain_key:
                continue
            normalized[domain_key] = self._normalize_fetch_strategy_name(strategy_name)
        return normalized

    def _resolve_fetch_strategy(
        self,
        url: str,
        site_overrides: dict[str, str],
        default_strategy: str,
    ) -> str:
        host = self._normalize_host(url)
        if not host:
            return default_strategy
        if host in site_overrides:
            return site_overrides[host]

        best_suffix = ""
        best_strategy = default_strategy
        for suffix, strategy in site_overrides.items():
            normalized_suffix = suffix.lstrip(".")
            if not normalized_suffix:
                continue
            if host == normalized_suffix or host.endswith(f".{normalized_suffix}"):
                if len(normalized_suffix) > len(best_suffix):
                    best_suffix = normalized_suffix
                    best_strategy = strategy
        return best_strategy

    def _build_fetch_strategy(self, strategy_name: str) -> FetchStrategy:
        normalized = self._normalize_fetch_strategy_name(strategy_name)
        if normalized == "gemini_url":
            return GeminiUrlFetchStrategy()
        if normalized == "jina":
            return JinaFetchStrategy(rate_limit_rpm=self._jina_rate_limit_rpm())
        return PlaywrightFetchStrategy()

    def execute(self):
        self.scrape_session.fetch_start_datetime = datetime.today()
        data_root = self._prepare_output_path()
        url_by_id = {
            url.url_id: str(url.url)
            for url in self.configuration.product_catalog_data.urls
        }
        jobs = [
            {
                "product_id": product.id,
                "url_id": url_id,
                "url": url_by_id[url_id],
            }
            for product in self.configuration.product_catalog_data.products
            for url_id in product.url_ids
        ]

        if not jobs:
            self.scrape_session.fetch_end_datetime = datetime.today()
            return []

        default_strategy = self._normalize_fetch_strategy_name(
            self._strategy_settings.get("default_fetch_strategy", "playwright")
        )
        site_overrides = self._load_site_fetch_strategy_overrides()
        self.configuration.logger.info(
            f"Using DB-configurable fetch strategies "
            f"(default={default_strategy}, jina_rate_limit_rpm={self._jina_rate_limit_rpm()})"
        )

        jobs_by_strategy: dict[str, list[tuple[int, dict]]] = {}
        for index, job in enumerate(jobs):
            strategy = self._resolve_fetch_strategy(
                url=job["url"],
                site_overrides=site_overrides,
                default_strategy=default_strategy,
            )
            self.configuration.logger.info(
                f"Planned fetch strategy for url_id={job['url_id']} "
                f"({job['url']}): {strategy}"
            )
            jobs_by_strategy.setdefault(strategy, []).append((index, job))

        raw_results_by_index: dict[int, dict] = {}
        for strategy_name, indexed_jobs in jobs_by_strategy.items():
            strategy_urls = [job["url"] for _, job in indexed_jobs]
            fetch_strategy = self._build_fetch_strategy(strategy_name)
            self.configuration.logger.info(
                f"Fetching {len(strategy_urls)} URL(s) with strategy '{strategy_name}'"
            )
            strategy_results = fetch_strategy.fetch_batch(
                urls=strategy_urls,
                output_dir=str(data_root),
                logger=self.configuration.logger,
            )
            for (index, _), result in zip(indexed_jobs, strategy_results):
                raw_results_by_index[index] = result

        all_results = []
        for index, job in enumerate(jobs):
            result = raw_results_by_index.get(
                index,
                {
                    "url": job["url"],
                    "status": "failed",
                    "error": "Missing fetch result for resolved strategy batch",
                },
            )
            output_dir = self._product_url_output_dir(
                data_root=data_root,
                product_id=job["product_id"],
                url_id=job["url_id"],
            )
            placed_result = self._place_result_into_product_url_folder(result, output_dir)
            all_results.append(
                {
                    "product_id": job["product_id"],
                    "url_id": job["url_id"],
                    "result": placed_result,
                }
            )

        # Keep data root organized strictly by product_id/url_id folders.
        for item in data_root.iterdir():
            if item.is_file():
                item.unlink()

        self.scrape_session.fetch_end_datetime = datetime.today()

        return all_results

    def execiute(self):
        """
        Backward-compatible misspelled alias.
        """
        return self.execute()
