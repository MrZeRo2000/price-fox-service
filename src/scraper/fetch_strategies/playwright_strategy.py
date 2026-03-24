import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

from .base import FetchStrategy


class PlaywrightFetchStrategy(FetchStrategy):
    @staticmethod
    def content_stable_wait(page, max_wait=120, logger: Optional[logging.Logger] = None):
        active_logger = logger or logging.getLogger("price_fox")
        active_logger.info("🔒 Waiting for content stability...")
        start_time = time.time()
        checks = {}

        def remaining_ms() -> int:
            remaining_seconds = max_wait - (time.time() - start_time)
            return max(0, int(remaining_seconds * 1000))

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
        return any(marker in combined for marker in denial_markers)

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
        )
        return is_access_error or host.endswith("watsons.ua")

    @classmethod
    def _save_single_page(
        cls, page, url, output_dir, browser_session_id, logger: Optional[logging.Logger] = None
    ):
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
                consent_accepted = cls._try_accept_cookie_consent(
                    page, logger=active_logger
                )
                if not consent_accepted:
                    consent_accepted = cls._disable_cookie_dialog_overlay(
                        page, logger=active_logger
                    )
                if consent_accepted:
                    try:
                        page.wait_for_load_state("networkidle", timeout=15000)
                    except Exception:
                        pass
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)

                quick_html = page.content()
                quick_text = page.evaluate("() => document.body.innerText")
                if cls._has_access_denied_content(quick_text, quick_html):
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

                wait_result = cls.content_stable_wait(page, max_wait=45, logger=active_logger)

                active_logger.info("📦 Extracting content...")
                html_content = page.content()
                text_content = page.evaluate("() => document.body.innerText")
                text_length = len(text_content.strip())

                is_access_denied = cls._has_access_denied_content(
                    text_content, html_content
                )
                if is_access_denied:
                    if attempt < max_retries:
                        retry_delay = retry_intervals_seconds[attempt]
                        active_logger.warning(
                            "  ⚠️ Access denied content detected. "
                            f"Retrying in {retry_delay}s after cookie-consent attempt..."
                        )
                        cls._try_accept_cookie_consent(page, logger=active_logger)
                        cls._disable_cookie_dialog_overlay(page, logger=active_logger)
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

    @classmethod
    def _run_antibot_fallback_fetch(
        cls,
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
            return cls._save_single_page(
                page=page,
                url=url,
                output_dir=output_dir,
                browser_session_id=f"{browser_session_id}_antibot",
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
        active_logger.info("🚀 BATCH SCRAPING - OPTIMIZED MODE")
        active_logger.info(f"{'=' * 70}")
        active_logger.info(f"URLs to process: {len(urls)}")
        active_logger.info(f"Output directory: {output_dir}")
        active_logger.info("Delay between pages: 3s")
        active_logger.info(f"Browser session ID: {browser_session_id}")
        active_logger.info(f"{'=' * 70}")

        with sync_playwright() as p:
            active_logger.info("🔧 Launching browser...")
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            )

            page = context.new_page()
            page.set_default_timeout(120000)
            active_logger.info("✓ Browser ready")

            start_time = time.time()
            for i, url in enumerate(urls, 1):
                active_logger.info(f"📄 Processing {i}/{len(urls)}")
                result = self._save_single_page(
                    page,
                    url,
                    output_dir,
                    browser_session_id,
                    logger=active_logger,
                )
                if self._needs_antibot_fallback(url, result):
                    active_logger.warning(
                        "  ⚠️ Primary fetch failed/blocked; trying anti-bot fallback."
                    )
                    fallback_result = self._run_antibot_fallback_fetch(
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
                results.append(result)
                if i < len(urls):
                    active_logger.info("⏳ Waiting 3s before next page...")
                    time.sleep(3)

            active_logger.info("🔧 Closing browser...")
            browser.close()
            total_time = time.time() - start_time

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
