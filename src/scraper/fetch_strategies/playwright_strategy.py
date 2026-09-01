"""Playwright fetch strategy -- the real page fetcher.

Drives a headless browser per URL and, when a page comes back blocked, walks a
recovery chain: anti-bot retry -> itbox persistent Chrome -> Jina reader.
"""
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

from logger import logger

from .base_fetch_strategy import BaseFetchStrategy
from .jina_strategy import JinaFetchStrategy


class PlaywrightFetchStrategy(BaseFetchStrategy):
    def fetch_batch(
        self,
        urls: list[str],
        output_dir: str,
    ) -> list[dict]:
        return self.batch_scrape_optimized(
            urls=urls,
            output_dir=output_dir,
        )

    @staticmethod
    def content_stable_wait(page, max_wait=120):
        """
        Maximum reliability for content only - ignores images
        (same as before - keeping it for completeness)
        """
        logger.info("🔒 Waiting for content stability...")
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
                logger.info(f"  ✓ Network idle (check {attempt + 1}/3)")
                time.sleep(2)
            except Exception:
                checks[f"networkidle_{attempt}"] = False

        # Content stabilization
        logger.info("  Checking content stability...")
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
                    logger.info(f"  ✓ Content stable ({content_signature})")
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

        logger.info(f"  ⏱️  Wait time: {elapsed:.1f}s")
        logger.info(f"  ✅ Reliability: {passed}/{total} ({passed / total * 100:.1f}%)")

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
    ):
        """
        Saves a single page using an existing page instance
        """
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        safe_name = (
            url.replace("https://", "").replace("http://", "").replace("/", "_")[:50]
        )
        base_name = f"{safe_name}_{timestamp}"
        retry_intervals_seconds = [2, 5, 10]
        max_retries = len(retry_intervals_seconds)

        logger.info(f"{'=' * 70}")
        logger.info(f"🌐 URL: {url}")
        logger.info(f"{'=' * 70}")

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
                logger.info(
                    f"Loading page... (attempt {attempt_count}/{max_retries + 1})"
                )
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                consent_accepted = PlaywrightFetchStrategy._try_accept_cookie_consent(
                    page
                )
                if not consent_accepted:
                    consent_accepted = PlaywrightFetchStrategy._disable_cookie_dialog_overlay(
                        page
                    )
                if consent_accepted:
                    # Some anti-bot setups release full content only after consent is stored.
                    try:
                        page.wait_for_load_state("networkidle", timeout=15000)
                    except Exception:
                        pass
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)

                if PlaywrightFetchStrategy._is_itbox_url(url):
                    challenge_cleared = PlaywrightFetchStrategy._wait_out_itbox_cloudflare_challenge(
                        page=page,
                        url=url,
                    )
                    if not challenge_cleared:
                        raise RuntimeError(
                            "Cloudflare challenge persisted on itbox.ua; "
                            "browser fetch could not continue"
                        )

                PlaywrightFetchStrategy._dismiss_blocking_modals(page)

                # Quick pre-check to avoid long waits on known blocked pages.
                quick_html = page.content()
                quick_text = page.evaluate("() => document.body.innerText")
                if PlaywrightFetchStrategy._has_access_denied_content(quick_text, quick_html):
                    if attempt < max_retries:
                        retry_delay = retry_intervals_seconds[attempt]
                        logger.warning(
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
                wait_result = PlaywrightFetchStrategy.content_stable_wait(
                    page, max_wait=45
                )

                # Extract content
                logger.info("📦 Extracting content...")
                PlaywrightFetchStrategy._dismiss_blocking_modals(page)
                html_content = page.content()
                text_content = page.evaluate("() => document.body.innerText")
                text_length = len(text_content.strip())

                is_access_denied = PlaywrightFetchStrategy._has_access_denied_content(
                    text_content, html_content
                )
                if is_access_denied:
                    if attempt < max_retries:
                        retry_delay = retry_intervals_seconds[attempt]
                        logger.warning(
                            "  ⚠️ Access denied content detected. "
                            f"Retrying in {retry_delay}s after cookie-consent attempt..."
                        )
                        PlaywrightFetchStrategy._try_accept_cookie_consent(page)
                        PlaywrightFetchStrategy._disable_cookie_dialog_overlay(page)
                        time.sleep(retry_delay)
                        continue
                    raise RuntimeError(
                        f"Access denied content persisted after {max_retries} retries "
                        f"(attempts={max_retries + 1})"
                    )

                if text_length > 0:
                    logger.info(
                        f"  ✅ Extracted non-empty text ({text_length:,} chars)"
                    )
                    break

                if attempt < max_retries:
                    retry_delay = retry_intervals_seconds[attempt]
                    logger.warning(
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
            logger.info(f"  💾 HTML: {len(html_content):,} bytes")

            text_path = f"{output_dir}/{base_name}.txt"
            with open(text_path, "w", encoding="utf-8") as f:
                f.write(text_content)
            logger.info(f"  💾 Text: {len(text_content):,} chars")

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

            logger.info(
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
            logger.error(f"  ❌ Error: {e}")
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
        max_wait_seconds: int = 30,
    ) -> bool:
        if not PlaywrightFetchStrategy._is_itbox_url(url):
            return False

        started_at = time.time()
        check_interval_seconds = 3
        challenge_seen = False
        next_interaction_at = started_at
        first_check_text = page.evaluate("() => document.body.innerText")
        first_check_html = page.content()
        is_challenged = PlaywrightFetchStrategy._has_cloudflare_challenge_content(
            first_check_text, first_check_html
        )
        if not is_challenged:
            return True

        challenge_seen = True
        logger.warning(
            "  ⚠️ itbox.ua Cloudflare challenge detected. Waiting for automatic clearance..."
        )

        while time.time() - started_at < max_wait_seconds:
            now = time.time()
            # Excessive interaction can restart Cloudflare checks. Keep this sparse.
            if now >= next_interaction_at:
                PlaywrightFetchStrategy._try_interact_with_cloudflare_widget(page)
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
            if not PlaywrightFetchStrategy._has_cloudflare_challenge_content(current_text, current_html):
                logger.info(
                    "  ✓ itbox.ua Cloudflare challenge appears cleared in current tab."
                )
                return True

        if challenge_seen:
            logger.warning(
                "  ⚠️ itbox.ua Cloudflare challenge did not clear within timeout."
            )
        return False

    @staticmethod
    def _try_interact_with_cloudflare_widget(
        page
    ) -> bool:
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
                        logger.info(
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
            # "forbidden",
            "request blocked",
            "blocked by security policy",
        )
        has_access_denied = any(marker in combined for marker in denial_markers)
        if has_access_denied:
            return True
        return PlaywrightFetchStrategy._has_cloudflare_challenge_content(text_content, html_content)

    @staticmethod
    def _try_accept_cookie_consent(page) -> bool:
        logger.info("🍪 Checking for cookie consent dialog...")

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
                    logger.info(f"  ✓ Accepted cookies via selector: {selector}")
                    return True
            except Exception:
                continue

        for text in button_text_candidates:
            try:
                locator = page.get_by_role("button", name=text, exact=False).first
                if locator.is_visible(timeout=1000):
                    locator.click(timeout=3000)
                    time.sleep(1.5)
                    logger.info(f"  ✓ Accepted cookies via button text: {text}")
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
                        logger.info(
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
                        logger.info(
                            f"  ✓ Accepted cookies via iframe button text: {text}"
                        )
                        return True
                except Exception:
                    continue

        logger.info("  ℹ️ Cookie consent button was not found.")
        return False

    @staticmethod
    def _disable_cookie_dialog_overlay(
        page
    ) -> bool:

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
                logger.info("  ✓ Disabled OneTrust cookie dialog overlay.")
            return bool(removed_any)
        except Exception:
            return False

    @staticmethod
    def _dismiss_blocking_modals(
        page
    ) -> bool:
        logger.info("🧹 Checking for blocking modal windows...")

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
            logger.info("  ✓ Dismissed one or more blocking modal windows.")
        else:
            logger.info("  ℹ️ No blocking modal windows detected.")
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
    ) -> dict:
        logger.warning("🛡️ Running anti-bot fallback browser session...")
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
            return PlaywrightFetchStrategy.save_single_page(
                page=page,
                url=url,
                output_dir=output_dir,
                browser_session_id=f"{browser_session_id}_antibot",
                scraping_strategy_used="playwright_antibot",
            )
        except Exception as exc:
            logger.error(f"  ❌ Anti-bot fallback failed: {exc}")
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
    ) -> dict:
        logger.warning("🛰️ Running Jina fallback fetch...")
        fallback_rpm = 20

        try:
            jina = JinaFetchStrategy(rate_limit_rpm=fallback_rpm, timeout_seconds=45)
            markdown_content = jina._fetch_markdown(url=url)
            if not markdown_content.strip():
                raise RuntimeError("Received empty content from Jina fallback")
            if PlaywrightFetchStrategy._has_cloudflare_challenge_content(
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
            )
        except Exception as exc:
            logger.error(f"  ❌ Jina fallback failed: {exc}")
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
    ) -> dict:
        logger.warning(
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
                    PlaywrightFetchStrategy._try_interact_with_cloudflare_widget(
                        page=page
                    )
                    next_interaction_at = now + 18
                PlaywrightFetchStrategy._dismiss_blocking_modals(page)
                current_text = page.evaluate("() => document.body.innerText")
                current_html = page.content()
                if (
                    PlaywrightFetchStrategy._has_readable_page_content(current_text)
                    and not PlaywrightFetchStrategy._has_access_denied_content(current_text, current_html)
                ):
                    logger.info(
                        "  ✓ Product-like readable content detected in persistent tab."
                    )
                    return PlaywrightFetchStrategy.save_single_page(
                        page=page,
                        url=url,
                        output_dir=output_dir,
                        browser_session_id=f"{browser_session_id}_itbox_chrome",
                        scraping_strategy_used="itbox_persistent_chrome",
                    )
                still_challenge = PlaywrightFetchStrategy._has_cloudflare_challenge_content(
                    current_text, current_html
                )
                if not still_challenge:
                    logger.info(
                        "  ✓ Persistent Chrome fallback passed Cloudflare challenge."
                    )
                    return PlaywrightFetchStrategy.save_single_page(
                        page=page,
                        url=url,
                        output_dir=output_dir,
                        browser_session_id=f"{browser_session_id}_itbox_chrome",
                        scraping_strategy_used="itbox_persistent_chrome",
                    )

                challenge_seen = True
                if manual_solve and not headless and now >= next_manual_notice_at:
                    logger.warning(
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
            logger.error(f"  ❌ Persistent Chrome fallback failed: {exc}")
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
        urls, output_dir="batch_scrapes", delay_between_pages=3
    ):
        """
        OPTIMIZED: Reuses browser instance for all URLs
        """
        Path(output_dir).mkdir(exist_ok=True)

        browser_session_id = time.strftime("%Y%m%d_%H%M%S")
        results = []

        logger.info(f"{'=' * 70}")
        logger.info("🚀 BATCH SCRAPING - OPTIMIZED MODE")
        logger.info(f"{'=' * 70}")
        logger.info(f"URLs to process: {len(urls)}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Delay between pages: {delay_between_pages}s")
        logger.info(f"Browser session ID: {browser_session_id}")
        logger.info(f"{'=' * 70}")

        with sync_playwright() as p:
            # Create browser ONCE
            logger.info("🔧 Launching browser...")
            browser = p.chromium.launch(headless=True)

            # Create context with realistic settings
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            )

            # Create page ONCE
            page = context.new_page()
            page.set_default_timeout(120000)

            logger.info("✓ Browser ready")

            # Process all URLs with the same browser
            start_time = time.time()

            for i, url in enumerate(urls, 1):
                logger.info(f"📄 Processing {i}/{len(urls)}")

                result = PlaywrightFetchStrategy.save_single_page(
                    page,
                    url,
                    output_dir,
                    browser_session_id,
                    scraping_strategy_used="playwright",
                )
                if PlaywrightFetchStrategy._needs_antibot_fallback(url, result):
                    logger.warning(
                        "  ⚠️ Primary fetch failed/blocked; trying anti-bot fallback."
                    )
                    fallback_result = PlaywrightFetchStrategy._run_antibot_fallback_fetch(
                        playwright=p,
                        url=url,
                        output_dir=output_dir,
                        browser_session_id=browser_session_id,
                    )
                    if fallback_result.get("status") == "success":
                        logger.info("  ✓ Anti-bot fallback succeeded.")
                        result = fallback_result
                    else:
                        logger.warning(
                            "  ⚠️ Anti-bot fallback did not resolve blocking."
                        )
                        if PlaywrightFetchStrategy._is_itbox_url(url):
                            logger.warning(
                                "  ⚠️ Trying itbox.ua persistent Chrome fallback."
                            )
                            chrome_fallback_result = (
                                PlaywrightFetchStrategy._run_itbox_persistent_chrome_fallback_fetch(
                                    playwright=p,
                                    url=url,
                                    output_dir=output_dir,
                                    browser_session_id=browser_session_id,
                                )
                            )
                            if chrome_fallback_result.get("status") == "success":
                                logger.info(
                                    "  ✓ Persistent Chrome fallback succeeded."
                                )
                                result = chrome_fallback_result
                            else:
                                logger.warning(
                                    "  ⚠️ Persistent Chrome fallback did not resolve blocking."
                                )
                                logger.warning(
                                    "  ⚠️ Trying itbox.ua Jina fallback after browser blocking."
                                )
                                jina_fallback_result = PlaywrightFetchStrategy._run_jina_fallback_fetch(
                                    url=url,
                                    output_dir=output_dir,
                                    browser_session_id=browser_session_id,
                                )
                                if jina_fallback_result.get("status") == "success":
                                    logger.info("  ✓ Jina fallback succeeded.")
                                    result = jina_fallback_result
                                else:
                                    logger.warning(
                                        "  ⚠️ Jina fallback did not resolve blocking."
                                    )
                results.append(result)

                # Delay between pages (be nice to servers)
                if i < len(urls):
                    logger.info(
                        f"⏳ Waiting {delay_between_pages}s before next page..."
                    )
                    time.sleep(delay_between_pages)

            # Close browser ONCE at the end
            logger.info("🔧 Closing browser...")
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

        logger.info(f"{'=' * 70}")
        logger.info("✅ BATCH COMPLETE")
        logger.info(f"{'=' * 70}")
        logger.info(f"Total URLs: {summary['total_urls']}")
        logger.info(f"Successful: {summary['successful']}")
        logger.info(f"Failed: {summary['failed']}")
        logger.info(f"Total time: {total_time:.1f}s")
        logger.info(f"Avg per URL: {summary['avg_time_per_url']:.1f}s")
        logger.info(f"Summary: {summary_path}")
        logger.info(f"{'=' * 70}")

        return results
