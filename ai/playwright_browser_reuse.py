from playwright.sync_api import sync_playwright
from pathlib import Path
import time
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


def content_stable_wait(page, max_wait=120):
    """
    Maximum reliability for content only - ignores images
    (same as before - keeping it for completeness)
    """
    logger.info("🔒 Waiting for content stability...\n")
    start_time = time.time()
    checks = {}

    # Wait for network idle multiple times
    for attempt in range(3):
        try:
            page.wait_for_load_state("networkidle", timeout=30000)
            checks[f'networkidle_{attempt}'] = True
            logger.info(f"  ✓ Network idle (check {attempt + 1}/3)")
            time.sleep(2)
        except:
            checks[f'networkidle_{attempt}'] = False

    # Content stabilization
    logger.info("  Checking content stability...")
    stable_count = 0
    required_stable = 5
    last_hash = ""

    for i in range(50):
        content_signature = page.evaluate("""
            () => {
                const text = document.body.innerText;
                const elements = document.querySelectorAll('*').length;
                const html_length = document.body.innerHTML.length;
                return `${text.length}:${elements}:${html_length}`;
            }
        """)

        current_hash = hashlib.md5(content_signature.encode()).hexdigest()

        if current_hash == last_hash:
            stable_count += 1
            if stable_count >= required_stable:
                checks['content_stable'] = True
                logger.info(f"  ✓ Content stable ({content_signature})")
                break
        else:
            stable_count = 0

        last_hash = current_hash
        time.sleep(1)
    else:
        checks['content_stable'] = False

    # Scroll to trigger lazy content
    for pos in [0.33, 0.66, 1.0, 0]:
        page.evaluate(f"""
            () => {{
                const height = Math.max(
                    document.body.scrollHeight,
                    document.documentElement.scrollHeight
                );
                window.scrollTo(0, height * {pos});
            }}
        """)
        time.sleep(1.5)
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except:
            pass

    checks['lazy_triggered'] = True

    # Final verification
    time.sleep(5)

    snapshot = page.evaluate("""
        () => {
            const text = document.body.innerText;
            return {
                textLength: text.length,
                elementCount: document.querySelectorAll('*').length,
                htmlLength: document.body.innerHTML.length
            };
        }
    """)

    elapsed = time.time() - start_time
    passed = sum(1 for v in checks.values() if v)
    total = len(checks)

    logger.info(f"  ⏱️  Wait time: {elapsed:.1f}s")
    logger.info(f"  ✅ Reliability: {passed}/{total} ({passed / total * 100:.1f}%)\n")

    return {
        "elapsed": elapsed,
        "checks": checks,
        "success_rate": passed / total,
        "snapshot": snapshot
    }


def save_single_page(page, url, output_dir, browser_session_id):
    """
    Saves a single page using an existing page instance
    """
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    safe_name = url.replace("https://", "").replace("http://", "").replace("/", "_")[:50]
    base_name = f"{safe_name}_{timestamp}"

    logger.info(f"{'=' * 70}")
    logger.info(f"🌐 URL: {url}")
    logger.info(f"{'=' * 70}\n")

    try:
        # Navigate
        logger.info("Loading page...")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Wait for content stability
        wait_result = content_stable_wait(page, max_wait=120)

        # Extract content
        logger.info("📦 Extracting content...")

        # 1. Full HTML
        html_content = page.content()
        html_path = f"{output_dir}/{base_name}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        logger.info(f"  💾 HTML: {len(html_content):,} bytes")

        # 2. Plain text
        text_content = page.evaluate("() => document.body.innerText")
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
            "element_count": wait_result['snapshot']['elementCount'],
            "reliability_score": wait_result['success_rate'],
            "wait_time": wait_result['elapsed']
        }

        metadata_path = f"{output_dir}/{base_name}_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        logger.info(f"  ✅ Success! Reliability: {wait_result['success_rate'] * 100:.1f}%\n")

        return {
            "url": url,
            "status": "success",
            "html": html_path,
            "text": text_path,
            "metadata": metadata_path,
            "reliability": wait_result['success_rate'],
            "size": len(html_content)
        }

    except Exception as e:
        logger.error(f"  ❌ Error: {e}\n")
        return {
            "url": url,
            "status": "failed",
            "error": str(e)
        }


def batch_scrape_optimized(urls, output_dir="batch_scrapes", delay_between_pages=3):
    """
    OPTIMIZED: Reuses browser instance for all URLs
    """
    Path(output_dir).mkdir(exist_ok=True)

    browser_session_id = time.strftime("%Y%m%d_%H%M%S")
    results = []

    logger.info(f"\n{'=' * 70}")
    logger.info(f"🚀 BATCH SCRAPING - OPTIMIZED MODE")
    logger.info(f"{'=' * 70}")
    logger.info(f"URLs to process: {len(urls)}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Delay between pages: {delay_between_pages}s")
    logger.info(f"Browser session ID: {browser_session_id}")
    logger.info(f"{'=' * 70}\n")

    with sync_playwright() as p:
        # Create browser ONCE
        logger.info("🔧 Launching browser...")
        browser = p.chromium.launch(headless=True)

        # Create context with realistic settings
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )

        # Create page ONCE
        page = context.new_page()
        page.set_default_timeout(120000)

        logger.info(f"✓ Browser ready\n")

        # Process all URLs with the same browser
        start_time = time.time()

        for i, url in enumerate(urls, 1):
            logger.info(f"📄 Processing {i}/{len(urls)}")

            result = save_single_page(page, url, output_dir, browser_session_id)
            results.append(result)

            # Delay between pages (be nice to servers)
            if i < len(urls):
                logger.info(f"⏳ Waiting {delay_between_pages}s before next page...\n")
                time.sleep(delay_between_pages)

        # Close browser ONCE at the end
        logger.info("🔧 Closing browser...")
        browser.close()

        total_time = time.time() - start_time

    # Save summary
    summary = {
        "session_id": browser_session_id,
        "total_urls": len(urls),
        "successful": sum(1 for r in results if r['status'] == 'success'),
        "failed": sum(1 for r in results if r['status'] == 'failed'),
        "total_time": total_time,
        "avg_time_per_url": total_time / len(urls),
        "results": results
    }

    summary_path = f"{output_dir}/batch_summary_{browser_session_id}.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    logger.info(f"\n{'=' * 70}")
    logger.info(f"✅ BATCH COMPLETE")
    logger.info(f"{'=' * 70}")
    logger.info(f"Total URLs: {summary['total_urls']}")
    logger.info(f"Successful: {summary['successful']}")
    logger.info(f"Failed: {summary['failed']}")
    logger.info(f"Total time: {total_time:.1f}s")
    logger.info(f"Avg per URL: {summary['avg_time_per_url']:.1f}s")
    logger.info(f"Summary: {summary_path}")
    logger.info(f"{'=' * 70}\n")

    return results


# Usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    urls = [
        "https://news.ycombinator.com",
        "https://example.com",
        "https://www.wikipedia.org",
        "https://github.com/microsoft/playwright-python"
    ]

    results = batch_scrape_optimized(urls, delay_between_pages=3)