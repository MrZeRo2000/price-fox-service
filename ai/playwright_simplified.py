import logging

logger = logging.getLogger(__name__)


def simple_content_reliable(url, output_file="page.html"):
    """
    Simplified but still highly reliable
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(120000)
        
        logger.info(f"🌐 Loading: {url}")
        page.goto(url, wait_until="domcontentloaded")
        
        # Wait for network idle (multiple times)
        for i in range(3):
            page.wait_for_load_state("networkidle", timeout=30000)
            time.sleep(2)
        
        # Content stabilization
        logger.info("⏳ Waiting for content stability...")
        stable = 0
        last_length = 0
        
        for _ in range(20):
            current_length = page.evaluate("() => document.body.innerText.length")
            
            if current_length == last_length:
                stable += 1
                if stable >= 5:
                    logger.info(f"✓ Content stable at {current_length:,} chars")
                    break
            else:
                stable = 0
            
            last_length = current_length
            time.sleep(1)
        
        # Trigger lazy content
        logger.info("⏳ Triggering lazy content...")
        for pos in [0.5, 1.0, 0]:
            page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {pos})")
            time.sleep(2)
            page.wait_for_load_state("networkidle", timeout=10000)
        
        # Final wait
        logger.info("⏳ Final stabilization...")
        time.sleep(5)
        
        # Save
        html = page.content()
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html)
        
        browser.close()
        
        logger.info(f"✅ Saved: {output_file} ({len(html):,} bytes)")
        return output_file

# Usage
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
simple_content_reliable("https://example.com", "example.html")