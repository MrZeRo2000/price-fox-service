from playwright.sync_api import sync_playwright
import sys

from logger import create_application_logger
from config.settings import default_data_path


def verify_installation():
    """Verify Playwright is installed correctly"""
    logger = create_application_logger(
        data_path=default_data_path()
    )
    try:
        with sync_playwright() as p:
            logger.info("✓ Playwright imported successfully")
            
            browser = p.chromium.launch(headless=True)
            logger.info("✓ Chromium browser launched")
            
            page = browser.new_page()
            logger.info("✓ Page created")
            
            page.goto("https://www.nme.com/")
            logger.info("✓ Navigation successful")
            
            title = page.title()
            logger.info(f"✓ Page title: {title}")
            
            browser.close()
            logger.info("✅ All checks passed! Playwright is ready to use.")
            return True
            
    except Exception as e:
        logger.error(f"❌ Installation verification failed: {e}")
        logger.info("Try running: playwright install chromium")
        return False

if __name__ == "__main__":
    success = verify_installation()
    sys.exit(0 if success else 1)