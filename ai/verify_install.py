from playwright.sync_api import sync_playwright
import sys

def verify_installation():
    """Verify Playwright is installed correctly"""
    try:
        with sync_playwright() as p:
            print("✓ Playwright imported successfully")
            
            browser = p.chromium.launch(headless=True)
            print("✓ Chromium browser launched")
            
            page = browser.new_page()
            print("✓ Page created")
            
            page.goto("https://www.nme.com/")
            print("✓ Navigation successful")
            
            title = page.title()
            print(f"✓ Page title: {title}")
            
            browser.close()
            print("\n✅ All checks passed! Playwright is ready to use.")
            return True
            
    except Exception as e:
        print(f"\n❌ Installation verification failed: {e}")
        print("\nTry running: playwright install chromium")
        return False

if __name__ == "__main__":
    success = verify_installation()
    sys.exit(0 if success else 1)