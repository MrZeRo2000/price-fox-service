# requirements.txt
## Web automation and scraping
playwright==1.48.0

## Optional: Async support (if using async version)
asyncio==3.4.3

## Optional: Better JSON handling
orjson==3.10.12

## Optional: Progress bars for batch processing
tqdm==4.67.1

## Optional: Logging
colorlog==6.9.0

# Installation Instructions

## Install Python dependencies
pip install -r requirements.txt

## Install Playwright browsers (REQUIRED)
playwright install chromium

## Or install all browsers
playwright install

# Full Installation with Optional Dependencies
pip install playwright==1.48.0
playwright install chromium

## Optional but recommended
pip install tqdm colorlog