import logging

logger = logging.getLogger(__name__)


def batch_reliable_scrape(urls, output_dir="batch_scrapes"):
    """
    Process multiple URLs with full reliability checks
    """
    Path(output_dir).mkdir(exist_ok=True)
    
    results = []
    
    for i, url in enumerate(urls, 1):
        logger.info(f"{'='*70}")
        logger.info(f"Processing {i}/{len(urls)}: {url}")
        logger.info(f"{'='*70}")
        
        try:
            result = save_content_reliable(url, output_dir)
            result['status'] = 'success'
            results.append(result)
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            results.append({
                'url': url,
                'status': 'failed',
                'error': str(e)
            })
    
    # Save summary
    summary_path = f"{output_dir}/batch_summary_{time.strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_path, "w") as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"{'='*70}")
    logger.info("BATCH COMPLETE")
    logger.info(f"{'='*70}")
    logger.info(f"Total: {len(urls)}")
    logger.info(f"Success: {sum(1 for r in results if r['status'] == 'success')}")
    logger.info(f"Failed: {sum(1 for r in results if r['status'] == 'failed')}")
    logger.info(f"Summary: {summary_path}")
    logger.info(f"{'='*70}")
    
    return results

# Usage
urls = [
    "https://news.ycombinator.com",
    "https://example.com",
    "https://www.wikipedia.org"
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
batch_reliable_scrape(urls)