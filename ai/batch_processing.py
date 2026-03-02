def batch_reliable_scrape(urls, output_dir="batch_scrapes"):
    """
    Process multiple URLs with full reliability checks
    """
    Path(output_dir).mkdir(exist_ok=True)
    
    results = []
    
    for i, url in enumerate(urls, 1):
        print(f"\n{'='*70}")
        print(f"Processing {i}/{len(urls)}: {url}")
        print(f"{'='*70}")
        
        try:
            result = save_content_reliable(url, output_dir)
            result['status'] = 'success'
            results.append(result)
        except Exception as e:
            print(f"❌ Error: {e}")
            results.append({
                'url': url,
                'status': 'failed',
                'error': str(e)
            })
    
    # Save summary
    summary_path = f"{output_dir}/batch_summary_{time.strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_path, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\n{'='*70}")
    print(f"BATCH COMPLETE")
    print(f"{'='*70}")
    print(f"Total: {len(urls)}")
    print(f"Success: {sum(1 for r in results if r['status'] == 'success')}")
    print(f"Failed: {sum(1 for r in results if r['status'] == 'failed')}")
    print(f"Summary: {summary_path}")
    print(f"{'='*70}")
    
    return results

# Usage
urls = [
    "https://news.ycombinator.com",
    "https://example.com",
    "https://www.wikipedia.org"
]

batch_reliable_scrape(urls)