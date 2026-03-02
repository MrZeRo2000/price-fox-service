from playwright.sync_api import sync_playwright
from pathlib import Path
import time
import hashlib
import json

def content_stable_wait(page, max_wait=120):
    """
    Maximum reliability for content only - ignores images
    """
    print("🔒 CONTENT-ONLY RELIABILITY MODE\n")
    start_time = time.time()
    checks = {}
    
    # LAYER 1: Basic loading
    print("Layer 1: Basic Page Load")
    try:
        page.wait_for_load_state("domcontentloaded", timeout=60000)
        checks['dom_loaded'] = True
        print("  ✓ DOM loaded")
    except Exception as e:
        checks['dom_loaded'] = False
        print(f"  ✗ DOM load failed: {e}")
    
    # Wait for network idle multiple times
    for attempt in range(3):
        try:
            page.wait_for_load_state("networkidle", timeout=30000)
            checks[f'networkidle_{attempt}'] = True
            print(f"  ✓ Network idle (check {attempt + 1}/3)")
            time.sleep(2)
        except:
            checks[f'networkidle_{attempt}'] = False
    
    # LAYER 2: Document ready
    print("\nLayer 2: Document Ready State")
    for i in range(15):
        ready_state = page.evaluate("() => document.readyState")
        if ready_state == "complete":
            checks['ready_state'] = True
            print(f"  ✓ Document ready: complete")
            break
        time.sleep(1)
    else:
        checks['ready_state'] = False
    
    # LAYER 3: Content hash stabilization (CRITICAL)
    print("\nLayer 3: Content Stabilization")
    stable_count = 0
    required_stable = 5  # Must be identical 5 times in a row
    last_hash = ""
    
    for i in range(50):  # Check for up to 50 seconds
        # Get content hash (text + element count)
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
            print(f"  Stable: {stable_count}/{required_stable} ({content_signature})")
            
            if stable_count >= required_stable:
                checks['content_stable'] = True
                print("  ✓ Content fully stabilized!")
                break
        else:
            if last_hash:
                print(f"  Content changed: {content_signature}")
            stable_count = 0
        
        last_hash = current_hash
        time.sleep(1)
    else:
        checks['content_stable'] = False
        print("  ⚠️ Content still changing")
    
    # LAYER 4: DOM Mutation Monitoring
    print("\nLayer 4: DOM Mutation Stability")
    mutation_result = page.evaluate("""
        async () => {
            return new Promise((resolve) => {
                let mutationCount = 0;
                let timeoutId;
                
                const observer = new MutationObserver((mutations) => {
                    mutationCount += mutations.length;
                    clearTimeout(timeoutId);
                    
                    // Wait for 4 seconds of no mutations
                    timeoutId = setTimeout(() => {
                        observer.disconnect();
                        resolve({ stable: true, mutations: mutationCount });
                    }, 4000);
                });
                
                observer.observe(document.body, {
                    childList: true,
                    subtree: true,
                    attributes: true,
                    characterData: true
                });
                
                // Max 40 second timeout
                setTimeout(() => {
                    observer.disconnect();
                    resolve({ stable: false, mutations: mutationCount });
                }, 40000);
            });
        }
    """)
    
    checks['dom_stable'] = mutation_result['stable']
    print(f"  Mutations detected: {mutation_result['mutations']}")
    if mutation_result['stable']:
        print("  ✓ DOM stable (no changes for 4s)")
    else:
        print("  ⚠️ DOM still mutating")
    
    # LAYER 5: JavaScript execution complete
    print("\nLayer 5: JavaScript Execution")
    try:
        page.wait_for_function("""
            () => {
                // Check if common JS indicators show completion
                if (typeof jQuery !== 'undefined' && jQuery.active > 0) {
                    return false;
                }
                
                // Check if document has meaningful content
                if (document.body.innerText.length < 100) {
                    return false;
                }
                
                return true;
            }
        """, timeout=20000)
        checks['js_complete'] = True
        print("  ✓ JavaScript execution complete")
    except:
        checks['js_complete'] = False
        print("  ⚠️ JavaScript check timeout")
    
    # LAYER 6: Scroll to trigger lazy-loaded content
    print("\nLayer 6: Lazy Content Triggering")
    scroll_positions = [0.33, 0.66, 1.0, 0]  # 33%, 66%, 100%, top
    
    for pos in scroll_positions:
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
        
        # Wait for network after each scroll
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except:
            pass
    
    checks['lazy_triggered'] = True
    print("  ✓ Lazy content triggered")
    
    # LAYER 7: Final content verification
    print("\nLayer 7: Final Verification")
    time.sleep(5)  # Final 5-second pause
    
    # Take two content snapshots 3 seconds apart
    snapshot1 = page.evaluate("""
        () => {
            const text = document.body.innerText;
            return {
                textLength: text.length,
                textHash: text.substring(0, 1000),  // First 1000 chars
                elementCount: document.querySelectorAll('*').length,
                htmlLength: document.body.innerHTML.length
            };
        }
    """)
    
    time.sleep(3)
    
    snapshot2 = page.evaluate("""
        () => {
            const text = document.body.innerText;
            return {
                textLength: text.length,
                textHash: text.substring(0, 1000),
                elementCount: document.querySelectorAll('*').length,
                htmlLength: document.body.innerHTML.length
            };
        }
    """)
    
    final_stable = (
        snapshot1['textLength'] == snapshot2['textLength'] and
        snapshot1['elementCount'] == snapshot2['elementCount'] and
        snapshot1['htmlLength'] == snapshot2['htmlLength']
    )
    
    checks['final_verification'] = final_stable
    
    if final_stable:
        print(f"  ✓ Final verification PASSED")
        print(f"    Text: {snapshot2['textLength']:,} chars")
        print(f"    Elements: {snapshot2['elementCount']:,}")
        print(f"    HTML: {snapshot2['htmlLength']:,} bytes")
    else:
        print(f"  ⚠️ Content differs between snapshots")
        print(f"    Snapshot 1: {snapshot1['textLength']} chars, {snapshot1['elementCount']} elements")
        print(f"    Snapshot 2: {snapshot2['textLength']} chars, {snapshot2['elementCount']} elements")
    
    elapsed = time.time() - start_time
    passed = sum(1 for v in checks.values() if v)
    total = len(checks)
    
    print(f"\n{'='*60}")
    print(f"⏱️  Wait time: {elapsed:.1f}s")
    print(f"✅ Reliability: {passed}/{total} checks passed ({passed/total*100:.1f}%)")
    print(f"{'='*60}\n")
    
    return {
        "elapsed": elapsed,
        "checks": checks,
        "success_rate": passed / total,
        "snapshot": snapshot2
    }

def save_content_reliable(url, output_dir="scraped_content"):
    """
    Maximum reliability content scraper - saves HTML and text
    """
    Path(output_dir).mkdir(exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    safe_name = url.replace("https://", "").replace("http://", "").replace("/", "_")[:50]
    base_name = f"{safe_name}_{timestamp}"
    
    print(f"\n{'='*70}")
    print(f"🎯 MAXIMUM RELIABILITY - CONTENT ONLY")
    print(f"{'='*70}")
    print(f"URL: {url}")
    print(f"Time: {timestamp}")
    print(f"{'='*70}\n")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        page.set_default_timeout(120000)
        
        # Navigate
        print("🌐 Navigating to URL...")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        print("✓ Initial load complete\n")
        
        # Ultra-reliable wait
        wait_result = content_stable_wait(page, max_wait=120)
        
        # Extract content
        print("📦 Extracting content...\n")
        
        # 1. Full HTML
        html_content = page.content()
        html_path = f"{output_dir}/{base_name}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"💾 HTML: {html_path} ({len(html_content):,} bytes)")
        
        # 2. Plain text content
        text_content = page.evaluate("() => document.body.innerText")
        text_path = f"{output_dir}/{base_name}.txt"
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(text_content)
        print(f"💾 Text: {text_path} ({len(text_content):,} chars)")
        
        # 3. Structured content extraction
        structured_content = page.evaluate("""
            () => {
                const getText = (selector) => {
                    const elements = document.querySelectorAll(selector);
                    return Array.from(elements).map(el => el.textContent.trim()).filter(t => t);
                };
                
                return {
                    title: document.title,
                    headings: {
                        h1: getText('h1'),
                        h2: getText('h2'),
                        h3: getText('h3')
                    },
                    paragraphs: getText('p').slice(0, 100),
                    lists: getText('li').slice(0, 100),
                    links: Array.from(document.querySelectorAll('a')).map(a => ({
                        text: a.textContent.trim(),
                        href: a.href
                    })).filter(l => l.text).slice(0, 200),
                    meta: Array.from(document.querySelectorAll('meta')).map(m => ({
                        name: m.getAttribute('name') || m.getAttribute('property'),
                        content: m.getAttribute('content')
                    })).filter(m => m.name && m.content)
                };
            }
        """)
        
        structured_path = f"{output_dir}/{base_name}_structured.json"
        with open(structured_path, "w", encoding="utf-8") as f:
            json.dump(structured_content, f, indent=2, ensure_ascii=False)
        print(f"💾 Structured: {structured_path}")
        
        # 4. Metadata
        metadata = {
            "url": url,
            "timestamp": timestamp,
            "title": structured_content['title'],
            "text_length": len(text_content),
            "html_length": len(html_content),
            "element_count": wait_result['snapshot']['elementCount'],
            "wait_result": wait_result,
            "reliability_score": wait_result['success_rate']
        }
        
        metadata_path = f"{output_dir}/{base_name}_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        print(f"💾 Metadata: {metadata_path}")
        
        # 5. Final verification
        print("\n🔍 Final Verification...")
        time.sleep(5)
        
        final_html = page.content()
        final_hash = hashlib.md5(html_content.encode()).hexdigest()
        verification_hash = hashlib.md5(final_html.encode()).hexdigest()
        
        is_stable = (final_hash == verification_hash)
        metadata['verified_stable'] = is_stable
        
        print(f"   Initial hash:  {final_hash}")
        print(f"   Final hash:    {verification_hash}")
        
        if is_stable:
            print(f"   ✅ STABLE - Content unchanged")
        else:
            print(f"   ⚠️  UNSTABLE - Content still changing")
            print(f"   Difference: {abs(len(final_html) - len(html_content))} bytes")
            
            # Save the final version too
            final_html_path = f"{output_dir}/{base_name}_final.html"
            with open(final_html_path, "w", encoding="utf-8") as f:
                f.write(final_html)
            print(f"   💾 Saved final version: {final_html_path}")
        
        # Update metadata
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        browser.close()
        
        print(f"\n{'='*70}")
        print(f"✅ COMPLETE")
        print(f"{'='*70}")
        print(f"📊 Summary:")
        print(f"   Duration: {wait_result['elapsed']:.1f}s")
        print(f"   Reliability: {wait_result['success_rate']*100:.1f}%")
        print(f"   Verified Stable: {'YES' if is_stable else 'NO'}")
        print(f"   Content Size: {len(text_content):,} chars")
        print(f"{'='*70}\n")
        
        return {
            "html": html_path,
            "text": text_path,
            "structured": structured_path,
            "metadata": metadata_path,
            "verified_stable": is_stable,
            "reliability": wait_result['success_rate']
        }

# Usage
if __name__ == "__main__":
    result = save_content_reliable("https://news.ycombinator.com")
    print(f"✅ Saved to: {result['html']}")
    print(f"   Reliability: {result['reliability']*100:.1f}%")
    print(f"   Stable: {result['verified_stable']}")