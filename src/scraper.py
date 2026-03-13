from playwright.sync_api import sync_playwright
from pathlib import Path
import time
import hashlib
import json
import shutil
import re
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup
from transformers import pipeline
from cfg import Configuration
from src.models import ScrapeSession


class Scraper:
    def __init__(self, configuration: Configuration):
        self.configuration = configuration
        self.scrape_session = ScrapeSession(start_datetime=datetime.today())

    def execute(self):
        fetcher = Fetcher(self.configuration, self.scrape_session)
        fetch_results = fetcher.execute()

        parser = Parser(self.configuration)
        parse_results = parser.execute()

        self.scrape_session.end_datetime = datetime.today()
        return {
            "fetch_results": fetch_results,
            "parse_results": parse_results,
        }

class Fetcher:
    def __init__(self, configuration: Configuration, scrape_session: ScrapeSession):
        self.configuration = configuration
        self.scrape_session = scrape_session

    @staticmethod
    def content_stable_wait(page, max_wait=120):
        """
        Maximum reliability for content only - ignores images
        (same as before - keeping it for completeness)
        """
        print("🔒 Waiting for content stability...\n")
        start_time = time.time()
        checks = {}

        # Wait for network idle multiple times
        for attempt in range(3):
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
                checks[f'networkidle_{attempt}'] = True
                print(f"  ✓ Network idle (check {attempt + 1}/3)")
                time.sleep(2)
            except:
                checks[f'networkidle_{attempt}'] = False

        # Content stabilization
        print("  Checking content stability...")
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
                    print(f"  ✓ Content stable ({content_signature})")
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

        print(f"  ⏱️  Wait time: {elapsed:.1f}s")
        print(f"  ✅ Reliability: {passed}/{total} ({passed / total * 100:.1f}%)\n")

        return {
            "elapsed": elapsed,
            "checks": checks,
            "success_rate": passed / total,
            "snapshot": snapshot
        }

    @staticmethod
    def save_single_page(page, url, output_dir, browser_session_id):
        """
        Saves a single page using an existing page instance
        """
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        safe_name = url.replace("https://", "").replace("http://", "").replace("/", "_")[:50]
        base_name = f"{safe_name}_{timestamp}"

        print(f"{'=' * 70}")
        print(f"🌐 URL: {url}")
        print(f"{'=' * 70}\n")

        try:
            # Navigate
            print("Loading page...")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # Wait for content stability
            wait_result = Fetcher.content_stable_wait(page, max_wait=120)

            # Extract content
            print("📦 Extracting content...")

            # 1. Full HTML
            html_content = page.content()
            html_path = f"{output_dir}/{base_name}.html"
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            print(f"  💾 HTML: {len(html_content):,} bytes")

            # 2. Plain text
            text_content = page.evaluate("() => document.body.innerText")
            text_path = f"{output_dir}/{base_name}.txt"
            with open(text_path, "w", encoding="utf-8") as f:
                f.write(text_content)
            print(f"  💾 Text: {len(text_content):,} chars")

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

            print(f"  ✅ Success! Reliability: {wait_result['success_rate'] * 100:.1f}%\n")

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
            print(f"  ❌ Error: {e}\n")
            return {
                "url": url,
                "status": "failed",
                "error": str(e)
            }

    @staticmethod
    def batch_scrape_optimized(urls, output_dir="batch_scrapes", delay_between_pages=3):
        """
        OPTIMIZED: Reuses browser instance for all URLs
        """
        Path(output_dir).mkdir(exist_ok=True)

        browser_session_id = time.strftime("%Y%m%d_%H%M%S")
        results = []

        print(f"\n{'=' * 70}")
        print(f"🚀 BATCH SCRAPING - OPTIMIZED MODE")
        print(f"{'=' * 70}")
        print(f"URLs to process: {len(urls)}")
        print(f"Output directory: {output_dir}")
        print(f"Delay between pages: {delay_between_pages}s")
        print(f"Browser session ID: {browser_session_id}")
        print(f"{'=' * 70}\n")

        with sync_playwright() as p:
            # Create browser ONCE
            print("🔧 Launching browser...")
            browser = p.chromium.launch(headless=True)

            # Create context with realistic settings
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )

            # Create page ONCE
            page = context.new_page()
            page.set_default_timeout(120000)

            print(f"✓ Browser ready\n")

            # Process all URLs with the same browser
            start_time = time.time()

            for i, url in enumerate(urls, 1):
                print(f"📄 Processing {i}/{len(urls)}")

                result = Fetcher.save_single_page(page, url, output_dir, browser_session_id)
                results.append(result)

                # Delay between pages (be nice to servers)
                if i < len(urls):
                    print(f"⏳ Waiting {delay_between_pages}s before next page...\n")
                    time.sleep(delay_between_pages)

            # Close browser ONCE at the end
            print("🔧 Closing browser...")
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

        print(f"\n{'=' * 70}")
        print(f"✅ BATCH COMPLETE")
        print(f"{'=' * 70}")
        print(f"Total URLs: {summary['total_urls']}")
        print(f"Successful: {summary['successful']}")
        print(f"Failed: {summary['failed']}")
        print(f"Total time: {total_time:.1f}s")
        print(f"Avg per URL: {summary['avg_time_per_url']:.1f}s")
        print(f"Summary: {summary_path}")
        print(f"{'=' * 70}\n")

        return results

    def _prepare_output_path(self) -> Path:
        """
        Remove previous scrape artifacts and return clean data root.
        """
        data_root = Path(self.configuration.data_path)
        data_root.mkdir(parents=True, exist_ok=True)

        for item in data_root.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

        return data_root

    @staticmethod
    def _product_shop_output_dir(data_root: Path, product_id: int, shop_id: int) -> Path:
        output_dir = data_root / str(product_id) / str(shop_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    @staticmethod
    def _place_result_into_product_shop_folder(result: dict, output_dir: Path) -> dict:
        if result.get("status") != "success":
            return result

        html_target = output_dir / "page.html"
        text_target = output_dir / "page.txt"
        metadata_target = output_dir / "metadata.json"

        shutil.move(result["html"], html_target)
        shutil.move(result["text"], text_target)
        shutil.move(result["metadata"], metadata_target)

        result["html"] = str(html_target)
        result["text"] = str(text_target)
        result["metadata"] = str(metadata_target)
        return result

    def execute(self):
        self.scrape_session.fetch_start_datetime = datetime.today()
        data_root = self._prepare_output_path()
        jobs = [
            {
                "product_id": product.id,
                "shop_id": shop_url.shop_id,
                "url": str(shop_url.url),
            }
            for product in self.configuration.product_catalog_data.products
            for shop_url in product.urls
        ]

        if not jobs:
            self.scrape_session.fetch_end_datetime = datetime.today()
            return []

        urls = [job["url"] for job in jobs]
        raw_results = Fetcher.batch_scrape_optimized(urls=urls, output_dir=str(data_root))

        all_results = []
        for job, result in zip(jobs, raw_results):
            output_dir = self._product_shop_output_dir(
                data_root=data_root,
                product_id=job["product_id"],
                shop_id=job["shop_id"],
            )
            placed_result = self._place_result_into_product_shop_folder(result, output_dir)
            all_results.append({
                "product_id": job["product_id"],
                "shop_id": job["shop_id"],
                "result": placed_result,
            })

        # Keep data root organized strictly by product_id/shop_id folders.
        for item in data_root.iterdir():
            if item.is_file():
                item.unlink()

        self.scrape_session.fetch_end_datetime = datetime.today()

        return all_results

    def execiute(self):
        """
        Backward-compatible misspelled alias.
        """
        return self.execute()


class Parser:
    """
    Parses fetched page content and extracts product pricing using Hugging Face.
    """

    def __init__(self, configuration: Configuration, model_id: str = "Qwen/Qwen2.5-1.5B-Instruct"):
        self.configuration = configuration
        self.model_id = model_id
        self.generator = None
        self.generator_task = None
        self._generator_init_error = None
        self._init_generator()

    def _init_generator(self):
        tasks = ("text2text-generation", "text-generation")
        last_error = None

        for task in tasks:
            try:
                self.generator = pipeline(
                    task=task,
                    model=self.model_id,
                    tokenizer=self.model_id,
                )
                self.generator_task = task
                self._generator_init_error = None
                return
            except Exception as exc:
                last_error = exc

        self._generator_init_error = str(last_error) if last_error is not None else "Unknown initialization error"

    @staticmethod
    def _find_primary_file(shop_folder: Path, extension: str) -> Optional[Path]:
        canonical = shop_folder / f"page.{extension}"
        if canonical.exists():
            return canonical

        matches = sorted(shop_folder.glob(f"*.{extension}"))
        return matches[0] if matches else None

    @staticmethod
    def _read_text_sources(shop_folder: Path) -> dict:
        html_file = Parser._find_primary_file(shop_folder, "html")
        txt_file = Parser._find_primary_file(shop_folder, "txt")

        html_text = ""
        page_text = ""

        if html_file is not None:
            raw_html = html_file.read_text(encoding="utf-8", errors="ignore")
            soup = BeautifulSoup(raw_html, "lxml")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            html_text = soup.get_text(separator="\n", strip=True)

        if txt_file is not None:
            page_text = txt_file.read_text(encoding="utf-8", errors="ignore")

        combined_text = page_text if page_text.strip() else html_text
        combined_text = re.sub(r"[ \t]+", " ", combined_text)
        combined_text = re.sub(r"\n{3,}", "\n\n", combined_text).strip()

        return {
            "html_path": str(html_file) if html_file is not None else None,
            "txt_path": str(txt_file) if txt_file is not None else None,
            "text": combined_text,
        }

    @staticmethod
    def _chunk_text(text: str, chunk_size: int = 24000, overlap: int = 2000) -> list[str]:
        if not text:
            return []
        chunks = []
        start = 0
        text_len = len(text)
        while start < text_len:
            end = min(start + chunk_size, text_len)
            chunks.append(text[start:end])
            if end == text_len:
                break
            start = max(end - overlap, start + 1)
        return chunks

    @staticmethod
    def _price_focused_snippets(text: str, max_snippets: int = 8) -> list[str]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return []

        price_keywords = (
            "price",
            "цена",
            "ціна",
            "варт",
            "грн",
            "uah",
            "₴",
            "usd",
            "$",
            "eur",
            "€",
            "pln",
            "zł",
            "cost",
        )

        scored = []
        for idx, line in enumerate(lines):
            lower = line.lower()
            score = sum(1 for word in price_keywords if word in lower)
            if score > 0:
                left = max(0, idx - 2)
                right = min(len(lines), idx + 3)
                snippet = "\n".join(lines[left:right])
                scored.append((score, snippet))

        scored.sort(key=lambda item: item[0], reverse=True)
        unique = []
        seen = set()
        for _, snippet in scored:
            if snippet in seen:
                continue
            seen.add(snippet)
            unique.append(snippet[:2200])
            if len(unique) >= max_snippets:
                break

        return unique

    @staticmethod
    def _build_prompt(text: str) -> str:
        return (
            "You extract product pricing from e-commerce page text.\n"
            "Return only one strict JSON object with fields:\n"
            '{"price": number|null, "currency": string|null, "raw_price_text": string|null, "confidence": number}\n'
            "Rules:\n"
            "- Pick the current selling price, not old/discount labels if possible.\n"
            "- confidence must be a number from 0 to 1.\n"
            "- If no price is found, use null values and confidence 0.\n\n"
            f"TEXT:\n{text}"
        )

    @staticmethod
    def _to_number(raw_value: str) -> Optional[float]:
        cleaned = (
            raw_value.replace("\u00A0", "")
            .replace(" ", "")
            .replace(",", ".")
            .strip()
        )
        filtered = re.sub(r"[^0-9.]", "", cleaned)
        if not filtered or filtered.count(".") > 1:
            return None
        try:
            value = float(filtered)
        except ValueError:
            return None
        if value <= 0 or value > 1_000_000:
            return None
        return value

    @staticmethod
    def _normalize_currency(text: str) -> Optional[str]:
        lower = text.lower()
        if "грн" in lower or "₴" in text or "uah" in lower:
            return "UAH"
        if "zł" in lower or "pln" in lower:
            return "PLN"
        if "€" in text or "eur" in lower:
            return "EUR"
        if "$" in text or "usd" in lower:
            return "USD"
        return None

    @staticmethod
    def _extract_from_html_attributes(html_path: Optional[str]) -> Optional[dict]:
        if not html_path:
            return None

        path = Path(html_path)
        if not path.exists():
            return None

        soup = BeautifulSoup(path.read_text(encoding="utf-8", errors="ignore"), "lxml")
        attribute_keys = (
            "content",
            "data-price",
            "data-product-price",
            "data-current-price",
            "value",
        )
        selectors = (
            "meta[itemprop='price']",
            "[data-price]",
            "[data-product-price]",
            "[data-current-price]",
            "[itemprop='price']",
            ".price",
            ".product-price",
            ".current-price",
        )

        best = None
        for selector in selectors:
            for node in soup.select(selector):
                raw = " ".join(
                    [node.get(key, "") for key in attribute_keys if node.get(key, "")]
                ) or node.get_text(" ", strip=True)
                match = re.search(r"\d{1,6}(?:[ \u00A0]?\d{3})*(?:[.,]\d{1,2})?", raw)
                if not match:
                    continue
                value = Parser._to_number(match.group(0))
                if value is None:
                    continue
                candidate = {
                    "status": "success",
                    "price": int(value) if float(value).is_integer() else value,
                    "currency": Parser._normalize_currency(raw),
                    "raw_price_text": match.group(0),
                    "confidence": 0.92,
                    "provider": "html-heuristic",
                }
                if best is None or candidate["confidence"] > best["confidence"]:
                    best = candidate
        return best

    @staticmethod
    def _extract_from_text_candidates(text: str) -> Optional[dict]:
        if not text:
            return None

        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return None

        price_words = ("ціна", "цена", "price", "варт", "грн", "uah", "₴", "sale", "акц")
        number_pattern = re.compile(r"\d{1,6}(?:[ \u00A0]?\d{3})*(?:[.,]\d{1,2})?")
        best = None

        for line in lines:
            matches = number_pattern.findall(line)
            if not matches:
                continue

            lower = line.lower()
            base_score = 0.35
            if any(word in lower for word in price_words):
                base_score += 0.35
            if Parser._normalize_currency(line) is not None:
                base_score += 0.2
            if "%" in line:
                base_score -= 0.15

            for raw in matches:
                value = Parser._to_number(raw)
                if value is None:
                    continue
                if value > 100_000:
                    continue

                score = base_score
                if 10 <= value <= 20_000:
                    score += 0.1

                candidate = {
                    "status": "success",
                    "price": int(value) if float(value).is_integer() else value,
                    "currency": Parser._normalize_currency(line),
                    "raw_price_text": raw,
                    "confidence": min(score, 0.89),
                    "provider": "text-heuristic",
                }
                if best is None or candidate["confidence"] > best["confidence"]:
                    best = candidate

        return best

    @staticmethod
    def _safe_json_from_response(response_text: str) -> Optional[dict]:
        raw = response_text.strip()
        fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.DOTALL)
        if fenced:
            raw = fenced.group(1)
        else:
            block = re.search(r"\{.*\}", raw, flags=re.DOTALL)
            if block:
                raw = block.group(0)
            else:
                return None

        parsed = json.loads(raw)
        confidence = parsed.get("confidence", 0)
        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            confidence = 0

        return {
            "status": "success" if parsed.get("price") is not None else "failed",
            "price": parsed.get("price"),
            "currency": parsed.get("currency"),
            "raw_price_text": parsed.get("raw_price_text"),
            "confidence": max(0.0, min(1.0, confidence)),
            "provider": "huggingface-local",
        }

    def _extract_price_with_hf(self, text: str) -> dict:
        if self.generator is None:
            return {
                "status": "failed",
                "price": None,
                "currency": None,
                "raw_price_text": None,
                "confidence": 0,
                "provider": "huggingface-local",
                "error": f"Local model initialization failed: {self._generator_init_error}",
            }

        snippets = self._price_focused_snippets(text)
        chunks = self._chunk_text(text, chunk_size=24000, overlap=2000)
        candidates = snippets + chunks[:6]

        if not candidates:
            return {
                "status": "failed",
                "price": None,
                "currency": None,
                "raw_price_text": None,
                "confidence": 0,
                "provider": "huggingface-local",
                "error": "No text content to parse",
            }

        best = None
        parsing_errors = []

        for candidate in candidates:
            prompt = self._build_prompt(candidate)
            try:
                generated = self.generator(
                    prompt,
                    max_new_tokens=160,
                    do_sample=False,
                    return_full_text=False,
                )
                response_text = generated[0]["generated_text"]
            except Exception as exc:
                parsing_errors.append(str(exc))
                continue
            try:
                parsed = self._safe_json_from_response(response_text)
            except Exception as exc:
                parsing_errors.append(str(exc))
                continue

            if parsed is None:
                continue

            if best is None or parsed["confidence"] > best["confidence"]:
                best = parsed

            if parsed["price"] is not None and parsed["confidence"] >= 0.7:
                return parsed

        if best is not None:
            return best

        return {
            "status": "failed",
            "price": None,
            "currency": None,
            "raw_price_text": None,
            "confidence": 0,
            "provider": "huggingface-local",
            "error": "Model did not return parseable JSON price output",
            "details": parsing_errors[:3],
        }

    def _parse_single_folder(self, product_id: int, shop_id: int, shop_folder: Path) -> dict:
        source = self._read_text_sources(shop_folder)
        if not source["text"]:
            result = {
                "status": "failed",
                "product_id": product_id,
                "shop_id": shop_id,
                "price": None,
                "currency": None,
                "raw_price_text": None,
                "confidence": 0,
                "error": "Missing readable HTML/TXT content",
                "model_id": self.model_id,
                "parsed_at": datetime.utcnow().isoformat(),
                "html_path": source["html_path"],
                "txt_path": source["txt_path"],
            }
            (shop_folder / "parsed.json").write_text(
                json.dumps(result, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            return result

        try:
            extracted = self._extract_price_with_hf(source["text"])
        except Exception as exc:
            extracted = {
                "status": "failed",
                "price": None,
                "currency": None,
                "raw_price_text": None,
                "confidence": 0,
                "provider": "huggingface-local",
                "error": f"Local Hugging Face parse failed: {exc}",
            }

        if extracted.get("price") is None:
            html_candidate = self._extract_from_html_attributes(source["html_path"])
            text_candidate = self._extract_from_text_candidates(source["text"])
            fallback = None
            if html_candidate is not None and text_candidate is not None:
                fallback = html_candidate if html_candidate["confidence"] >= text_candidate["confidence"] else text_candidate
            else:
                fallback = html_candidate or text_candidate

            if fallback is not None:
                extracted = fallback

        result = {
            "status": extracted.get("status"),
            "product_id": product_id,
            "shop_id": shop_id,
            "price": extracted.get("price"),
            "currency": extracted.get("currency"),
            "raw_price_text": extracted.get("raw_price_text"),
            "confidence": extracted.get("confidence", 0),
            "provider": extracted.get("provider"),
            "error": extracted.get("error"),
            "model_id": self.model_id,
            "parsed_at": datetime.utcnow().isoformat(),
            "html_path": source["html_path"],
            "txt_path": source["txt_path"],
        }
        (shop_folder / "parsed.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return result

    def execute(self) -> list[dict]:
        data_root = Path(self.configuration.data_path)
        all_results = []

        for product_folder in sorted([p for p in data_root.iterdir() if p.is_dir()]):
            if not product_folder.name.isdigit():
                continue
            product_id = int(product_folder.name)

            for shop_folder in sorted([s for s in product_folder.iterdir() if s.is_dir()]):
                if not shop_folder.name.isdigit():
                    continue
                shop_id = int(shop_folder.name)
                all_results.append(self._parse_single_folder(product_id, shop_id, shop_folder))

        return all_results
