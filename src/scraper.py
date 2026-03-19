from playwright.sync_api import sync_playwright
from pathlib import Path
import time
import hashlib
import json
import shutil
import re
import logging
import os
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup
from huggingface_hub import snapshot_download
from transformers import pipeline
from cfg import Configuration
from models import ScrapeSession

DATA_SESSION_FOLDER_DATETIME_FORMAT = "%Y%m%d_%H%M%S"


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
    def content_stable_wait(page, max_wait=120, logger: Optional[logging.Logger] = None):
        """
        Maximum reliability for content only - ignores images
        (same as before - keeping it for completeness)
        """
        active_logger = logger or logging.getLogger("price_fox")
        active_logger.info("🔒 Waiting for content stability...")
        start_time = time.time()
        checks = {}

        # Wait for network idle multiple times
        for attempt in range(3):
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
                checks[f'networkidle_{attempt}'] = True
                active_logger.info(f"  ✓ Network idle (check {attempt + 1}/3)")
                time.sleep(2)
            except:
                checks[f'networkidle_{attempt}'] = False

        # Content stabilization
        active_logger.info("  Checking content stability...")
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
                    active_logger.info(f"  ✓ Content stable ({content_signature})")
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

        active_logger.info(f"  ⏱️  Wait time: {elapsed:.1f}s")
        active_logger.info(f"  ✅ Reliability: {passed}/{total} ({passed / total * 100:.1f}%)")

        return {
            "elapsed": elapsed,
            "checks": checks,
            "success_rate": passed / total,
            "snapshot": snapshot
        }

    @staticmethod
    def save_single_page(page, url, output_dir, browser_session_id, logger: Optional[logging.Logger] = None):
        """
        Saves a single page using an existing page instance
        """
        active_logger = logger or logging.getLogger("price_fox")
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        safe_name = url.replace("https://", "").replace("http://", "").replace("/", "_")[:50]
        base_name = f"{safe_name}_{timestamp}"
        retry_intervals_seconds = [2, 5, 10]
        max_retries = len(retry_intervals_seconds)

        active_logger.info(f"{'=' * 70}")
        active_logger.info(f"🌐 URL: {url}")
        active_logger.info(f"{'=' * 70}")

        try:
            html_content = ""
            text_content = ""
            wait_result = None
            attempt_count = 0

            for attempt in range(max_retries + 1):
                attempt_count = attempt + 1
                active_logger.info(f"Loading page... (attempt {attempt_count}/{max_retries + 1})")
                page.goto(url, wait_until="domcontentloaded", timeout=60000)

                # Wait for content stability
                wait_result = Fetcher.content_stable_wait(page, max_wait=120, logger=active_logger)

                # Extract content
                active_logger.info("📦 Extracting content...")
                html_content = page.content()
                text_content = page.evaluate("() => document.body.innerText")
                text_length = len(text_content.strip())

                if text_length > 0:
                    active_logger.info(f"  ✅ Extracted non-empty text ({text_length:,} chars)")
                    break

                if attempt < max_retries:
                    retry_delay = retry_intervals_seconds[attempt]
                    active_logger.warning(
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
            active_logger.info(f"  💾 HTML: {len(html_content):,} bytes")

            text_path = f"{output_dir}/{base_name}.txt"
            with open(text_path, "w", encoding="utf-8") as f:
                f.write(text_content)
            active_logger.info(f"  💾 Text: {len(text_content):,} chars")

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
                "wait_time": wait_result['elapsed'],
                "fetch_attempts": attempt_count,
            }

            metadata_path = f"{output_dir}/{base_name}_metadata.json"
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            active_logger.info(f"  ✅ Success! Reliability: {wait_result['success_rate'] * 100:.1f}%")

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
            active_logger.error(f"  ❌ Error: {e}")
            return {
                "url": url,
                "status": "failed",
                "error": str(e)
            }

    @staticmethod
    def batch_scrape_optimized(urls, output_dir="batch_scrapes", delay_between_pages=3, logger: Optional[logging.Logger] = None):
        """
        OPTIMIZED: Reuses browser instance for all URLs
        """
        active_logger = logger or logging.getLogger("price_fox")
        Path(output_dir).mkdir(exist_ok=True)

        browser_session_id = time.strftime("%Y%m%d_%H%M%S")
        results = []

        active_logger.info(f"{'=' * 70}")
        active_logger.info("🚀 BATCH SCRAPING - OPTIMIZED MODE")
        active_logger.info(f"{'=' * 70}")
        active_logger.info(f"URLs to process: {len(urls)}")
        active_logger.info(f"Output directory: {output_dir}")
        active_logger.info(f"Delay between pages: {delay_between_pages}s")
        active_logger.info(f"Browser session ID: {browser_session_id}")
        active_logger.info(f"{'=' * 70}")

        with sync_playwright() as p:
            # Create browser ONCE
            active_logger.info("🔧 Launching browser...")
            browser = p.chromium.launch(headless=True)

            # Create context with realistic settings
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )

            # Create page ONCE
            page = context.new_page()
            page.set_default_timeout(120000)

            active_logger.info("✓ Browser ready")

            # Process all URLs with the same browser
            start_time = time.time()

            for i, url in enumerate(urls, 1):
                active_logger.info(f"📄 Processing {i}/{len(urls)}")

                result = Fetcher.save_single_page(
                    page,
                    url,
                    output_dir,
                    browser_session_id,
                    logger=active_logger,
                )
                results.append(result)

                # Delay between pages (be nice to servers)
                if i < len(urls):
                    active_logger.info(f"⏳ Waiting {delay_between_pages}s before next page...")
                    time.sleep(delay_between_pages)

            # Close browser ONCE at the end
            active_logger.info("🔧 Closing browser...")
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

        active_logger.info(f"{'=' * 70}")
        active_logger.info("✅ BATCH COMPLETE")
        active_logger.info(f"{'=' * 70}")
        active_logger.info(f"Total URLs: {summary['total_urls']}")
        active_logger.info(f"Successful: {summary['successful']}")
        active_logger.info(f"Failed: {summary['failed']}")
        active_logger.info(f"Total time: {total_time:.1f}s")
        active_logger.info(f"Avg per URL: {summary['avg_time_per_url']:.1f}s")
        active_logger.info(f"Summary: {summary_path}")
        active_logger.info(f"{'=' * 70}")

        return results

    def _prepare_output_path(self) -> Path:
        """
        Create and return this run's timestamped session output root.
        """
        base_data_root = Path(self.configuration.data_path)
        base_data_root.mkdir(parents=True, exist_ok=True)
        scrape_root = base_data_root / "scrape"
        scrape_root.mkdir(parents=True, exist_ok=True)
        session_folder_name = self.scrape_session.fetch_start_datetime.strftime(
            DATA_SESSION_FOLDER_DATETIME_FORMAT
        )
        session_data_root = scrape_root / session_folder_name
        session_data_root.mkdir(parents=True, exist_ok=True)
        return session_data_root

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
        raw_results = Fetcher.batch_scrape_optimized(
            urls=urls,
            output_dir=str(data_root),
            logger=self.configuration.logger,
        )

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
        self.logger = configuration.logger
        self.model_id = model_id
        self.generator = None
        self.generator_task = None
        self._generator_init_error = None
        self._shop_name_by_id = {
            shop.id: shop.name for shop in self.configuration.product_catalog_data.shops
        }
        self._product_name_by_id = {
            product.id: product.name for product in self.configuration.product_catalog_data.products
        }
        self._url_by_product_shop = {}
        for product in self.configuration.product_catalog_data.products:
            for shop_url in product.urls:
                self._url_by_product_shop[(product.id, shop_url.shop_id)] = str(shop_url.url)
        self._init_generator()

    @staticmethod
    def _enable_hf_offline_mode():
        # Enforce fully-local inference and keep output clean.
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"
        os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"

    @staticmethod
    def _enable_hf_online_mode():
        # Temporarily allow network only for one-time model bootstrap.
        os.environ.pop("HF_HUB_OFFLINE", None)
        os.environ.pop("TRANSFORMERS_OFFLINE", None)
        os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"

    def _is_model_cached_locally(self) -> bool:
        try:
            snapshot_download(
                repo_id=self.model_id,
                local_files_only=True,
            )
            return True
        except Exception:
            return False

    def _ensure_model_available_locally(self):
        if self._is_model_cached_locally():
            self.logger.info(f"Model '{self.model_id}' found in local HF cache.")
            return

        self.logger.warning(
            f"Model '{self.model_id}' not found in local HF cache. Starting one-time download."
        )
        self._enable_hf_online_mode()
        try:
            snapshot_download(
                repo_id=self.model_id,
                resume_download=True,
            )
            self.logger.info(
                f"Model '{self.model_id}' downloaded to local HF cache."
            )
        finally:
            self._enable_hf_offline_mode()

    @staticmethod
    def _is_session_folder_name(folder_name: str) -> bool:
        try:
            datetime.strptime(folder_name, DATA_SESSION_FOLDER_DATETIME_FORMAT)
            return True
        except ValueError:
            return False

    @staticmethod
    def _resolve_data_root(base_data_root: Path) -> Path:
        # Prefer the new fetch layout (`data/scrape/<timestamp>`), but keep
        # backward compatibility with the older (`data/<timestamp>`) structure.
        candidate_roots = [base_data_root / "scrape", base_data_root]
        for root in candidate_roots:
            if not root.exists():
                continue
            session_folders = sorted(
                [
                    folder
                    for folder in root.iterdir()
                    if folder.is_dir() and Parser._is_session_folder_name(folder.name)
                ],
                key=lambda folder: folder.name,
            )
            if session_folders:
                return session_folders[-1]
        return base_data_root

    def _resolve_catalog_context(self, product_id: int, shop_id: int) -> tuple[str, str, Optional[str]]:
        product_name = self._product_name_by_id.get(product_id, f"unknown_product_{product_id}")
        shop_name = self._shop_name_by_id.get(shop_id, f"unknown_shop_{shop_id}")
        url = self._url_by_product_shop.get((product_id, shop_id))
        return product_name, shop_name, url

    def _init_generator(self):
        try:
            self._ensure_model_available_locally()
        except Exception as exc:
            self._generator_init_error = (
                f"Model bootstrap failed for '{self.model_id}': {exc}"
            )
            return

        self._enable_hf_offline_mode()
        tasks = ("text2text-generation", "text-generation")
        last_error = None

        for task in tasks:
            try:
                self.generator = pipeline(
                    task=task,
                    model=self.model_id,
                    tokenizer=self.model_id,
                    local_files_only=True,
                )
                self.generator_task = task
                self._generator_init_error = None
                return
            except Exception as exc:
                last_error = exc

        if last_error is None:
            self._generator_init_error = "Unknown initialization error"
            return
        self._generator_init_error = (
            f"{last_error}. Offline mode is enabled; pre-download '{self.model_id}' "
            "to local cache before running."
        )

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
            '{"price": number|null, "currency": string|null, "raw_price_text": string|null, '
            '"price_type": "product|delivery|old_price|other", "evidence_text": string|null, '
            '"confidence": number}\n'
            "Rules:\n"
            "- Pick the current selling price, not old/discount labels if possible.\n"
            "- Ignore numbers starting with '-', numbers ending with %.\n"
            "- If two candidates are similar, choose shown in larger or visually prominent font and the one that is both higher on page and larger visually.\n"            
            "- Ignore other unrelated offers.\n"            
            "- confidence must be a number from 0 to 1.\n"
            "- If no price is found, use null values and confidence 0.\n\n"
            f"TEXT:\n{text}"
        )

    @staticmethod
    def _normalize_price_type(value: Optional[str]) -> str:
        if not value:
            return "other"
        normalized = str(value).strip().lower().replace("-", "_").replace(" ", "_")
        aliases = {
            "product_price": "product",
            "item": "product",
            "item_price": "product",
            "delivery_price": "delivery",
            "shipping": "delivery",
            "shipping_price": "delivery",
            "oldprice": "old_price",
            "old": "old_price",
            "previous_price": "old_price",
        }
        mapped = aliases.get(normalized, normalized)
        return mapped if mapped in {"product", "delivery", "old_price", "other"} else "other"

    @staticmethod
    def _delivery_markers() -> tuple[str, ...]:
        return (
            "доставка",
            "shipping",
            "delivery",
            "courier",
            "кур'єр",
            "курьер",
            "самовивіз",
            "самовывоз",
            "pickup",
            "пошта",
            "нова пошта",
            "nova poshta",
            "відправка",
            "отправка",
            "postal",
            "postomat",
        )

    @staticmethod
    def _old_price_markers() -> tuple[str, ...]:
        return (
            "стара ціна",
            "старая цена",
            "old price",
            "було",
            "было",
            "before",
            "discount",
            "знижка",
            "скидка",
            "акція",
            "акция",
            "regular price",
            "strike",
        )

    @staticmethod
    def _product_markers() -> tuple[str, ...]:
        return (
            "ціна",
            "цена",
            "price",
            "вартість",
            "вартiсть",
            "грн",
            "uah",
            "₴",
            "buy",
            "купити",
            "в кошик",
            "в корзину",
            "add to cart",
            "in stock",
            "наявн",
            "в наявност",
            "sale price",
            "current price",
        )

    @staticmethod
    def _context_price_type(text: str) -> str:
        lower = text.lower()
        if any(marker in lower for marker in Parser._delivery_markers()):
            return "delivery"
        if any(marker in lower for marker in Parser._old_price_markers()):
            return "old_price"
        return "product"

    @staticmethod
    def _infer_price_type_from_text(line_text: str) -> str:
        return Parser._context_price_type(line_text)

    @staticmethod
    def _to_number(raw_value: str) -> Optional[float]:
        cleaned = (
            raw_value.replace("\u00A0", "")
            .replace(" ", "")
            .replace(",", ".")
            .strip()
        )
        # Ignore discount/offset-style negative values (e.g. "-240.30").
        if cleaned.startswith(("-", "−", "–")):
            return None
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
    def _is_negative_prefixed(text: str, match_start: int) -> bool:
        """
        Detect minus-prefixed numeric token even with separators, e.g. "- | 240.30".
        """
        i = match_start - 1
        while i >= 0 and text[i].isspace():
            i -= 1
        while i >= 0 and text[i] in "|:/;":
            i -= 1
            while i >= 0 and text[i].isspace():
                i -= 1
        return i >= 0 and text[i] in ("-", "−", "–")

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
            "[itemtype*='Product'] [itemprop='price']",
            "meta[itemprop='price']",
            "[itemprop='offers'] [itemprop='price']",
            "[data-price]",
            "[data-product-price]",
            "[data-current-price]",
            "[itemprop='price']",
            ".price",
            ".product-price",
            ".current-price",
        )

        best = None
        number_pattern = re.compile(r"[-−–]?\d{1,6}(?:[ \u00A0]?\d{3})*(?:[.,]\d{1,2})?")

        # Prefer explicit structured product metadata when present.
        for script in soup.select("script[type='application/ld+json']"):
            payload = script.string or script.get_text(" ", strip=True)
            if not payload:
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            objects = parsed if isinstance(parsed, list) else [parsed]
            for item in objects:
                if not isinstance(item, dict):
                    continue
                offers = item.get("offers")
                offers_list = offers if isinstance(offers, list) else [offers]
                for offer in offers_list:
                    if not isinstance(offer, dict):
                        continue
                    raw_price = offer.get("price")
                    if raw_price is None:
                        continue
                    value = Parser._to_number(str(raw_price))
                    if value is None:
                        continue
                    currency = offer.get("priceCurrency")
                    if isinstance(currency, str):
                        currency = currency.strip().upper() or None
                    candidate = {
                        "status": "success",
                        "price": int(value) if float(value).is_integer() else value,
                        "currency": currency,
                        "raw_price_text": str(raw_price),
                        "price_type": "product",
                        "evidence_text": str(offer)[:300],
                        "confidence": 0.97,
                        "provider": "html-heuristic",
                    }
                    if best is None or candidate["confidence"] > best["confidence"]:
                        best = candidate

        for selector in selectors:
            for node in soup.select(selector):
                raw = " ".join(
                    [node.get(key, "") for key in attribute_keys if node.get(key, "")]
                ) or node.get_text(" ", strip=True)
                matches = list(number_pattern.finditer(raw))
                if not matches:
                    continue

                context_parts = [raw]
                parent = node.parent
                if parent is not None:
                    context_parts.append(parent.get_text(" ", strip=True)[:280])
                    parent_attrs = " ".join(
                        [
                            parent.get("class", "") if isinstance(parent.get("class"), str) else " ".join(parent.get("class", [])),
                            parent.get("id", "") or "",
                            parent.get("itemprop", "") or "",
                        ]
                    )
                    context_parts.append(parent_attrs)
                node_attrs = " ".join(
                    [
                        node.get("class", "") if isinstance(node.get("class"), str) else " ".join(node.get("class", [])),
                        node.get("id", "") or "",
                        node.get("itemprop", "") or "",
                        node.get("name", "") or "",
                    ]
                )
                context_parts.append(node_attrs)
                context_text = " ".join(part for part in context_parts if part).strip()
                context_type = Parser._context_price_type(context_text)

                if context_type == "delivery":
                    continue

                for match in matches:
                    raw_number = match.group(0).strip()
                    if Parser._is_negative_prefixed(raw, match.start()):
                        continue
                    if raw_number.startswith(("-", "−", "–")):
                        continue
                    value = Parser._to_number(raw_number)
                    if value is None:
                        continue

                    confidence = 0.83
                    lower_context = context_text.lower()
                    if "itemprop" in lower_context and "price" in lower_context:
                        confidence += 0.08
                    if any(marker in lower_context for marker in Parser._product_markers()):
                        confidence += 0.05
                    if context_type == "old_price":
                        confidence -= 0.2
                    candidate = {
                        "status": "success",
                        "price": int(value) if float(value).is_integer() else value,
                        "currency": Parser._normalize_currency(context_text),
                        "raw_price_text": raw_number,
                        "price_type": "product" if context_type != "old_price" else "old_price",
                        "evidence_text": context_text[:300],
                        "confidence": max(0.0, min(0.95, confidence)),
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
        number_pattern = re.compile(r"[-−–]?\d{1,6}(?:[ \u00A0]?\d{3})*(?:[.,]\d{1,2})?")
        best = None

        for idx, line in enumerate(lines):
            matches = list(number_pattern.finditer(line))
            if not matches:
                continue
            prev_line = lines[idx - 1].strip() if idx > 0 else ""
            if prev_line in {"-", "−", "–"}:
                # Discount deltas are often rendered as a standalone "-" line
                # followed by the numeric amount on the next line.
                continue

            left = max(0, idx - 1)
            right = min(len(lines), idx + 2)
            context = " | ".join(lines[left:right])
            lower = context.lower()
            price_type = Parser._infer_price_type_from_text(context)
            if price_type == "delivery":
                continue
            base_score = 0.35
            if any(word in lower for word in price_words):
                base_score += 0.35
            if Parser._normalize_currency(context) is not None:
                base_score += 0.2
            if "%" in context:
                base_score -= 0.15
            if price_type == "old_price":
                base_score -= 0.25

            for match in matches:
                raw = match.group(0).strip()
                if Parser._is_negative_prefixed(line, match.start()):
                    continue
                if raw.startswith(("-", "−", "–")):
                    continue
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
                    "currency": Parser._normalize_currency(context),
                    "raw_price_text": raw,
                    "price_type": price_type,
                    "evidence_text": context[:300],
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
            "price_type": Parser._normalize_price_type(parsed.get("price_type")),
            "evidence_text": parsed.get("evidence_text"),
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
                "price_type": "other",
                "evidence_text": None,
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
                "price_type": "other",
                "evidence_text": None,
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
                    max_new_tokens=320,
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

            if (
                parsed["price"] is not None
                and parsed.get("price_type") == "product"
                and parsed["confidence"] >= 0.7
            ):
                return parsed

        if best is not None and best.get("price_type") == "product":
            return best

        if best is not None:
            return {
                "status": "failed",
                "price": None,
                "currency": None,
                "raw_price_text": None,
                "price_type": best.get("price_type", "other"),
                "evidence_text": best.get("evidence_text"),
                "confidence": best.get("confidence", 0),
                "provider": "huggingface-local",
                "error": f"Model classified best candidate as non-product price ({best.get('price_type', 'other')})",
            }

        return {
            "status": "failed",
            "price": None,
            "currency": None,
            "raw_price_text": None,
            "price_type": "other",
            "evidence_text": None,
            "confidence": 0,
            "provider": "huggingface-local",
            "error": "Model did not return parseable JSON price output",
            "details": parsing_errors[:3],
        }

    def _parse_single_folder(self, product_id: int, shop_id: int, shop_folder: Path) -> dict:
        parse_started_at_dt = datetime.utcnow()
        parse_started_at = parse_started_at_dt.isoformat()
        source = self._read_text_sources(shop_folder)
        if not source["text"]:
            parse_finished_at_dt = datetime.utcnow()
            parse_finished_at = parse_finished_at_dt.isoformat()
            parse_duration_seconds = (parse_finished_at_dt - parse_started_at_dt).total_seconds()
            result = {
                "status": "failed",
                "product_id": product_id,
                "shop_id": shop_id,
                "price": None,
                "currency": None,
                "raw_price_text": None,
                "price_type": "other",
                "evidence_text": None,
                "confidence": 0,
                "error": "Missing readable HTML/TXT content",
                "model_id": self.model_id,
                "parse_started_at": parse_started_at,
                "parse_finished_at": parse_finished_at,
                "parse_duration_seconds": parse_duration_seconds,
                "parsed_at": parse_finished_at,
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
                "price_type": "other",
                "evidence_text": None,
                "confidence": 0,
                "provider": "huggingface-local",
                "error": f"Local Hugging Face parse failed: {exc}",
            }

        if extracted.get("price") is None or extracted.get("price_type") != "product":
            html_candidate = self._extract_from_html_attributes(source["html_path"])
            text_candidate = self._extract_from_text_candidates(source["text"])
            fallback = None
            if html_candidate is not None and text_candidate is not None:
                fallback = html_candidate if html_candidate["confidence"] >= text_candidate["confidence"] else text_candidate
            else:
                fallback = html_candidate or text_candidate

            if fallback is not None and fallback.get("price_type") == "product":
                extracted = fallback

        parse_finished_at_dt = datetime.utcnow()
        parse_finished_at = parse_finished_at_dt.isoformat()
        parse_duration_seconds = (parse_finished_at_dt - parse_started_at_dt).total_seconds()
        result = {
            "status": extracted.get("status"),
            "product_id": product_id,
            "shop_id": shop_id,
            "price": extracted.get("price"),
            "currency": extracted.get("currency"),
            "raw_price_text": extracted.get("raw_price_text"),
            "price_type": extracted.get("price_type", "other"),
            "evidence_text": extracted.get("evidence_text"),
            "confidence": extracted.get("confidence", 0),
            "provider": extracted.get("provider"),
            "error": extracted.get("error"),
            "model_id": self.model_id,
            "parse_started_at": parse_started_at,
            "parse_finished_at": parse_finished_at,
            "parse_duration_seconds": parse_duration_seconds,
            "parsed_at": parse_finished_at,
            "html_path": source["html_path"],
            "txt_path": source["txt_path"],
        }
        (shop_folder / "parsed.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return result

    def execute(self) -> list[dict]:
        base_data_root = Path(self.configuration.data_path)
        data_root = self._resolve_data_root(base_data_root)
        all_results = []

        for product_folder in sorted([p for p in data_root.iterdir() if p.is_dir()]):
            if not product_folder.name.isdigit():
                continue
            product_id = int(product_folder.name)

            for shop_folder in sorted([s for s in product_folder.iterdir() if s.is_dir()]):
                if not shop_folder.name.isdigit():
                    continue
                shop_id = int(shop_folder.name)
                product_name, shop_name, url = self._resolve_catalog_context(
                    product_id=product_id,
                    shop_id=shop_id,
                )
                self.logger.info(
                    f"Parsing product='{product_name}' (id={product_id}) "
                    f"from shop='{shop_name}' (id={shop_id}), "
                    f"url='{url if url is not None else 'unknown'}'"
                )
                all_results.append(self._parse_single_folder(product_id, shop_id, shop_folder))

        return all_results
