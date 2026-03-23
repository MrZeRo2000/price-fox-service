import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup
from huggingface_hub import snapshot_download
from transformers import pipeline

from cfg import Configuration
from session import resolve_parser_data_root


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
        self._product_name_by_id = {
            product.id: product.name for product in self.configuration.product_catalog_data.products
        }
        self._url_by_id = {
            item.url_id: str(item.url)
            for item in self.configuration.product_catalog_data.urls
        }
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

        allow_download = os.environ.get("PRICE_FOX_ALLOW_MODEL_DOWNLOAD", "0").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        if not allow_download:
            raise RuntimeError(
                f"Model '{self.model_id}' is not present in local cache and automatic download is disabled. "
                "Set PRICE_FOX_ALLOW_MODEL_DOWNLOAD=1 to enable one-time model download."
            )

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
    def _resolve_data_root(base_data_root: Path) -> Path:
        return resolve_parser_data_root(base_data_root)

    def _resolve_catalog_context(
        self, product_id: int, url_id: int
    ) -> tuple[str, Optional[str]]:
        product_name = self._product_name_by_id.get(
            product_id, f"unknown_product_{product_id}"
        )
        url = self._url_by_id.get(url_id)
        return product_name, url

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
    def _find_primary_file(url_folder: Path, extension: str) -> Optional[Path]:
        canonical = url_folder / f"page.{extension}"
        if canonical.exists():
            return canonical

        matches = sorted(url_folder.glob(f"*.{extension}"))
        return matches[0] if matches else None

    @staticmethod
    def _read_text_sources(url_folder: Path) -> dict:
        html_file = Parser._find_primary_file(url_folder, "html")
        txt_file = Parser._find_primary_file(url_folder, "txt")

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
    def _has_adjacent_currency(text: str, match_start: int, match_end: int) -> bool:
        left = text[max(0, match_start - 8):match_start]
        right = text[match_end:min(len(text), match_end + 12)]
        probe = f"{left} {right}".lower()
        return bool(
            re.search(r"(грн|₴|uah|usd|eur|pln|zł|\$|€)", probe, flags=re.IGNORECASE)
        )

    @staticmethod
    def _is_measurement_amount(text: str, match_start: int, match_end: int) -> bool:
        # Ignore package-size numbers like "100 мл", "50 g", etc.
        right = text[match_end:min(len(text), match_end + 14)].lower()
        return bool(
            re.search(
                r"^\s*(мл|ml|л|l|г|гр|g|kg|кг|oz|унц|шт|pcs|табл|капсул|pack)\b",
                right,
                flags=re.IGNORECASE,
            )
        )

    @staticmethod
    def _is_time_like_token(text: str, match_start: int, match_end: int) -> bool:
        """
        Ignore time-like fragments such as "17:00" that regex can split into "17".
        """
        left = text[max(0, match_start - 3):match_start]
        center = text[match_start:match_end]
        right = text[match_end:min(len(text), match_end + 4)]
        probe = f"{left}{center}{right}"
        return bool(re.search(r"\b\d{1,2}:\d{2}\b", probe))

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
                raw = (
                    " ".join(
                        [node.get(key, "") for key in attribute_keys if node.get(key, "")]
                    )
                    or node.get_text(" ", strip=True)
                )
                matches = list(number_pattern.finditer(raw))
                if not matches:
                    continue

                context_parts = [raw]
                parent = node.parent
                if parent is not None:
                    context_parts.append(parent.get_text(" ", strip=True)[:280])
                    parent_attrs = " ".join(
                        [
                            parent.get("class", "")
                            if isinstance(parent.get("class"), str)
                            else " ".join(parent.get("class", [])),
                            parent.get("id", "") or "",
                            parent.get("itemprop", "") or "",
                        ]
                    )
                    context_parts.append(parent_attrs)
                node_attrs = " ".join(
                    [
                        node.get("class", "")
                        if isinstance(node.get("class"), str)
                        else " ".join(node.get("class", [])),
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
                    if Parser._is_time_like_token(raw, match.start(), match.end()):
                        continue
                    if Parser._is_measurement_amount(raw, match.start(), match.end()):
                        continue
                    value = Parser._to_number(raw_number)
                    if value is None:
                        continue

                    confidence = 0.83
                    lower_context = context_text.lower()
                    if "itemprop" in lower_context and "price" in lower_context:
                        confidence += 0.08
                    if any(
                        marker in lower_context for marker in Parser._product_markers()
                    ):
                        confidence += 0.05
                    if Parser._has_adjacent_currency(raw, match.start(), match.end()):
                        confidence += 0.15
                    if context_type == "old_price":
                        confidence -= 0.2
                    candidate = {
                        "status": "success",
                        "price": int(value) if float(value).is_integer() else value,
                        "currency": Parser._normalize_currency(context_text),
                        "raw_price_text": raw_number,
                        "price_type": "product"
                        if context_type != "old_price"
                        else "old_price",
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

        price_words = (
            "ціна",
            "цена",
            "price",
            "варт",
            "грн",
            "uah",
            "₴",
            "sale",
            "акц",
        )
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
                if Parser._is_time_like_token(line, match.start(), match.end()):
                    continue
                if Parser._is_measurement_amount(line, match.start(), match.end()):
                    continue
                value = Parser._to_number(raw)
                if value is None:
                    continue
                if value > 100_000:
                    continue

                score = base_score
                if 10 <= value <= 20_000:
                    score += 0.1
                if Parser._has_adjacent_currency(line, match.start(), match.end()):
                    score += 0.35

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
            raw_price_text = str(parsed.get("raw_price_text") or "")
            if raw_price_text and ":" in raw_price_text:
                parsed["status"] = "failed"
                parsed["price"] = None
                parsed["confidence"] = 0
                parsed["error"] = "Rejected time-like token in raw_price_text"

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

    def _parse_single_folder(self, product_id: int, url_id: int, url_folder: Path) -> dict:
        parse_started_at_dt = datetime.utcnow()
        parse_started_at = parse_started_at_dt.isoformat()
        source = self._read_text_sources(url_folder)
        if not source["text"]:
            parse_finished_at_dt = datetime.utcnow()
            parse_finished_at = parse_finished_at_dt.isoformat()
            parse_duration_seconds = (
                parse_finished_at_dt - parse_started_at_dt
            ).total_seconds()
            result = {
                "status": "failed",
                "product_id": product_id,
                "url_id": url_id,
                "url": self._url_by_id.get(url_id),
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
            (url_folder / "parsed.json").write_text(
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
            if html_candidate is not None and text_candidate is not None:
                fallback = (
                    html_candidate
                    if html_candidate["confidence"] >= text_candidate["confidence"]
                    else text_candidate
                )
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
            "url_id": url_id,
            "url": self._url_by_id.get(url_id),
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
        (url_folder / "parsed.json").write_text(
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

            for url_folder in sorted([s for s in product_folder.iterdir() if s.is_dir()]):
                if not url_folder.name.isdigit():
                    continue
                url_id = int(url_folder.name)
                product_name, url = self._resolve_catalog_context(
                    product_id=product_id,
                    url_id=url_id,
                )
                self.logger.info(
                    f"Parsing product='{product_name}' (id={product_id}) "
                    f"for url_id={url_id}, "
                    f"url='{url if url is not None else 'unknown'}'"
                )
                all_results.append(
                    self._parse_single_folder(product_id, url_id, url_folder)
                )

        return all_results
