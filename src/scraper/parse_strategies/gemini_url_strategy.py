import json
import re
import time
from typing import Optional
from urllib import error as urllib_error
from urllib import request as urllib_request


class GeminiUrlParseStrategy:
    def __init__(self, strategy_settings: dict[str, str], logger):
        self._strategy_settings = strategy_settings
        self._logger = logger
        self._models_logged = False

    @staticmethod
    def _clamp_confidence(value) -> float:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            numeric = 0.0
        return max(0.0, min(1.0, numeric))

    @staticmethod
    def _safe_json_object_from_text(response_text: str) -> Optional[dict]:
        raw = (response_text or "").strip()
        if not raw:
            return None
        fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.DOTALL)
        if fenced:
            raw = fenced.group(1)
        else:
            block = re.search(r"\{.*\}", raw, flags=re.DOTALL)
            if block:
                raw = block.group(0)
        try:
            parsed = json.loads(raw)
        except Exception:
            return None
        return parsed if isinstance(parsed, dict) else None

    @staticmethod
    def _extract_gemini_text_response(payload: dict) -> str:
        candidates = payload.get("candidates", [])
        if not candidates:
            return ""
        candidate = candidates[0] if isinstance(candidates[0], dict) else {}
        content = candidate.get("content", {})
        parts = content.get("parts", []) if isinstance(content, dict) else []
        text_parts = []
        for part in parts:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                text_parts.append(part["text"])
        return "\n".join(text_parts).strip()

    @staticmethod
    def _extract_retry_seconds_from_error_message(error_message: str) -> Optional[float]:
        if not error_message:
            return None
        match = re.search(
            r"please\s+retry\s+in\s+([0-9]+(?:\.[0-9]+)?)s",
            error_message,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        try:
            seconds = float(match.group(1))
        except Exception:
            return None
        return seconds if seconds >= 0 else None

    @staticmethod
    def _to_number(raw_value: str) -> Optional[float]:
        cleaned = (
            raw_value.replace("\u00A0", "")
            .replace(" ", "")
            .replace(",", ".")
            .strip()
        )
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
    def _build_failed_result(reason: str) -> dict:
        return {
            "status": "failed",
            "price": None,
            "currency": None,
            "raw_price_text": None,
            "price_type": "other",
            "evidence_text": None,
            "confidence": 0.0,
            "provider": "gemini-url",
            "error": reason,
        }

    def _log_models_once(self, api_key: str, timeout_seconds: int) -> None:
        if self._models_logged:
            return
        endpoint = "https://generativelanguage.googleapis.com/v1beta/models"
        request = urllib_request.Request(
            endpoint,
            headers={
                "Content-Type": "application/json",
                "X-goog-api-key": api_key,
            },
            method="GET",
        )
        try:
            with urllib_request.urlopen(request, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        except urllib_error.HTTPError as exc:
            try:
                error_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                error_body = ""
            self._logger.warning(
                "Gemini ListModels HTTP error: status=%s reason=%s endpoint=%s body=%s",
                exc.code,
                exc.reason,
                endpoint,
                error_body[:1200] if error_body else "",
            )
            self._models_logged = True
            return
        except Exception as exc:
            self._logger.warning(
                "Gemini ListModels request failed: endpoint=%s error=%s",
                endpoint,
                exc,
            )
            self._models_logged = True
            return

        models = payload.get("models", [])
        self._logger.info("Gemini ListModels: total_models=%s", len(models))
        for model in models:
            if not isinstance(model, dict):
                continue
            model_name = model.get("name") or model.get("displayName") or "unknown"
            methods = model.get("supportedGenerationMethods", [])
            self._logger.info(
                "Gemini model: name=%s supported_methods=%s",
                model_name,
                methods,
            )
        self._models_logged = True

    def extract_price_from_url(self, url: Optional[str]) -> dict:
        if not url:
            return self._build_failed_result("Gemini strategy selected but URL is missing")

        api_key = self._strategy_settings.get("gemini_api_key", "").strip()
        if not api_key:
            return self._build_failed_result(
                "Gemini strategy selected but gemini_api_key is missing in strategy_settings"
            )

        model_name = self._strategy_settings.get("gemini_model", "gemini-2.0-flash").strip()
        if not model_name:
            model_name = "gemini-2.0-flash"
        timeout_raw = self._strategy_settings.get("gemini_timeout_seconds", "45").strip()
        try:
            timeout_seconds = max(10, int(timeout_raw))
        except Exception:
            timeout_seconds = 45

        self._log_models_once(api_key=api_key, timeout_seconds=timeout_seconds)

        prompt = (
            f"get price from page: {url}\n"
            'Provide response in json like: {"price": 123.12, "confidence": 0.6}, '
            "where confidence reflects reliability, from 0 to 1."
        )
        endpoint = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{model_name}:generateContent"
        )
        body = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "tools": [{"google_search_retrieval": {}}],
            "generationConfig": {
                "temperature": 0,
                "responseMimeType": "application/json",
            },
        }
        request_body_json = json.dumps(body, ensure_ascii=False)
        request_headers = {
            "Content-Type": "application/json",
            "X-goog-api-key": api_key,
        }
        request_headers_for_log = {
            "Content-Type": "application/json",
            "X-goog-api-key": "***",
        }
        request = urllib_request.Request(
            endpoint,
            data=request_body_json.encode("utf-8"),
            headers=request_headers,
            method="POST",
        )
        self._logger.info(
            "Gemini request: model=%s timeout=%ss endpoint=%s",
            model_name,
            timeout_seconds,
            endpoint,
        )
        self._logger.info(
            "Gemini request headers: %s",
            json.dumps(request_headers_for_log, ensure_ascii=False),
        )
        self._logger.info("Gemini request body: %s", request_body_json)

        retry_delays_seconds = [3, 10, 20]
        last_error_result = None
        payload = None
        for attempt in range(len(retry_delays_seconds) + 1):
            try:
                with urllib_request.urlopen(request, timeout=timeout_seconds) as response:
                    payload = json.loads(response.read().decode("utf-8", errors="replace"))
                break
            except urllib_error.HTTPError as exc:
                try:
                    error_body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    error_body = ""
                error_body_snippet = error_body[:1200] if error_body else ""
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                self._logger.error(
                    "Gemini HTTP error: status=%s reason=%s retry_after=%s endpoint=%s body=%s",
                    exc.code,
                    exc.reason,
                    retry_after,
                    endpoint,
                    error_body_snippet,
                )
                last_error_result = {
                    "status": "failed",
                    "price": None,
                    "currency": None,
                    "raw_price_text": None,
                    "price_type": "other",
                    "evidence_text": None,
                    "confidence": 0.0,
                    "provider": "gemini-url",
                    "error": f"Gemini HTTP {exc.code}: {exc.reason}",
                    "details": {
                        "model": model_name,
                        "endpoint": endpoint,
                        "retry_after": retry_after,
                        "response_body": error_body_snippet,
                        "attempt": attempt + 1,
                    },
                }
                is_retryable_429 = exc.code == 429 and attempt < len(retry_delays_seconds)
                if is_retryable_429:
                    hint_seconds = self._extract_retry_seconds_from_error_message(error_body)
                    if hint_seconds is not None:
                        delay = hint_seconds + 1.0
                    else:
                        delay = float(retry_delays_seconds[attempt])
                    self._logger.warning(
                        "Gemini HTTP 429 detected. Retrying in %.3fs (attempt %s/%s)...",
                        delay,
                        attempt + 1,
                        len(retry_delays_seconds) + 1,
                    )
                    time.sleep(delay)
                    continue
                return last_error_result
            except Exception as exc:
                self._logger.error(
                    "Gemini request failed: model=%s endpoint=%s error=%s",
                    model_name,
                    endpoint,
                    exc,
                )
                return {
                    "status": "failed",
                    "price": None,
                    "currency": None,
                    "raw_price_text": None,
                    "price_type": "other",
                    "evidence_text": None,
                    "confidence": 0.0,
                    "provider": "gemini-url",
                    "error": f"Gemini request failed: {exc}",
                    "details": {
                        "model": model_name,
                        "endpoint": endpoint,
                        "attempt": attempt + 1,
                    },
                }
        if payload is None:
            return last_error_result or self._build_failed_result(
                "Gemini request failed with unknown retry flow state"
            )

        response_text = self._extract_gemini_text_response(payload)
        self._logger.info("Gemini raw response text: %s", response_text)
        parsed = self._safe_json_object_from_text(response_text)
        if parsed is None:
            return {
                "status": "failed",
                "price": None,
                "currency": None,
                "raw_price_text": None,
                "price_type": "other",
                "evidence_text": response_text[:300] if response_text else None,
                "confidence": 0.0,
                "provider": "gemini-url",
                "error": "Gemini response is not parseable JSON object",
            }
        self._logger.info(
            "Gemini parsed response JSON: %s",
            json.dumps(parsed, ensure_ascii=False),
        )

        raw_price = parsed.get("price")
        if raw_price is None:
            return {
                "status": "failed",
                "price": None,
                "currency": None,
                "raw_price_text": None,
                "price_type": "other",
                "evidence_text": response_text[:300] if response_text else None,
                "confidence": self._clamp_confidence(parsed.get("confidence", 0)),
                "provider": "gemini-url",
                "error": "Gemini did not return price",
            }

        value = self._to_number(str(raw_price))
        if value is None:
            return {
                "status": "failed",
                "price": None,
                "currency": None,
                "raw_price_text": str(raw_price),
                "price_type": "other",
                "evidence_text": response_text[:300] if response_text else None,
                "confidence": self._clamp_confidence(parsed.get("confidence", 0)),
                "provider": "gemini-url",
                "error": "Gemini price is not a valid positive number",
            }

        return {
            "status": "success",
            "price": int(value) if float(value).is_integer() else value,
            "currency": "UAH",
            "raw_price_text": str(raw_price),
            "price_type": "product",
            "evidence_text": response_text[:300] if response_text else None,
            "confidence": self._clamp_confidence(parsed.get("confidence", 0.8)),
            "provider": "gemini-url",
            "error": None,
        }
