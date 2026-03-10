from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


class StrictModel(BaseModel):
    """Base model that rejects unknown input fields."""

    model_config = ConfigDict(extra="forbid")


class Config(StrictModel):
    """Runtime configuration."""

    data_path: str
    product_catalog_path: str


class Shop(StrictModel):
    """Shop dictionary entity used by all pricing relations."""

    id: int
    name: str = Field(min_length=1)
    is_active: bool = True


class ShopUrl(StrictModel):
    """Single product URL in a specific shop."""

    shop_id: int
    url: HttpUrl
    is_active: bool = True


class Category(StrictModel):
    """Product category from the static catalog."""

    id: int
    name: str = Field(min_length=1)


class Product(StrictModel):
    """Product from the static catalog."""

    id: int
    name: str = Field(min_length=1)
    category_ids: list[int] = Field(default_factory=list)
    urls: list[ShopUrl] = Field(default_factory=list)

    @model_validator(mode="after")
    def ensure_single_url_per_shop(self) -> "Product":
        """
        Protect against multiple URLs for the same shop in one product.
        """
        seen_shops: set[int] = set()
        for item in self.urls:
            if item.shop_id in seen_shops:
                raise ValueError(
                    f"Duplicate shop_id '{item.shop_id}' in product '{self.name}' URLs"
                )
            seen_shops.add(item.shop_id)
        return self


class CatalogData(StrictModel):
    """Root model containing static categories and products."""

    shops: list[Shop] = Field(default_factory=list)
    categories: list[Category] = Field(default_factory=list)
    products: list[Product] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_references(self) -> "CatalogData":
        category_ids = {c.id for c in self.categories}
        shop_ids = {s.id for s in self.shops}

        for product in self.products:
            for category_id in product.category_ids:
                if category_id not in category_ids:
                    raise ValueError(
                        f"Product '{product.name}' references unknown category_id={category_id}"
                    )
            for mapping in product.urls:
                if mapping.shop_id not in shop_ids:
                    raise ValueError(
                        f"Product '{product.name}' references unknown shop_id={mapping.shop_id}"
                    )
        return self


class PriceStatus(str, Enum):
    """Result of processing product-shop URL for a given day."""

    SUCCESS = "success"
    FETCH_FAILED = "fetch_failed"
    PARSE_FAILED = "parse_failed"
    OUT_OF_STOCK = "out_of_stock"


class Money(StrictModel):
    """Normalized money value extracted from the source page."""

    amount: Decimal = Field(gt=0)
    currency: str = Field(default="PLN", min_length=3, max_length=3)


class DailyPriceRecord(StrictModel):
    """
    Daily result for one (product, shop) pair.
    This is the core unit for historical analytics.
    """

    date: date
    product_id: int
    shop_id: int
    url: HttpUrl
    status: PriceStatus = PriceStatus.SUCCESS
    price: Optional[Money] = None
    scraped_at: datetime
    raw_price_text: Optional[str] = None
    error: Optional[str] = None
    html_path: Optional[str] = None
    text_path: Optional[str] = None
    metadata_path: Optional[str] = None

    @model_validator(mode="after")
    def validate_price_consistency(self) -> "DailyPriceRecord":
        if self.status == PriceStatus.SUCCESS and self.price is None:
            raise ValueError("price is required when status is 'success'")
        if self.status != PriceStatus.SUCCESS and self.error is None:
            raise ValueError("error is required when status is not 'success'")
        return self


class DailyPriceBatch(StrictModel):
    """
    Full daily calculation output.
    Typically one batch per day (or per rerun) with many records.
    """

    run_id: str = Field(min_length=1)
    calculation_date: date
    started_at: datetime
    finished_at: Optional[datetime] = None
    records: list[DailyPriceRecord] = Field(default_factory=list)

    @model_validator(mode="after")
    def ensure_unique_daily_keys(self) -> "DailyPriceBatch":
        unique_keys: set[tuple[int, int]] = set()
        for record in self.records:
            if record.date != self.calculation_date:
                raise ValueError(
                    "All records in a batch must have date equal to calculation_date"
                )
            key = (record.product_id, record.shop_id)
            if key in unique_keys:
                raise ValueError(
                    f"Duplicate daily record for product_id={record.product_id}, "
                    f"shop_id='{record.shop_id}'"
                )
            unique_keys.add(key)
        return self


@dataclass
class ScrapeSession:
    """
    Runtime timings for one scraping execution.
    Kept as dataclass because scraper mutates this object incrementally.
    """

    start_datetime: datetime
    end_datetime: Optional[datetime] = None
    fetch_start_datetime: Optional[datetime] = None
    fetch_end_datetime: Optional[datetime] = None
    parse_start_datetime: Optional[datetime] = None
    parse_end_datetime: Optional[datetime] = None