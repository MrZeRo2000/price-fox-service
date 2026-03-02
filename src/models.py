from pydantic import BaseModel, HttpUrl
from typing import List, Optional
from datetime import datetime
import json
from dataclasses import dataclass


class Config(BaseModel):
    """Model for configuration"""
    data_path: str
    product_catalog_path: str

class ShopUrl(BaseModel):
    """Model for shop URL information"""
    shop: str
    url: str

class Category(BaseModel):
    """Model for product category"""
    id: int
    name: str

class Product(BaseModel):
    """Model for product information"""
    id: int
    name: str
    category_ids: List[int]
    urls: List[ShopUrl]


class CatalogData(BaseModel):
    """Root model containing all categories and products"""
    categories: List[Category]
    products: List[Product]

@dataclass
class ScrapeSession:
    start_datetime: datetime
    end_datetime: Optional[datetime] = None
    fetch_start_datetime: Optional[datetime] = None
    fetch_end_datetime: Optional[datetime] = None
    parse_start_datetime: Optional[datetime] = None
    parse_end_datetime: Optional[datetime] = None