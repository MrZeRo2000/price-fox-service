import pytest
import os

from cfg import CatalogConfig

def test_configuration():
    configuration = CatalogConfig()

    assert configuration.product_catalog_data is not None
    assert len(configuration.product_catalog_data.categories) > 0
    assert len(configuration.product_catalog_data.products) > 0