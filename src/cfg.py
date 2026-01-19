import os

class Configuration:
    """Class for configuration"""
    def __init__(self):
        data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/"))
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config/"))

        if not os.path.exists(data_path):
            raise ValueError(f"Data path {data_path} does not exist")
        if not os.path.exists(config_path):
            raise ValueError(f"Config path {data_path} does not exist")

        product_catalog_path = os.path.abspath(os.path.join(config_path, "product-catalog.json"))
        if not os.path.exists(product_catalog_path):
            raise ValueError(f"Product catalog path {product_catalog_path} does not exist")
