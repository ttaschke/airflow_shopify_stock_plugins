from airflow.plugins_manager import AirflowPlugin

from plugins.hooks.shopify_hook import ShopifyHook
from plugins.operators.shopify_update_stock_csv_operator import (
    ShopifyUpdateStockCsvOperator,
)


class ShopifyStockPlugin(AirflowPlugin):
    name = "shopify_stock_plugin"
    hooks = [ShopifyHook]
    operators = [ShopifyUpdateStockCsvOperator]
