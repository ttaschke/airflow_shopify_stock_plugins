import json
import unittest
from unittest import mock

from airflow import configuration
from airflow.exceptions import AirflowException

from plugins.operators.shopify_update_stock_csv_operator import (
    ShopifyUpdateStockCsvOperator,
)

PRODUCT_VARIANTS_RESULT = {
    "data": {
        "productVariants": {
            "edges": [
                {
                    "node": {
                        "sku": "SKU1",
                        "inventoryQuantity": 30,
                        "inventoryItem": {"id": "inventory_item_id1"},
                    }
                },
                {
                    "node": {
                        "sku": "SKU2",
                        "inventoryQuantity": 150,
                        "inventoryItem": {"id": "inventory_item_id2"},
                    }
                },
            ]
        }
    },
    "extensions": {"cost": {"requestedQueryCost": 10}},
}


def get_inventory_query(sku_per_request, sku_query):
    query = (
        """
            query {
            productVariants(first: """
        + str(sku_per_request)
        + """, query: \""""
        + sku_query
        + """\") {
                edges {
                cursor
                node {
                    sku
                    inventoryQuantity
                    inventoryItem {
                    id
                    }
                }
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
            }
            }"""
    )
    return query


class TestShopifyUpdateStockCsvOperator(unittest.TestCase):

    def setUp(self):
        configuration.conf.load_test_config()
        self.operator = ShopifyUpdateStockCsvOperator(
            task_id="test_task",
            conn_id="test_conn",
            location_id="test_location",
            file_location="test.csv",
            sku_per_request=100,
            wait_seconds=1,
        )

    @mock.patch(
        "plugins.operators.shopify_update_stock_csv_operator.open",
        new_callable=mock.mock_open,
        read_data="SKU1,10\nSKU2,20\n",
    )
    def test_read_stock_data_success(self, mock_file):
        stock_data = self.operator.read_stock_data()
        expected_stock_data = {"SKU1": 10, "SKU2": 20}
        self.assertEqual(stock_data, expected_stock_data)

    @mock.patch(
        "plugins.operators.shopify_update_stock_csv_operator.open",
        side_effect=Exception("[Errno 2] No such file or directory"),
    )
    def test_read_stock_data_file_not_found(self, mock_file):
        with self.assertRaises(AirflowException) as context:
            self.operator.read_stock_data()

        self.assertIn(
            "Error trying to read file. [Errno 2] No such file or directory",
            str(context.exception),
        )

    def test_get_product_variants_success(self):
        skus = ["SKU1", "SKU2"]
        sku_per_request = self.operator.sku_per_request
        sku_query = "sku:SKU1 OR sku:SKU2"

        sample_response = {
            "data": {
                "productVariants": {
                    "edges": [
                        {
                            "cursor": "cursor1",
                            "node": {
                                "sku": "SKU1",
                                "inventoryQuantity": 100,
                                "inventoryItem": {"id": "inventory_item_id1"},
                            },
                        },
                        {
                            "cursor": "cursor2",
                            "node": {
                                "sku": "SKU2",
                                "inventoryQuantity": 200,
                                "inventoryItem": {"id": "inventory_item_id2"},
                            },
                        },
                    ],
                    "pageInfo": {"hasNextPage": False, "hasPreviousPage": False},
                }
            },
            "extensions": {
                "cost": {
                    "requestedQueryCost": 10,
                    "actualQueryCost": 10,
                    "throttleStatus": {
                        "maximumAvailable": 1000,
                        "currentlyAvailable": 990,
                        "restoreRate": 50,
                    },
                }
            },
        }

        sample_response_json = json.dumps(sample_response)

        mock_client = mock.Mock()
        mock_client.execute.return_value = sample_response_json

        response = self.operator.get_product_variants(mock_client, skus)

        mock_client.execute.assert_called_with(
            get_inventory_query(sku_per_request, sku_query)
        )
        self.assertEqual(response, sample_response)

    def test_get_product_variants_execute_exception(self):
        skus = ["SKU1", "SKU2"]
        sku_per_request = self.operator.sku_per_request
        sku_query = "sku:SKU1 OR sku:SKU2"

        mock_client = mock.Mock()
        mock_client.execute.side_effect = Exception("Client execution error")

        with self.assertRaises(AirflowException) as context:
            self.operator.get_product_variants(mock_client, skus)

        self.assertIn(
            "Error fetching product variants: Client execution error",
            str(context.exception),
        )
        mock_client.execute.assert_called_with(
            get_inventory_query(sku_per_request, sku_query)
        )

    def test_get_product_variants_api_error(self):
        skus = ["SKU1", "SKU2"]
        sku_per_request = self.operator.sku_per_request
        sku_query = "sku:SKU1 OR sku:SKU2"

        mock_client = mock.Mock()

        error_response = {
            "errors": [
                {
                    "message": "Error returned by Shopify API",
                }
            ]
        }
        error_response_json = json.dumps(error_response)
        mock_client.execute.return_value = error_response_json

        with self.assertRaises(AirflowException) as context:
            self.operator.get_product_variants(mock_client, skus)

        self.assertIn(
            "Errors returned by Shopify API for product variants query: [{'message': 'Error returned by Shopify API'}]",
            str(context.exception),
        )
        mock_client.execute.assert_called_with(
            get_inventory_query(sku_per_request, sku_query)
        )

    @mock.patch(
        "plugins.operators.shopify_update_stock_csv_operator.time.sleep",
        return_value=None,
    )
    @mock.patch("plugins.operators.shopify_update_stock_csv_operator.ShopifyHook")
    def test_execute_success(self, mock_shopify_hook, mock_sleep):
        mock_client = mock.Mock()
        mock_shopify_hook.return_value.get_conn.return_value = mock_client

        self.operator.read_stock_data = mock.Mock(
            return_value={"SKU1": 50, "SKU2": 150}
        )

        self.operator.get_product_variants = mock.Mock(
            return_value=PRODUCT_VARIANTS_RESULT
        )

        stock_update_response = {
            "data": {
                "inventoryAdjustQuantities": {
                    "inventoryAdjustmentGroup": {
                        "createdAt": "2025-01-01T00:00:00Z",
                        "reason": "other",
                        "referenceDocumentUri": None,
                        "changes": [
                            {"name": "available", "delta": 2},
                            {"name": "on_hand", "delta": 2},
                        ],
                    },
                }
            },
            "extensions": {
                "cost": {
                    "requestedQueryCost": 11,
                    "actualQueryCost": 11,
                    "throttleStatus": {
                        "maximumAvailable": 2000.0,
                        "currentlyAvailable": 1989,
                        "restoreRate": 100.0,
                    },
                }
            },
        }
        mock_client.execute.return_value = json.dumps(stock_update_response)

        context = {}

        self.operator.execute(context)

        self.operator.read_stock_data.assert_called_once()
        self.operator.get_product_variants.assert_called()
        mock_client.execute.assert_called_once()  # Stock update query
        mock_sleep.assert_called()

    @mock.patch(
        "plugins.operators.shopify_update_stock_csv_operator.time.sleep",
        return_value=None,
    )
    @mock.patch("plugins.operators.shopify_update_stock_csv_operator.ShopifyHook")
    def test_execute_no_stock_changes(self, mock_shopify_hook, mock_sleep):
        mock_client = mock.Mock()
        mock_shopify_hook.return_value.get_conn.return_value = mock_client

        self.operator.read_stock_data = mock.Mock(
            return_value={"SKU1": 30, "SKU2": 150}
        )

        self.operator.get_product_variants = mock.Mock(
            return_value=PRODUCT_VARIANTS_RESULT
        )

        context = {}

        self.operator.execute(context)

        self.operator.read_stock_data.assert_called_once()
        self.operator.get_product_variants.assert_called()
        mock_client.execute.assert_not_called()  # No stock update query
        mock_sleep.assert_called()

    @mock.patch(
        "plugins.operators.shopify_update_stock_csv_operator.time.sleep",
        return_value=None,
    )
    @mock.patch("plugins.operators.shopify_update_stock_csv_operator.ShopifyHook")
    def test_execute_error_updating_stock(self, mock_shopify_hook, mock_sleep):
        mock_client = mock.Mock()
        mock_shopify_hook.return_value.get_conn.return_value = mock_client

        self.operator.read_stock_data = mock.Mock(
            return_value={"SKU1": 50, "SKU2": 150}
        )

        self.operator.get_product_variants = mock.Mock(
            return_value=PRODUCT_VARIANTS_RESULT
        )

        mock_client.execute.side_effect = Exception("Error updating stock")

        context = {}

        with self.assertRaises(AirflowException) as context_manager:
            self.operator.execute(context)

        self.assertIn(
            "Error updating stock: Error updating stock", str(context_manager.exception)
        )
        self.operator.read_stock_data.assert_called_once()
        self.operator.get_product_variants.assert_called()
        mock_client.execute.assert_called()

    @mock.patch(
        "plugins.operators.shopify_update_stock_csv_operator.time.sleep",
        return_value=None,
    )
    @mock.patch("plugins.operators.shopify_update_stock_csv_operator.ShopifyHook")
    def test_execute_dry_run_mode(self, mock_shopify_hook, mock_sleep):
        mock_client = mock.Mock()
        mock_shopify_hook.return_value.get_conn.return_value = mock_client

        operator_dry_run = ShopifyUpdateStockCsvOperator(
            task_id="test_task",
            conn_id="test_conn",
            location_id="test_location",
            file_location="test.csv",
            sku_per_request=100,
            wait_seconds=1,
            dry_run=True,
        )
        operator_dry_run.read_stock_data = mock.Mock(
            return_value={"SKU1": 50, "SKU2": 100}
        )

        operator_dry_run.get_product_variants = mock.Mock(
            return_value=PRODUCT_VARIANTS_RESULT
        )

        context = {}

        operator_dry_run.execute(context)

        # Assert that the client does not execute the query
        mock_client.execute.assert_not_called()
