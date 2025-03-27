import csv
import json
import time

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.shopify_hook import ShopifyHook


class ShopifyUpdateStockCsvOperator(BaseOperator):
    """
    Operator that takes a CSV file (columns: sku, stock) as input
    and syncs the stock of corresponding product variants in Shopify.
    Works through the list of product variants in adjustable batches using two Shopify API requests each.
    Use batch size and wait time between batches to optimize for performance and avoid API throttling, if needed.
    :param conn_id: Shopify connection
    :param location_id: Shopify location GraphQL API id to define at which location to update the inventory
    :param file_location: File location of the CSV file
    :param sku_per_request: Number of SKU to be used for each batch of Shopify API requests
    :param wait_seconds: Seconds to wait between batches of Shopify API requests
    """

    template_fields = ["file_location"]

    @apply_defaults
    def __init__(
        self,
        conn_id,
        location_id,
        file_location,
        sku_per_request=100,
        wait_seconds=1,
        dry_run=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.location_id = location_id
        self.file_location = file_location
        self.sku_per_request = sku_per_request
        self.wait_seconds = wait_seconds
        self.dry_run = dry_run

    def execute(self, context):
        """Updates product variants (SKU) stock at the given location via the Shopify API"""
        shopify_hook = ShopifyHook(conn_id=self.conn_id)
        client = shopify_hook.get_conn()

        stock_data = self.read_stock_data()

        index = 0

        # Update stock in batches with length of sku_per_request
        while index < len(stock_data):

            # Get product variants result the current batch of skus from the Shopify API
            product_variants_result = self.get_product_variants(
                client, list(stock_data)[index : index + self.sku_per_request]
            )

            # Calculate stock delta and build inventory changes for stock update query
            inventory_changes = ""

            for product_variant in product_variants_result["data"]["productVariants"][
                "edges"
            ]:
                updated_stock = (
                    stock_data[product_variant["node"]["sku"]]
                    - product_variant["node"]["inventoryQuantity"]
                )
                # Skip if no change in stock level
                if updated_stock == 0:
                    continue
                inventory_changes += (
                    '{inventoryItemId: "'
                    + product_variant["node"]["inventoryItem"]["id"]
                    + '", delta: '
                    + str(updated_stock)
                    + ', locationId: "'
                    + self.location_id
                    + '"},'
                )

            # Skip if no changes in any stock level were found
            if len(inventory_changes) == 0:
                self.log.info(
                    "Update stock query skipped, because of no stock level changes for product variants in batch"
                )
                index = index + self.sku_per_request
                time.sleep(self.wait_seconds)
                continue

            # Update stock for product variants using calculated deltas
            stock_update_query = (
                """
                mutation {
                  inventoryAdjustQuantities(input: {
                    reason: "other",
                    name: "available",
                    changes : ["""
                + inventory_changes
                + """]
                  })
                }
            """
            )
            if self.dry_run:
                self.log.info("DRY RUN - Query for batch:")
                self.log.info(stock_update_query)
            else:
                try:
                    response = client.execute(stock_update_query)
                    stock_update_response = json.loads(response)
                except Exception as e:
                    raise AirflowException(f"Error updating stock: {e}")

                if "errors" in stock_update_response:
                    raise AirflowException(
                        f"Errors returned by Shopify API for stock update query: {stock_update_response['errors']}"
                    )

                self.log.info(
                    f"Update stock query cost: {stock_update_response['extensions']}"
                )

            index = index + self.sku_per_request
            time.sleep(self.wait_seconds)

    def read_stock_data(self):
        """Reads stock data from the CSV file specified by `file_location`.
        The CSV file should contain two columns per row: SKU and stock quantity.

        :return: A dictionary mapping SKUs to their corresponding stock quantities.
        :raises AirflowException: If there is an error reading the CSV file.
        """
        try:
            with open(self.file_location) as csv_file:
                stock_data = {row[0]: int(row[1]) for row in csv.reader(csv_file)}
                self.log.info(
                    f'Parsed {len(stock_data)} sku from csv file "{self.file_location}"'
                )
                return stock_data
        except Exception as e:
            raise AirflowException(f"Error trying to read file. {e}")

    def get_product_variants(self, client, skus):
        """Retrieves the productVariants API response for a list of SKUs from the Shopify API.

        :param client: The Shopify GraphQL client to use for executing the query.
        :param skus: List of SKUs to retrieve stock information for.
        :return: The response from Shopify containing the product variants.
        """
        sku_query = ""

        for sku in skus:
            if len(sku_query) > 0:
                sku_query += " OR "
            sku_query += f"sku:{sku}"

        inventory_query = (
            """
            query {
            productVariants(first: """
            + str(self.sku_per_request)
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
        try:
            response = client.execute(inventory_query)
            product_variants_result = json.loads(response)
        except Exception as e:
            raise AirflowException(f"Error fetching product variants: {e}")

        if "errors" in product_variants_result:
            raise AirflowException(
                f"Errors returned by Shopify API for product variants query: {product_variants_result['errors']}"
            )

        self.log.info(
            f"Inventory query cost:    {product_variants_result.get('extensions', {})}"
        )
        return product_variants_result
