from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import csv
import shopify
import time

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

    template_fields = ['file_location']

    @apply_defaults
    def __init__(self, conn_id, location_id, file_location, sku_per_request=100, wait_seconds=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.location_id = location_id
        self.file_location = file_location
        self.sku_per_request = sku_per_request
        self.wait_seconds = wait_seconds

    def execute(self, context):

        shopify_hook = ShopifyHook(conn_id=self.conn_id)
        client = shopify_hook.get_conn()

        with open(self.file_location) as csv_file:
            stock_data = {row[0]: int(row[1]) for row in csv.reader(csv_file)}

        total_sku = len(stock_data)
        print(f'Parsed {total_sku} sku from csv file "{self.file_location}"')

        index = 0

        while index < total_sku:
            sku_query = ''

            for sku in list(stock_data)[index:index+self.sku_per_request]:
                if len(sku_query) > 0:
                    sku_query += ' OR '
                sku_query += f'sku:{sku}'

            # Get current stock for target product variants
            inventory_query = """
                query {
                  productVariants(first: """+str(self.sku_per_request)+""", query: \""""+sku_query+"""\") {
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
            inventory_response = json.loads(client.execute(inventory_query))
            print(f"Inventory query cost:    {inventory_response['extensions']}")

            inventory_adjustments = ''

            for product_variant in inventory_response['data']['productVariants']['edges']:
                updated_stock = stock_data[product_variant['node']['sku']] - product_variant['node']['inventoryQuantity']
                # Skip if no change in stock level
                if updated_stock == 0:
                    continue
                inventory_adjustments += '{inventoryItemId: "' + product_variant['node']['inventoryItem']['id'] + '", availableDelta: ' + str(updated_stock) + '},'

            # Skip if no changes in any stock level were found
            if len(inventory_adjustments) == 0:
                print(f"Update stock query skipped, because of no stock level changes for product variants in batch")
                index = index + self.sku_per_request
                time.sleep(self.wait_seconds)
                continue

            # Update stock for product variants using calculated deltas
            stock_update_query = """
                mutation {
                  inventoryBulkAdjustQuantityAtLocation(
                    locationId: \"""" + self.location_id + """\",
                    inventoryItemAdjustments: [""" + inventory_adjustments + """
                      ]) {

                    inventoryLevels {
                      available
                    }
                  }
                }
            """
            stock_update_response = json.loads(client.execute(stock_update_query))
            print(f"Update stock query cost: {stock_update_response['extensions']}")

            index = index + self.sku_per_request
            time.sleep(self.wait_seconds)
