from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import csv
import shopify

from hooks.shopify_hook import ShopifyHook


class ShopifyUpdateStockCsvOperator(BaseOperator):
    """
    Operator that takes a CSV file (columns: sku, stock) as input
    and syncs the stock of corresponding product variants in Shopify
    :param conn_id: Shopify connection
    :param location_id: Shopify location GraphQL API id to define at which location to update the inventory
    :param file_location: File location of the CSV file
    """

    template_fields = ['file_location']

    @apply_defaults
    def __init__(self, conn_id, location_id, file_location, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.location_id = location_id
        self.file_location = file_location

    def execute(self, context):

        shopify_hook = ShopifyHook(conn_id=self.conn_id)
        client = shopify_hook.get_conn()

        with open(self.file_location) as csv_file:
            stock_data = {row[0]: int(row[1]) for row in csv.reader(csv_file)}

        print(f'Parsed {len(stock_data)} sku from csv file "{self.file_location}"')

        sku_query = ''
        for sku in stock_data.keys():
            if len(sku_query) > 0:
                sku_query += ' OR '
            sku_query += f'sku:{sku}'

        inventory_query = """
            query {
              productVariants(first: 100, query: \""""+sku_query+"""\") {
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
              }
            }"""
        inventory_response = json.loads(client.execute(inventory_query))

        inventory_adjustments = ''

        for node in inventory_response['data']['productVariants']['edges']:
            updated_stock = stock_data[node['node']['sku']] - node['node']['inventoryQuantity']
            inventory_adjustments += '{inventoryItemId: "' + node['node']['inventoryItem']['id'] + '", availableDelta: ' + str(updated_stock) + '},'

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
        response = json.loads(client.execute(stock_update_query))
