# Airflow Shopify Stock Update Plugin

Airflow operator that can be used to regulary schedule imports of external stock data to update Shopify's inventory at a given location (https://shopify.dev/api/admin-graphql/2021-07/objects/location).

* Requirements
  * Setup of a custom app on the Shopify-side for access to the store's Shopify API via access token
     * https://help.shopify.com/en/manual/apps/
  * ShopifyAPI package 
     * https://pypi.org/project/ShopifyAPI/

### CSV file format example

* Note: Headers are only for explanation here, do not add headers to actual file.

| Shopify Variant SKU | Stock |
| ------------- | ------------- |
| SKU01  | 15 |
| SKU02  | 3 |