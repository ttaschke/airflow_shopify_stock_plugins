import unittest
from unittest import mock

from airflow import configuration
from airflow.hooks.base_hook import BaseHook

from plugins.hooks.shopify_hook import ShopifyHook


class TestShopifyHook(unittest.TestCase):

    def setUp(self):
        configuration.conf.load_test_config()

    @mock.patch("plugins.hooks.shopify_hook.shopify")
    @mock.patch.object(BaseHook, "get_connection")
    def test_get_conn_success(self, mock_get_connection, mock_shopify):

        # Mock the Airflow connection
        mock_connection = mock.MagicMock()
        mock_connection.host = "myshop.myshopify.com"
        mock_connection.password = "access_token"
        mock_get_connection.return_value = mock_connection

        # Mock the Shopify session and GraphQL connection
        mock_session = mock.MagicMock()
        mock_shopify.Session.return_value = mock_session
        mock_graphql_client = mock.MagicMock()
        mock_shopify.GraphQL.return_value = mock_graphql_client

        # Get client from Hook
        hook = ShopifyHook(conn_id="shopify_default")
        client = hook.get_conn()

        # Assertions
        mock_shopify.Session.assert_called_once_with(
            "myshop.myshopify.com", "2025-01", "access_token"
        )
        mock_shopify.ShopifyResource.activate_session.assert_called_once_with(
            mock_session
        )
        mock_shopify.GraphQL.assert_called_once()
        self.assertEqual(client, mock_graphql_client)
