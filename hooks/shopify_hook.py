from airflow.hooks.base_hook import BaseHook
import shopify


class ShopifyHook(BaseHook):
    """
    This hooks sets up the Shopify API session and returns a graphql API client
    :param conn_id: Connection to use for Shopify. 
                    Type: http,
                    Host: shopdomain without http://
                    password: private app access token
    """

    def __init__(
            self,
            conn_id,
            *args,
            **kwargs
    ):
        self.client = None
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs
        self.connection = self.get_connection(conn_id)

    def get_conn(self):
        api_version = '2020-10'

        shop_url = self.connection.host
        access_token = self.connection.password.strip()

        # Connects via private app access token
        session = shopify.Session(shop_url, api_version, access_token)
        shopify.ShopifyResource.activate_session(session)
        self.client = shopify.GraphQL()

        return self.client
