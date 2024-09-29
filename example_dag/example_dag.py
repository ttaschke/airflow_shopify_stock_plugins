from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.shopify_update_stock_csv_operator import (
    ShopifyUpdateStockCsvOperator,
)

dag = DAG(
    dag_id="shopify_stock_example",
    description="Example DAG for Shopify stock plugin",
    schedule_interval=None,
    start_date=datetime(2018, 10, 11),
    max_active_runs=1,
)


def import_stock_csv():
    print(
        "This could be an import or create the stock csv file from an external source"
    )


import_stock_csv = PythonOperator(
    task_id="import_stock_csv", python_callable=import_stock_csv, dag=dag
)

sync_stock = ShopifyUpdateStockCsvOperator(
    task_id="sync_stock",
    conn_id="shopify",
    location_id="gid://shopify/Location/99999999999",
    file_location="{{conf.get('core', 'dags_folder')}}/test_data/stock.csv",
    dag=dag,
)

import_stock_csv >> sync_stock
