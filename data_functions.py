import sqlite3
import pandas as pd
from datetime import datetime, timezone
import os
from typing import Dict, Any


BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "flask_app.db")

def create_sql_tables():
    """
    Creates connection to sqlite db/file which is mounted as a volume, creates cursor, creates tables,
    and commits transaction.

    No Arguments.
    """

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # SQL statement to create the orders table.
        create_order_table_sql = '''
        create table if not exists orders (
        transaction_id varchar(36) primary key,
        ingest_timestamp timestamp,
        customer_id int,
        customer_name varchar(500),
        transaction_date timestamp,
        cash_payment_total float,
        credit_payment_total float,
        order_total float,
        billing_country varchar(500),
        billing_street_address varchar(500),
        billing_city varchar(500),
        billing_state varchar(2),
        billing_zip_code varchar(5)
        )
        '''
        cursor.execute(create_order_table_sql)


        # SQL statement to create the order_items table.
        create_order_items_table = '''
        create table if not exists order_items (
        transaction_id varchar(36),
        item_id varchar(36),
        item_name varchar(500),
        quantity int,
        price_per_unit float,
        primary key (transaction_id, item_id)
        )
        '''
        cursor.execute(create_order_items_table)

        conn.commit()


def process_json(json_data: Dict[str, Any]):
    """
    Provides all pandas processing for incoming events and splits to the two sql tables.

    General processing to ensure proper columns names, dtypes, and values.

    Arguments:
        json_data {Dict[str, Any]} -- Dictionary of json data

    The json_data will be passed from the incoming post requests for processing.
    """
    df = pd.json_normalize(json_data)

    df['ingest_timestamp'] = datetime.now(timezone.utc)

    df['customer_name'] = df['customer_name'].str.replace('Mrs. ', '', regex=False)

    df['customer_name'] = df['customer_name'].str.replace('Mr. ', '', regex=False)

    # Convert all payment amounts to dollars from cents and sum the total.
    df['cash_payment_total'] = round(df['cash_payment_pennies'] / 100, 2)

    df['credit_payment_total'] = round(df['credit_payment_pennies'] / 100, 2)

    df['order_total'] = round(df['cash_payment_total'] + df['credit_payment_total'], 2)

    df.drop(columns=['cash_payment_pennies', 'credit_payment_pennies'], inplace=True)

    # Removes all periods left by the json_normalize function.
    df.rename(columns={'billing_address.street': 'billing_street_address',
                       'billing_address.city': 'billing_city',
                       'billing_address.state': 'billing_state',
                       'billing_address.zip_code': 'billing_zip_code',
                       'region': 'billing_country'}, inplace=True)

    df_items = df[['transaction_id', 'items']].copy()

    # List of columns to include in the output to be written to db.
    output_columns = ['transaction_id',
                      'ingest_timestamp',
                      'customer_id',
                      'customer_name',
                      'transaction_date',
                      'cash_payment_total',
                      'credit_payment_total',
                      'order_total',
                      'billing_country',
                      'billing_street_address',
                      'billing_city',
                      'billing_state',
                      'billing_zip_code']

    df = df[output_columns]

    # Extracts the single transaction_id before converting the remaining dict into columns and rows.
    item_transaction = df_items['transaction_id'].item()

    # Process the remaining items into columns and rows and inserts transaction_id for all rows for join key.
    df_items = df_items.explode('items', ignore_index=True)

    df_items = pd.json_normalize(df_items['items'])

    df_items['transaction_id'] = item_transaction

    df_items['price_per_unit'] = round(df_items['price_per_unit_pennies'] / 100, 2)

    # List of columns for order_items output.
    item_cols = ['transaction_id',
                 'item_id',
                 'item_name',
                 'quantity',
                 'price_per_unit']

    df_items = df_items[item_cols]

    # Ensures string formatting for all columns that should be strings.
    df_items[['transaction_id', 'item_id', 'item_name']] = df_items[
        ['transaction_id', 'item_id', 'item_name']].astype(str)

    # Creates connection to the two sqlite tables and appends the processed data.
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(name='orders', con=conn, if_exists='append', index=False)

        df_items.to_sql(name='order_items', con=conn, if_exists='append', index=False)
