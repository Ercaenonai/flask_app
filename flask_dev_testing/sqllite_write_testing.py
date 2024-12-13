import pandas as pd
import sqlite3
from faker import Faker
import random
import json
from datetime import datetime, timezone

# instantiate faker
fake = Faker()

conn = sqlite3.connect("../flask_app.db")

cursor = conn.cursor()

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

# conn.close()


payload = {
    "transaction_id": fake.uuid4(),
    "customer_id": str(random.randint(10000, 99999)),
    "customer_name": fake.name(),
    "transaction_date": fake.date_time_this_year().isoformat(),
    "items": [
        {
            "item_id": fake.uuid4(),
            "item_name": fake.word(),
            "quantity": random.randint(1, 5),
            "price_per_unit_pennies": random.randint(100, 5000),  # Price in pennies
        }
        for _ in range(random.randint(1, 10))  # Random number of items
    ],
    "cash_payment_pennies": random.randint(0, 10000),
    "credit_payment_pennies": random.randint(0, 20000),
    "billing_address": {
        "street": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip_code": int(fake.zipcode()),
    },
    "region": fake.country(),
}

df = pd.json_normalize(payload)

df['ingest_timestamp'] = datetime.now(timezone.utc)

df['customer_name'] = df['customer_name'].str.replace('Mrs. ', '', regex=False)

df['customer_name'] = df['customer_name'].str.replace('Mr. ', '', regex=False)

df['cash_payment_total'] = round(df['cash_payment_pennies'] / 100, 2)

df['credit_payment_total'] = round(df['credit_payment_pennies'] / 100, 2)

df['order_total'] = df['cash_payment_total'] + df['credit_payment_total']

df.drop(columns=['cash_payment_pennies', 'credit_payment_pennies'], inplace=True)

df.rename(columns={'billing_address.street': 'billing_street_address',
                   'billing_address.city': 'billing_city',
                   'billing_address.state': 'billing_state',
                   'billing_address.zip_code': 'billing_zip_code',
                   'region': 'billing_country'}, inplace=True)

df_items = df[['transaction_id', 'items']].copy()

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

item_transaction = df_items['transaction_id']

df_items = df_items.explode('items', ignore_index=True)

df_items = pd.json_normalize(df_items['items'])

df_items['transaction_id'] = [item_transaction] * len(df_items)

df_items['price_per_unit'] = round(df_items['price_per_unit_pennies'] / 100, 2)

item_cols = ['transaction_id',
             'item_id',
             'item_name',
             'quantity',
             'price_per_unit']

df_items = df_items[item_cols]

df_items[['transaction_id', 'item_id', 'item_name']] = df_items[['transaction_id', 'item_id', 'item_name']].astype(str)

df.to_sql(name='orders', con=conn, if_exists='append', index=False)

df_items.to_sql(name='order_items', con=conn, if_exists='append', index=False)
