from flask import Flask, request, jsonify
import json
from jsonschema import validate, ValidationError
import sqlite3
import pandas as pd
from datetime import datetime, timezone
from datetime import datetime
import logging
from werkzeug.exceptions import BadRequest
import os

app = Flask(__name__)

# configure logging
LOG_FILE_PATH = "malformed_requests.log"

logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger()

schema = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string", "minLength": 36},
        "customer_id": {"type": "integer", "minLength": 5, "maxLength": 5},
        "customer_name": {"type": "string"},
        "transaction_date": {"type": "string", "format": "date-time"},
        "items": {"type": "array"},
        "cash_payment_pennies": {"type": "integer", "min": 0, "max": 10000},
        "credit_payment_pennies": {"type": "integer", "min": 0, "max": 10000},
        "billing_address": {
            "type": "object",
            "properties": {
                "street": {"type": "string"},
                "city": {"type": "string"},
                "state": {"type": "string"},
                "zip_code": {"type": "integer"}
            }
        },
        "region": {"type": "string"},
    },
    "required": ["transaction_id", "customer_id", "customer_name", "transaction_date", "items",
                 "cash_payment_pennies", "credit_payment_pennies"],
    "additionalProperties": False
}

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "flask_app.db")

conn = sqlite3.connect(DB_PATH)

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

conn.close()


'''
Main processing for incoming events.
In production, any event caught in exceptions would be written to persistent storage for review 
and potential reprocessing.
'''

@app.route('/ingest', methods=['POST'])
def process_data():
    try:
        # Ensure payload is formatted properly and validate schema
        payload = request.get_json(force=True)

        # Intentionally cast customer_id as string in producer
        field_key = "customer_id"

        payload[field_key] = int(payload[field_key])

        validate(instance=payload, schema=schema)

        df = pd.json_normalize(payload)

        df['ingest_timestamp'] = datetime.now(timezone.utc)

        df['customer_name'] = df['customer_name'].str.replace('Mrs. ', '', regex=False)

        df['customer_name'] = df['customer_name'].str.replace('Mr. ', '', regex=False)

        df['cash_payment_total'] = round(df['cash_payment_pennies'] / 100, 2)

        df['credit_payment_total'] = round(df['credit_payment_pennies'] / 100, 2)

        df['order_total'] = round(df['cash_payment_total'] + df['credit_payment_total'], 2)

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

        df_items[['transaction_id', 'item_id', 'item_name']] = df_items[
            ['transaction_id', 'item_id', 'item_name']].astype(str)

        with sqlite3.connect(DB_PATH) as conn:
            df.to_sql(name='orders', con=conn, if_exists='append', index=False)

            df_items.to_sql(name='order_items', con=conn, if_exists='append', index=False)

        print("Event Processed Successfully")

        return jsonify({"status": "success", "message": "Event processed successfully"}), 200, payload

    except BadRequest:
        # Log the malformed JSON request
        logger.error(f"Malformed JSON: {request.data.decode('utf-8')}")

        return jsonify({"status": "error", "message": "Invalid JSON format"}), 400

    except ValueError as e:
        # Handle custom validation errors
        logger.error(f"ValueError: {e}")

        return jsonify({"status": "error", "message": str(e)}), 422

    except ValidationError as e:
        # Handle JSON schema validation errors
        message = e.schema["error_msg"] if "error_msg" in e.schema else e.message

        logger.error(f"Schema validation failed: {message}")

        return jsonify({"status": "error", "message": message})

    except Exception as e:
        # Catch-all for unexpected errors
        logger.exception("An unexpected error occurred")

        return jsonify({"status": "error", "message": "An unexpected error occurred"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
