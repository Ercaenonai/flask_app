from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import json
from jsonschema import validate, ValidationError
import uuid
from datetime import datetime
import logging
from werkzeug.exceptions import BadRequest

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

# TODO simplify schema to avoid nesting maybe. maybe process nested to be thorough
# schema = {
#     "type": "object",
#     "properties": {
#         "transaction_id": {"type": "string", "minLength": 36},
#         "customer_id": {"type": "integer", "minLength": 5, "maxLength": 5},
#         "customer_name": {"type": "string"},
#         "transaction_date": {"type": "timestamp"},
#         "items": {"type": "array", "items": {"type": "string"}},
#         "cash_payment_pennies": {"type": "integer", "min": 0, "max": 10000},
#         "credit_payment_pennies": {"type": "integer", "min": 0, "max": 10000},
#         "shipping_address": {"type": "array", "items": {"type": "string"}},
#         "region": {"type": "string"},
#     },
#     "required": ["transaction_id", "customer_id", "customer_name", "transaction_date", "items",
#                  "cash_payment_pennies", "credit_payment_pennies"],
#     "additionalProperties": False
# }


app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


class Order(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    uuid = db.Column(db.String(36), nullable=False)
    account_id = db.Column(db.String(50), nullable=False)
    order_id = db.Column(db.String(50), nullable=False)
    order_date = db.Column(db.DateTime, default=datetime.utcnow)
    product_id = db.Column(db.String(50), nullable=False)
    product_description = db.Column(db.String(255), nullable=False)
    unit_price = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    order_total = db.Column(db.Float, nullable=False)
    cash_total = db.Column(db.Float, nullable=False)
    credit_total = db.Column(db.Float, nullable=False)
    tax_applicable = db.Column(db.Boolean, nullable=False)
    billing_zip_code = db.Column(db.String(10), nullable=False)
    ip_address = db.Column(db.String(45), nullable=False)


# Initialize the database
with app.app_context():
    db.create_all()


@app.route('/ingest', methods=['POST'])
def process_data():
    try:
        data = request.get_json(force=True)

        # try:
        #     validate(instance=data, schema=schema)
        #
        # except ValidationError as e:
        #     # Log the malformed schema
        #     logger.error(f"Incorrect schema: {data}")
        #
        #     return jsonify({"error": str(e)}), 400

        return jsonify({"status": "success", "message": "Event processed successfully"}), 200, data

    except BadRequest as e:
        # Log the malformed JSON request
        logger.error(f"Malformed JSON: {request.data.decode('utf-8')}")

        return jsonify({"status": "error", "message": "Invalid JSON format"}), 400

    except ValueError as e:
        # Handle custom validation errors
        return jsonify({"status": "error", "message": str(e)}), 422

    except Exception as e:
        # Catch-all for unexpected errors
        return jsonify({"status": "error", "message": "An unexpected error occurred"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
