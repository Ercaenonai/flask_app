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
        "shipping_address": {
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

# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
# app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#
# db = SQLAlchemy(app)
#
#
# class Order(db.Model):
#     id = db.Column(db.Integer, primary_key=True)
#     uuid = db.Column(db.String(36), nullable=False)
#     account_id = db.Column(db.String(50), nullable=False)
#     order_id = db.Column(db.String(50), nullable=False)
#     order_date = db.Column(db.DateTime, default=datetime.utcnow)
#     product_id = db.Column(db.String(50), nullable=False)
#     product_description = db.Column(db.String(255), nullable=False)
#     unit_price = db.Column(db.Float, nullable=False)
#     quantity = db.Column(db.Integer, nullable=False)
#     order_total = db.Column(db.Float, nullable=False)
#     cash_total = db.Column(db.Float, nullable=False)
#     credit_total = db.Column(db.Float, nullable=False)
#     tax_applicable = db.Column(db.Boolean, nullable=False)
#     billing_zip_code = db.Column(db.String(10), nullable=False)
#     ip_address = db.Column(db.String(45), nullable=False)
#
#
# # Initialize the database
# with app.app_context():
#     db.create_all()

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
        logger.error(e)

        return jsonify({"status": "error", "message": "An unexpected error occurred"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
