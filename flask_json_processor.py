from flask import Flask, request, jsonify
import json
from jsonschema import validate, ValidationError
import logging
from werkzeug.exceptions import BadRequest
from data_functions import create_sql_tables, process_json
from valid_schema import valid_schema

app = Flask(__name__)

# Configure logging.
LOG_FILE_PATH = "malformed_requests.log"

logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger()

schema = valid_schema

# Builds the two sql tables in sqlite.
create_sql_tables()


@app.route('/ingest', methods=['POST'])
def process_data():
    """
    Main processing for incoming events.

    In production, any event caught in exceptions would be written to persistent storage for review
    and potential reprocessing.
    """
    try:
        # Ensure payload is formatted properly and validate schema.
        payload = request.get_json(force=True)

        validate(instance=payload, schema=schema)

        process_json(payload)

        print("Event Processed Successfully")

        return jsonify({"status": "success", "message": "Event processed successfully"}), 200, payload

    except BadRequest:
        # Log the malformed JSON request.
        logger.error(f"Malformed JSON: {request.data.decode('utf-8')}")

        return jsonify({"status": "error", "message": "Invalid JSON format"}), 400

    except ValueError as e:
        # Handle custom validation errors.
        logger.error(f"ValueError: {e}")

        return jsonify({"status": "error", "message": str(e)}), 422

    except ValidationError as e:
        # Handle JSON schema validation errors.
        message = e.schema["error_msg"] if "error_msg" in e.schema else e.message

        logger.error(f"Schema validation failed: {message}")

        return jsonify({"status": "error", "message": message})

    except Exception as e:
        # Catch-all for unexpected errors.
        logger.exception("An unexpected error occurred")

        return jsonify({"status": "error", "message": "An unexpected error occurred"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
