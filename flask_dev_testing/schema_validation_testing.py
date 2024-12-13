from faker import Faker
import random
import json
from jsonschema import validate, ValidationError

# instantiate faker
fake = Faker()

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

print(f'schema template: {schema}')

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
    "shipping_address": {
        "street": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip_code": int(fake.zipcode()),
    },
    "region": fake.country(),
}

field_key = "customer_id"

payload[field_key] = int(payload[field_key])

try:
    validate(instance=payload, schema=schema)

except ValidationError as e:
    print(payload)

    message = e.schema["error_msg"] if "error_msg" in e.schema else e.message

    print(f"validation failure {message}")
