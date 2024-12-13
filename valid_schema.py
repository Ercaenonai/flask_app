valid_schema = {
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
