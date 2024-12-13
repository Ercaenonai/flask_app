import requests
import time
from faker import Faker
import random

# Flask server URL.
url = "http://127.0.0.1:5000/ingest"

headers = {
    'Content-type': 'application/json'
}

# instantiate faker
fake = Faker()

# For loop to randomly generate json records per sleep interval.
for i in range(100):
    payload = {
        "transaction_id": fake.uuid4(),
        "customer_id": random.randint(10000, 99999),
        "customer_name": fake.name(),
        "transaction_date": fake.date_time_this_year().isoformat(),
        "items": [
            {
                "item_id": fake.uuid4(),
                "item_name": fake.word(),
                "quantity": random.randint(1, 5),
                "price_per_unit_pennies": random.randint(100, 5000),  # Price in pennies.
            }
            for _ in range(random.randint(1, 10))  # Random number of items.
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

    try:
        # Send post request to flask server.
        response = requests.post(url, verify=True, json=payload, headers=headers)

    except ConnectionError as e:
        print(e)

    # Ensure json is sent without error from server.
    if response.status_code == 200:
        print('JSON data sent successfully')

    else:
        print('Error sending JSON data', response.status_code)

    # Count of records submitted.
    print(i)

    time.sleep(.25)
