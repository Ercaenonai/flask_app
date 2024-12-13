# Flask Dummy Data Processor

A Flask-based project designed to ingest and process dummy JSON data, showcasing the use of Flask APIs and Docker for containerized deployment.

## General Purpose

This project demonstrates how to:
- Deploy a Flask application inside a Docker container.
- Handle JSON data ingestion and processing through RESTful API endpoints.
- Use Taskfile for streamlined local development and container management.

---

## Prerequisites
- Docker installed on your machine.
- Optional: [Taskfile](https://taskfile.dev/) installed for streamlined commands.


## How to Use

You can use either **Taskfile commands** or **pure Docker commands** to build, run, and manage the project.

### Taskfile Commands

Ensure you have [Taskfile](https://taskfile.dev/) installed on your system. The provided Taskfile contains commands for building, running, stopping, and removing the Flask container.

1. **Build the Docker Image**:
   ```bash
   task build
   ```
   This command copies the necessary Python files to the `images/flask` directory and builds the Docker image.

2. **Run the Flask App**:
   ```bash
   task run
   ```
   This will start the Flask server on port `5000` and mount the `flask_app.db` SQLite database from the local directory to the container.

3. **Stop the Flask App**:
   ```bash
   task stop
   ```
   Stops the running container.

4. **Remove the Flask Container**:
   ```bash
   task remove
   ```
   Removes the stopped container from Docker.

---

### Pure Docker Commands

If you prefer not to use Taskfile, you can manually execute the equivalent Docker commands.
You will need to ensure you have requirement.txt in the images directory.

1. **Build the Docker Image**:
   ```bash
   docker build -t repo/image:0.0.1 ./images/flask
   ```

2. **Run the Flask App**:
   ```bash
   docker run --name flask -d -p 5000:5000 -v $(pwd)/flask_app.db:/app/flask_app.db repo/image:0.0.1
   ```

3. **Stop the Flask App**:
   ```bash
   docker stop flask
   ```

4. **Remove the Flask Container**:
   ```bash
   docker rm flask
   ```

---

## API Endpoints

### `/ingest`
- **Method**: POST
- **Description**: Accepts JSON data to be ingested into the system.
- **Payload**:
  ```json
  {
    "transaction_id": "string",
    "customer_id": 12345,
    "customer_name": "John Doe",
    "transaction_date": "2024-12-13T10:15:30Z",
    "items": ["item1", "item2"],
    "cash_payment_pennies": 1000,
    "credit_payment_pennies": 2000,
    "billing_address": {
      "street": "123 Elm St",
      "city": "Somewhere",
      "state": "CA",
      "zip_code": 90210
    },
    "region": "US"
  }
  ```
- **Response**:
  ```json
  {
    "status": "success",
    "message": "Data ingested successfully"
  }
  ```

### Error Handling
For any invalid JSON the API will return:
- **Status Code**: `400` BadRequest
- **Status Code**: `422` ValueError
- **Status Code**: `None` Custom message pointing to invalid field in JSON
- **Status Code**: `500` Exception for all unexpected exceptions

---

## Generating Dummy Data

To generate and ingest dummy data, use the `generate_json.py` script. This script sends POST requests to the `/ingest` endpoint of the Flask server.

### Usage
1. Open the `generate_json.py` file.
2. Adjust the parameters for data generation:
   - **Total Records**: Modify the `for i in range(100):` line to specify the total number of records to generate.
   - **Frequency**: Adjust the `time.sleep(0.25)` line to control the interval (in seconds) between requests.
3. Run the script:
   ```bash
   python generate_json.py
   ```

---

## Notes
- Ensure the `flask_app.db` SQLite database file exists in the project root. If not, the application will automatically create it upon first run.
- For troubleshooting Docker volumes or connections, refer to the Docker logs using:
  ```bash
  docker logs flask
  ```
- The `generate_json.py` script requires the Flask server to be running for successful POST requests.

