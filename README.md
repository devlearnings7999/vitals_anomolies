# Vitals Anomaly Detector

This project contains a Python script that consumes real-time health vitals data from a Kafka topic, detects anomalies, and publishes them to another Kafka topic.

## Project Structure

```
vitals_anomolies/
├── .env
├── main.py
├── Dockerfile
├── requirements.txt
└── README.md
```

## Setup and Usage

### 1. Environment Configuration

Create a `.env` file in the `vitals_anomolies` directory with your Kafka connection details and topic names. A sample `.env` file is provided below:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
CONSUMER_TOPIC=vitals-ml-test1
PRODUCER_TOPIC=vitals_anomalies
SECURITY_PROTOCOL=SASL_PLAINTEXT
SASL_MECHANISM=SCRAM-SHA-512
SASL_USERNAME=user
SASL_PASSWORD=password
```

*   `KAFKA_BOOTSTRAP_SERVERS`: The Kafka broker address.
*   `CONSUMER_TOPIC`: The topic from which health vitals data will be consumed (e.g., `vitals-ml-test1`).
*   `PRODUCER_TOPIC`: The topic to which detected anomalies will be published (e.g., `vitals_anomalies`).
*   `SECURITY_PROTOCOL`: The security protocol for Kafka connection (e.g., `SASL_PLAINTEXT`).
*   `SASL_MECHANISM`: The SASL mechanism for authentication (e.g., `SCRAM-SHA-512`).
*   `SASL_USERNAME`: The SASL username for authentication.
*   `SASL_PASSWORD`: The SASL password for authentication.

### 2. Build the Docker Image

Navigate to the `vitals_anomolies` directory in your terminal and build the Docker image:

```bash
docker build -t vitals-anomaly-detector .
```

### 3. Run the Docker Container

Run the Docker container, mounting the `.env` file to provide the Kafka configuration:

```bash
docker run -d --name anomaly-detector --env-file ./.env vitals-anomaly-detector
```

### 4. Verify Functionality

*   **Console Output:** Check the Docker container logs to see detected anomalies printed to the console:
    ```bash
    docker logs anomaly-detector
    ```
*   **Kafka Topic:** Verify that anomaly messages are being published to the `vitals_anomalies` Kafka topic using a Kafka consumer tool.

### Anomaly Detection Logic

The script detects anomalies based on the following rules:

*   Body temperature > 38 or < 35 (Celsius)
*   Heart rate > 100 or < 50 (beats per minute)
*   Oxygen < 92 (%)

### Dead Letter Queue (DLQ)

Messages that fail to be published to the `PRODUCER_TOPIC` after retries will be sent to a Dead Letter Queue topic, which will be named `vitals_anomalies_dlq` by default.
