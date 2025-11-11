# Vitals Anomaly Detector

This project reads health vitals data from a Kafka topic, detects anomalies, and publishes the anomalies and average vitals to other Kafka topics.

## Prerequisites

*   Docker
*   Kafka cluster

## Setup

1.  Clone the repository.
2.  Navigate to the project directory: `/home/devraj-hireraddi/test_folder/Vitals_Anomolies`
3.  Create a `.env` file in the project directory with the following variables:
    *   `KAFKA_BROKER`: Kafka broker address (e.g., `your_kafka_broker:9092`)
    *   `KAFKA_TOPIC_VITALS`: Kafka topic for input vitals data (`vitals-ml-test1`)
    *   `KAFKA_TOPIC_ANOMALIES`: Kafka topic for anomaly data (`vitals_anomalies`)
    *   `KAFKA_TOPIC_ANOMALIES_AVG`: Kafka topic for average vitals data (`vitals_anomalies_avg`)
    *   `SECURITY_PROTOCOL`: Security protocol (`SASL_PLAINTEXT` or `PLAINTEXT`)
    *   `SASL_MECHANISM`: SASL mechanism (`SCRAM-SHA-512` if using SASL)
    *   `SASL_USERNAME`: Kafka username (if using SASL)
    *   `SASL_PASSWORD`: Kafka password (if using SASL)

## Running the Application

1.  Build the Docker image:

    ```bash
    docker build -t vitals-anomaly-detector .
    ```

2.  Run the Docker container:

    ```bash
    docker run -d --name vitals-anomaly-detector --env-file .env vitals-anomaly-detector
    ```

## Notes

*   Make sure the Kafka topics are created before running the application.
*   Adjust the anomaly thresholds in `main.py` as needed.
*   The application uses exponential backoff for retries when publishing messages to Kafka.
*   Messages that fail to publish after multiple retries are sent to a dead-letter topic (`vitals_anomalies_dead_letter`).
