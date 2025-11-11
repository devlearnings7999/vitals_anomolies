import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import logging
import random
import concurrent.futures

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC_VITALS = os.getenv('KAFKA_TOPIC_VITALS')
KAFKA_TOPIC_ANOMALIES = os.getenv('KAFKA_TOPIC_ANOMALIES')
KAFKA_TOPIC_ANOMALIES_AVG = os.getenv('KAFKA_TOPIC_ANOMALIES_AVG')
SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL', 'PLAINTEXT')
SASL_MECHANISM = os.getenv('SASL_MECHANISM', None)
SASL_USERNAME = os.getenv('SASL_USERNAME', None)
SASL_PASSWORD = os.getenv('SASL_PASSWORD', None)
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2
DEAD_LETTER_TOPIC = 'vitals_anomalies_dead_letter'  # Define dead-letter topic

# Kafka configuration
kafka_config = {
    'bootstrap_servers': KAFKA_BROKER,
    'security_protocol': SECURITY_PROTOCOL,
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'api_version': (0, 10, 1)
}

if SECURITY_PROTOCOL == 'SASL_PLAINTEXT':
    kafka_config['sasl_mechanism'] = SASL_MECHANISM
    kafka_config['sasl_plain_username'] = SASL_USERNAME
    kafka_config['sasl_plain_password'] = SASL_PASSWORD

anomaly_thresholds = {
    'body_temp': (35, 38),
    'heart_rate': (50, 100),
    'oxygen': (92, 100)
}


def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        security_protocol=SECURITY_PROTOCOL,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        api_version=(0, 10, 1)
    )
    if SECURITY_PROTOCOL == 'SASL_PLAINTEXT':
        producer.config['sasl_mechanism'] = SASL_MECHANISM
        producer.config['sasl_plain_username'] = SASL_USERNAME
        producer.config['sasl_plain_password'] = SASL_PASSWORD
    return producer

def publish_message(producer, topic, message, retry_attempts=RETRY_ATTEMPTS, retry_backoff=RETRY_BACKOFF_SECONDS):
    for attempt in range(retry_attempts):
        try:
            producer.send(topic, value=message).get(timeout=10)  # Adjust timeout as needed
            logging.info(f"Published message to topic {topic}: {message}")
            return True
        except KafkaError as e:
            logging.error(f"Failed to publish message (attempt {attempt + 1}/{retry_attempts}): {e}")
            if attempt < retry_attempts - 1:
                time.sleep(retry_backoff ** (attempt + 1))  # Exponential backoff
            else:
                logging.error(f"Failed to publish message after {retry_attempts} attempts. Sending to dead-letter topic.")
                send_to_dead_letter_topic(producer, message)
            return False

def send_to_dead_letter_topic(producer, message):
    try:
        producer.send(DEAD_LETTER_TOPIC, value=message).get(timeout=10)
        logging.info(f"Published message to dead-letter topic {DEAD_LETTER_TOPIC}: {message}")
    except KafkaError as e:
        logging.error(f"Failed to publish message to dead-letter topic: {e}")

def detect_anomalies(vitals):
    anomalies = []
    if not isinstance(vitals, dict):
        logging.warning(f"Received non-dictionary vitals: {vitals}")
        return anomalies

    if not all(key in vitals for key in ['body_temp', 'heart_rate', 'systolic', 'diastolic', 'breaths', 'oxygen', 'glucose']):
         logging.warning(f"Missing keys in vitals data: {vitals}")
         return anomalies

    if not all(isinstance(vitals[key], (int, float)) for key in ['body_temp', 'heart_rate', 'systolic', 'diastolic', 'breaths', 'oxygen', 'glucose']):
        logging.warning(f"Incorrect data types in vitals data: {vitals}")
        return anomalies


    if vitals['body_temp'] > anomaly_thresholds['body_temp'][1] or vitals['body_temp'] < anomaly_thresholds['body_temp'][0]:
        anomalies.append(f"Body Temperature: {vitals['body_temp']}")
    if vitals['heart_rate'] > anomaly_thresholds['heart_rate'][1] or vitals['heart_rate'] < anomaly_thresholds['heart_rate'][0]:
        anomalies.append(f"Heart Rate: {vitals['heart_rate']}")
    if vitals['oxygen'] < anomaly_thresholds['oxygen'][0]:
        anomalies.append(f"Oxygen: {vitals['oxygen']}")

    return anomalies

def process_vitals(vitals, producer):
    anomalies = detect_anomalies(vitals)
    if anomalies:
        anomaly_message = {
            'timestamp': datetime.now().isoformat(),
            'vitals': vitals,
            'anomalies': anomalies
        }
        print(f"Detected anomalies: {anomaly_message}")
        publish_message(producer, KAFKA_TOPIC_ANOMALIES, anomaly_message)


def consume_vitals(producer):
    consumer = KafkaConsumer(
        KAFKA_TOPIC_VITALS,
        **kafka_config,
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

    vitals_buffer = []
    last_published = time.time()

    try:
        for message in consumer:
            vitals = message.value
            vitals_buffer.append(vitals)
            process_vitals(vitals, producer)

            # Calculate and publish averages every 10 seconds
            if time.time() - last_published >= 10:
                if vitals_buffer:
                    avg_vitals = calculate_average_vitals(vitals_buffer)
                    publish_message(producer, KAFKA_TOPIC_ANOMALIES_AVG, avg_vitals)
                    vitals_buffer = []  # Clear the buffer
                    last_published = time.time()
                else:
                    logging.info("No vitals data to average in the last 10 seconds.")


    except KafkaError as e:
        logging.error(f"Consumer error: {e}")
    except Exception as e:
        logging.error(f"General error: {e}")
    finally:
        consumer.close()


def calculate_average_vitals(vitals_list):
    if not vitals_list:
        return {}

    summed_vitals = {}
    for vitals in vitals_list:
        for key, value in vitals.items():
            if isinstance(value, (int, float)):
                summed_vitals.setdefault(key, 0)
                summed_vitals[key] += value

    avg_vitals = {k: v / len(vitals_list) for k, v in summed_vitals.items()}
    return avg_vitals


def main():
    producer = create_kafka_producer()
    try:
        consume_vitals(producer)
    except Exception as e:
        logging.error(f"Main function error: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
