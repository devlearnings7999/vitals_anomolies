import os
import json
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
CONSUMER_TOPIC = os.getenv('CONSUMER_TOPIC', 'vitals-ml-test1')
PRODUCER_TOPIC = os.getenv('PRODUCER_TOPIC', 'vitals_anomalies')
SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
SASL_MECHANISM = os.getenv('SASL_MECHANISM', 'SCRAM-SHA-512')
SASL_USERNAME = os.getenv('SASL_USERNAME', 'user')
SASL_PASSWORD = os.getenv('SASL_PASSWORD', 'password')

# Dead-letter topic
DEAD_LETTER_TOPIC = f"{PRODUCER_TOPIC}_dlq"

def serialize_json(obj):
    return json.dumps(obj).encode('utf-8')

def deserialize_json(message):
    return json.loads(message.decode('utf-8'))

def detect_anomaly(vitals):
    anomalies = []
    if vitals['body_temp'] > 38 or vitals['body_temp'] < 35:
        anomalies.append(f"Body temperature anomaly: {vitals['body_temp']}°C")
    if vitals['heart_rate'] > 100 or vitals['heart_rate'] < 50:
        anomalies.append(f"Heart rate anomaly: {vitals['heart_rate']} bpm")
    if vitals['oxygen'] < 92:
        anomalies.append(f"Oxygen anomaly: {vitals['oxygen']}%")
    return anomalies

def main():
    consumer = None
    producer = None
    dlq_producer = None

    try:
        # Initialize Kafka Consumer
        consumer = KafkaConsumer(
            CONSUMER_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanism=SASL_MECHANISM,
            sasl_plain_username=SASL_USERNAME,
            sasl_plain_password=SASL_PASSWORD,
            value_deserializer=deserialize_json,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='vitals-anomaly-detector'
        )
        print(f"Connected to Kafka consumer topic: {CONSUMER_TOPIC}")

        # Initialize Kafka Producer for anomalies
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanism=SASL_MECHANISM,
            sasl_plain_username=SASL_USERNAME,
            sasl_plain_password=SASL_PASSWORD,
            value_serializer=serialize_json,
            retries=5,  # Retry up to 5 times
            acks='all'  # Ensure all replicas acknowledge the message
        )
        print(f"Connected to Kafka producer topic: {PRODUCER_TOPIC}")

        # Initialize Kafka Producer for Dead Letter Queue
        dlq_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanism=SASL_MECHANISM,
            sasl_plain_username=SASL_USERNAME,
            sasl_plain_password=SASL_PASSWORD,
            value_serializer=serialize_json
        )
        print(f"Connected to Kafka DLQ topic: {DEAD_LETTER_TOPIC}")

        for message in consumer:
            vitals_data = message.value
            if vitals_data:
                print(f"Received vital data: {vitals_data}")
                anomalies = detect_anomaly(vitals_data)

                if anomalies:
                    anomaly_report = {
                        "original_message": vitals_data,
                        "anomalies": anomalies,
                        "timestamp": os.getenv('CURRENT_TIMESTAMP') # A placeholder for timestamp generation if needed
                    }
                    print(f"ANOMALY DETECTED: {anomaly_report}")

                    try:
                        future = producer.send(PRODUCER_TOPIC, value=anomaly_report)
                        record_metadata = future.get(timeout=10) # Block until send is complete or timeout
                        print(f"Anomaly published to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
                    except KafkaError as e:
                        print(f"Failed to publish anomaly to {PRODUCER_TOPIC}. Sending to DLQ. Error: {e}")
                        try:
                            dlq_producer.send(DEAD_LETTER_TOPIC, value={"original_message": vitals_data, "error": str(e)})
                            dlq_producer.flush()
                        except KafkaError as dlq_e:
                            print(f"Failed to send message to DLQ: {dlq_e}")
                else:
                    print("No anomalies detected.")
            else:
                print("Received empty or malformed message.")

    except KafkaError as e:
        print(f"Kafka error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Kafka consumer closed.")
        if producer:
            producer.close()
            print("Kafka producer closed.")
        if dlq_producer:
            dlq_producer.close()
            print("Kafka DLQ producer closed.")

if __name__ == "__main__":
    main()
