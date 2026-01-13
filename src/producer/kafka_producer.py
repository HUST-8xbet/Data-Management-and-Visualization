import json
from kafka import KafkaProducer
from src.utils.helpers import load_config
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

def send_to_kafka(data):
    """Send data to Kafka topic."""
    config = load_config()
    producer = KafkaProducer(
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    topic = config['kafka']['topic']
    try:
        producer.send(topic, data)
        producer.flush()
        logger.info(f"Data sent to topic: {topic}")
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")