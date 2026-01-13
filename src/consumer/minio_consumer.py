import json
from kafka import KafkaConsumer
import boto3
from src.utils.helpers import load_config
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

def consume_and_save_minio():
    config = load_config()
    consumer = KafkaConsumer(
        config['kafka']['topic'],
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        group_id='minio-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    bucket_name = 'crypto-raw'
    # Create bucket if not exists
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(Bucket=bucket_name)

    for message in consumer:
        key = f"{message.value['instrument']}_{message.value['timestamp']}.json"
        s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(message.value))
        logger.info(f"Saved to MinIO: {key}")

if __name__ == "__main__":
    consume_and_save_minio()
