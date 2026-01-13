from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.crawler.coindesk_crawler import crawl_coindesk
from src.producer.kafka_producer import send_to_kafka
import logging

# Logger chuẩn cho DAG
logger = logging.getLogger("crypto_crawl_dag")
logger.setLevel(logging.INFO)

default_args = {
    'start_date': datetime(2026, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_crawl_dag',
    default_args=default_args,
    schedule_interval='* * * * *',  # chạy mỗi phút
    catchup=False,                  # không chạy backlog khi manual trigger
    description="DAG crawl dữ liệu crypto từ Coindesk và gửi vào Kafka"
)

def run_crawl_and_produce():
    """Hàm crawl dữ liệu và gửi vào Kafka"""
    try:
        data = crawl_coindesk()
        logger.info(f"Crawl result: {data}")
        if data:
            send_to_kafka(data)
            logger.info("Data sent to Kafka successfully")
        else:
            logger.warning("No data to send to Kafka")
    except Exception as e:
        logger.error(f"Error in run_crawl_and_produce: {e}")

task = PythonOperator(
    task_id='crawl_and_produce',
    python_callable=run_crawl_and_produce,
    dag=dag,
)
