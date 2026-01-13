import json
from datetime import datetime
from src.utils.helpers import load_config, get_retry_session
from src.utils.logging import setup_logger
from src.producer.kafka_producer import send_to_kafka

logger = setup_logger(__name__)

def crawl_coindesk():
    """Crawl data from Coindesk API and return processed results."""
    config = load_config()
    api_key = config['api'].get('api_key') or None
    
    instruments = config['api']['instruments']
    url = config['api']['coindesk_url'].format(instruments=instruments)
    
    session = get_retry_session()
    headers = {"authorization": f"Apikey {api_key}"} if api_key else {}
    
    try:
        logger.info(f"Extracting data for: {instruments}")
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        raw_data = response.json()
        if "Err" in raw_data and raw_data["Err"]:
            logger.error(f"API error: {raw_data['Err']}")
            return None
        
        results = []
        data_dict = raw_data.get("Data", {})
        for key, value in data_dict.items():
            results.append({
                "instrument": value.get("INSTRUMENT"),
                "price": value.get("VALUE"),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                # Add more fields if needed: open, high, low, etc.
            })
        
        logger.info("Data extracted successfully")
        return results
    
    except Exception as e:
        logger.error(f"Error in extraction: {e}")
        return None

if __name__ == "__main__":
    data = crawl_coindesk()
    if data:
        send_to_kafka(data)
        logger.info("Data sent to Kafka")
    else:
        logger.error("Extraction failed")