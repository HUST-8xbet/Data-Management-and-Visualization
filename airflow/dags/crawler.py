import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from kafka import KafkaProducer

# --- 1. DANH SÁCH 20 LOẠI COIN MUỐN LẤY TRÊN COINDESK ---
# Lưu ý: CoinDesk quy định mã phải kèm theo tiền tệ, ví dụ: "ETH-USD"
TOP_20_COINS = [
    'BTC', 'ETH', 'XRP', 'ADA', 'SOL', 'DOGE', 'DOT', 'TRX', 'AVAX', 'MATIC',
    'LTC', 'UNI', 'LINK', 'XLM', 'ATOM', 'ETC', 'FIL', 'HBAR', 'VET', 'ICP'
]

KAFKA_TOPIC = "bitcoin_prices"
if os.getenv('AIRFLOW_HOME'):
    # Nếu chạy trong Airflow (Docker) -> Dùng cổng nội bộ 29092
    KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
else:
    # Nếu chạy tay trên máy bạn (Local) -> Dùng cổng mở rộng 9092
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# --- 2. HÀM TẠO SESSION (GIỮ NGUYÊN TỪ CODE GỐC CỦA BẠN) ---
def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

# --- 3. HÀM CRAWL COINDESK (GỌI ĐÚNG MÃ NHƯ BẠN CỦA BẠN BẢO) ---
def crawl_coindesk_by_instrument(coin_symbol):
    # CoinDesk yêu cầu format: "CODE-USD" (Ví dụ: ETH-USD)
    instrument = f"{coin_symbol}-USD"
    
    # URL này hỗ trợ lấy nhiều loại coin (như code gốc của bạn)
    url = f"https://data-api.coindesk.com/index/cc/v1/latest/tick?market=ccix&instruments={instrument}"
    
    # Nếu bạn có API Key thì điền vào đây, nếu không thì để trống thử vận may
    # api_key = "YOUR_API_KEY"
    headers = {
        # "authorization": f"Apikey {api_key}" 
        # Tạm thời comment lại vì bạn chưa có key, gửi request trần xem được không
        "User-Agent": "Mozilla/5.0" 
    }

    session = create_session()
    
    try:
        response = session.get(url, headers=headers, timeout=5)
        
        # Nếu CoinDesk chặn (lỗi 401/403) vì thiếu Key
        if response.status_code in [401, 403]:
            print(f"⚠️ Không lấy được {coin_symbol}: CoinDesk yêu cầu API Key.")
            return None
            
        response.raise_for_status()
        raw_data = response.json()

        # Kiểm tra nếu API trả về lỗi
        if "Err" in raw_data and raw_data["Err"]:
            return None

        # --- XỬ LÝ DỮ LIỆU ---
        # API này trả về dạng Dictionary, ta phải bóc tách
        data_dict = raw_data.get("Data", {})
        
        # Vì ta query chính xác 1 mã, nên lấy item đầu tiên tìm được
        for key, value in data_dict.items():
            if coin_symbol in key: # Khớp mã
                return {
                    "instrument": value.get("INSTRUMENT"),
                    "price": float(value.get("VALUE")),
                    "open": value.get("CURRENT_DAY_OPEN"),
                    "high": value.get("CURRENT_DAY_HIGH"),
                    "low": value.get("CURRENT_DAY_LOW"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "CoinDesk_Data_API"
                }
        
        return None

    except Exception as e:
        print(f"Lỗi khi crawl {coin_symbol}: {e}")
        return None

# --- 4. QUY TRÌNH CHÍNH (ETL LOOP) ---
def etl_process(**context):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    print(f"--- BẮT ĐẦU QUÉT 20 MÃ TRÊN COINDESK ---")
    
    count = 0
    for coin in TOP_20_COINS:
        data = crawl_coindesk_by_instrument(coin)
        
        if data:
            producer.send(KAFKA_TOPIC, value=data)
            print(f"-> [OK] Lấy được giá {coin}: {data['price']}")
            count += 1
        else:
            print(f"-> [FAIL] Không lấy được dữ liệu cho {coin}")
            
    producer.flush()
    print(f"--- KẾT THÚC: Lấy thành công {count}/20 mã ---")

# --- 5. ĐỊNH NGHĨA DAG ---
default_args = {
    'owner': 'minh',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'coindesk_20_coins_dag',
    default_args=default_args,
    schedule='*/1 * * * *',
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='crawl_coindesk_task',
        python_callable=etl_process
    )