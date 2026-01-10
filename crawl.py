import json
import os
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

def crawl_coindesk():
    api_key = os.getenv('API_KEY')
    instruments = os.getenv('INSTRUMENTS')
    url = f'https://data-api.coindesk.com/index/cc/v1/latest/tick?market=ccix&instruments={instruments}'
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)

    headers = {
        "authorization": f"Apikey {api_key}"
    }

    try:
        print(f"--- Đang trích xuất (Extract) dữ liệu cho: {instruments} ---")
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        raw_data = response.json()
        
        # Kiểm tra lỗi từ phản hồi của API
        if "Err" in raw_data and raw_data["Err"]:
            print(f"API trả về lỗi: {raw_data['Err']}")
            return None

        # Transform sơ bộ để ánh xạ vào cấu trúc dữ liệu mong muốn 
        results = []
        data_dict = raw_data.get("Data", {})
        
        for key, value in data_dict.items():
            results.append({
                "iso": key.split('-')[0],
                "instrument": value.get("INSTRUMENT"),
                "date_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "current_price": value.get("VALUE"),
                "open": value.get("CURRENT_DAY_OPEN"),
                "high": value.get("CURRENT_DAY_HIGH"),
                "low": value.get("CURRENT_DAY_LOW"),
                "close": value.get("VALUE"),
            })
            
        return results

    except Exception as e:
        print(f"Pipeline gặp lỗi khi Extract: {e}")
        return None

if __name__ == "__main__":
    # Chạy hàm crawl
    processed_data = crawl_coindesk()
    
    if processed_data:
        if not os.path.exists('data_lake'):
            os.makedirs('data_lake')
            
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        file_path = f"data_lake/raw_crypto_{timestamp}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=4)
            
        print(f"--- Đã hoàn thành bước Load vào Data Lake: {file_path} ---")
    else:
        print("--- Pipeline thất bại ở bước trích xuất dữ liệu ---")
