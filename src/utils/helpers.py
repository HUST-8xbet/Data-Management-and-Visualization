import os
import yaml
from dotenv import load_dotenv

load_dotenv()  # Load .env vars

def load_config(config_path=None):
    """Load YAML config file."""
    if config_path is None:
        # Xác định đường dẫn tuyệt đối tới config.yaml dựa trên vị trí file helpers.py
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        config_path = os.path.join(base_dir, "config/config.yaml")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Override với environment variables nếu có
    config['api']['api_key'] = os.getenv('API_KEY', config['api'].get('api_key', ''))
    config['api']['instruments'] = os.getenv('INSTRUMENTS', config['api'].get('instruments', []))
    
    return config

def get_retry_session():
    """Create requests session with retry."""
    from requests import Session
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    session = Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session