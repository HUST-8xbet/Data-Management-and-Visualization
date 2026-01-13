import pytest
from src.crawler.coindesk_crawler import crawl_coindesk

def test_crawl_coindesk(monkeypatch):
    # Mock requests.get để test mà không gọi API thật
    class MockResponse:
        def json(self):
            return {"Data": {"BTC-USD": {"INSTRUMENT": "BTC-USD", "VALUE": 50000.0}}}
        def raise_for_status(self):
            pass
    
    def mock_get(*args, **kwargs):
        return MockResponse()
    
    monkeypatch.setattr("requests.Session.get", mock_get)
    data = crawl_coindesk()
    assert data is not None
    assert len(data) > 0
    