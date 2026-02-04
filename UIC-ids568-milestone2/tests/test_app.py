import pytest
import requests

def test_inference_endpoint():
    # 假设本地服务运行在 5000 端口
    response = requests.post("http://localhost:5000/predict", json={"data": [1,2,3]})
    assert response.status_code == 200
    assert "prediction" in response.json()
