import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fastapi.testclient import TestClient
from src.serving.app import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_predict_fraud():
    response = client.post("/predict", json={
        "amount": 4500.00,
        "hour": 2,
        "day_of_week": 1,
        "merchant": "Amazon",
        "category": "electronics"
    })
    assert response.status_code == 200
    assert response.json()["is_fraud"] == True
    assert response.json()["risk_level"] == "HIGH"

def test_predict_legit():
    response = client.post("/predict", json={
        "amount": 45.00,
        "hour": 14,
        "day_of_week": 1,
        "merchant": "Walmart",
        "category": "grocery"
    })
    assert response.status_code == 200
    assert response.json()["is_fraud"] == False
    assert response.json()["risk_level"] == "LOW"