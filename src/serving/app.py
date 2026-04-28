import pickle
import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest
from fastapi.responses import PlainTextResponse
import time

app = FastAPI(title="Fraud Detection API", version="1.0.0")

# Load model
with open("models/fraud_model.pkl", "rb") as f:
    model = pickle.load(f)
with open("models/label_encoders.pkl", "rb") as f:
    encoders = pickle.load(f)

# Prometheus metrics
PREDICTIONS_TOTAL = Counter("fraud_predictions_total", "Total predictions", ["result"])
PREDICTION_LATENCY = Histogram("fraud_prediction_latency_seconds", "Prediction latency")
FRAUD_AMOUNT_TOTAL = Counter("fraud_amount_total", "Total fraudulent amount detected")

class Transaction(BaseModel):
    amount: float
    hour: int
    day_of_week: int
    merchant: str
    category: str

class PredictionResponse(BaseModel):
    transaction_id: str
    is_fraud: bool
    fraud_probability: float
    risk_level: str

@app.get("/")
def root():
    return {"message": "Fraud Detection API is running", "version": "1.0.0"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.post("/predict", response_model=PredictionResponse)
def predict(transaction: Transaction):
    start_time = time.time()

    # Encode categoricals
    try:
        merchant_enc = encoders['merchant'].transform([transaction.merchant])[0]
    except:
        merchant_enc = 0
    try:
        category_enc = encoders['category'].transform([transaction.category])[0]
    except:
        category_enc = 0

    is_high_amount = 1 if transaction.amount > 500 else 0
    is_night_hour = 1 if transaction.hour <= 5 else 0

    features = np.array([[
        transaction.amount,
        transaction.hour,
        transaction.day_of_week,
        is_high_amount,
        is_night_hour,
        merchant_enc,
        category_enc
    ]])

    prob = model.predict_proba(features)[0][1]
    is_fraud = bool(prob > 0.5)

    risk_level = "LOW" if prob < 0.3 else "MEDIUM" if prob < 0.7 else "HIGH"

    # Update Prometheus metrics
    PREDICTIONS_TOTAL.labels(result="fraud" if is_fraud else "legit").inc()
    PREDICTION_LATENCY.observe(time.time() - start_time)
    if is_fraud:
        FRAUD_AMOUNT_TOTAL.inc(transaction.amount)

    import uuid
    return PredictionResponse(
        transaction_id=str(uuid.uuid4()),
        is_fraud=is_fraud,
        fraud_probability=round(float(prob), 4),
        risk_level=risk_level
    )

@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    return generate_latest()