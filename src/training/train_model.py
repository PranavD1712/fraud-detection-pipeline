import pandas as pd
import numpy as np
import mlflow
import mlflow.xgboost
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, precision_score, recall_score, f1_score
from sklearn.preprocessing import LabelEncoder
import pickle
import os

# generating training data
np.random.seed(42)
n_samples = 10000
n_fraud = int(n_samples * 0.05)
n_legit = n_samples - n_fraud

merchants = ['Amazon', 'Walmart', 'Netflix', 'Shell', 'McDonalds', 'Apple', 'Uber', 'Zara']
categories = ['electronics', 'grocery', 'entertainment', 'fuel', 'food', 'subscription', 'travel', 'clothing']

def generate_data(n, is_fraud):
    if is_fraud:
        amount = np.random.uniform(800, 5000, n)
        hour = np.random.choice([1,2,3,4], n)
    else:
        amount = np.random.uniform(5, 500, n)
        hour = np.random.randint(8, 22, n)
    return pd.DataFrame({
        'amount' : amount,
        'hour' : hour,
        'day_of_week' : np.random.randint(0, 7, n),
        'merchant' : np.random.choice(merchants, n),
        'category' : np.random.choice(categories, n),
        'is_high_amount' : (amount > 500).astype(int),
        'is_night_hour' : ((hour >= 0) & (hour <= 5)).astype(int),
        'is_fraud' : int(is_fraud)
    })

df = pd.concat([generate_data(n_legit, False), generate_data(n_fraud, True)]).sample(frac=1).reset_index(drop=True)

# Encoding Categoricals

le_merchant = LabelEncoder()
le_category = LabelEncoder()

df['merchant_enc'] = le_merchant.fit_transform(df['merchant'])
df['category_enc'] = le_category.fit_transform(df['category'])

features = ['amount', 'hour', 'day_of_week', 'is_high_amount', 'is_night_hour', 'merchant_enc', 'category_enc']
X = df[features]
Y = df['is_fraud']

x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=42, stratify=Y)

print(f"Training samples: {len(x_train)} | Test samples: {len(x_test)}")
print(f"Fraud rate: {Y.mean():.2%}")

# Training with mlflow tracking
mlflow.set_experiment("fraud-detection")

with mlflow.start_run(run_name = "xgboost-baseline"):
    params = {
        "n_estimators": 100,
        "max_depth": 6,
        "learning_rate": 0.1,
        "scale_pos_weight" : n_legit / n_fraud,
        "random_state" : 42
    } 

model = XGBClassifier(**params)
model.fit(x_train, y_train)

y_pred = model.predict(x_test)
y_prob = model.predict_proba(x_test)[:, 1]

auc = roc_auc_score(y_test, y_prob)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)

# Log to MLflow
mlflow.log_params(params)
mlflow.log_metric("auc", auc)
mlflow.log_metric("precision", precision)
mlflow.log_metric("f1", f1)
mlflow.xgboost.log_model(model, "model")

print("\n=== Model Performance ===")
print(f"AUC-ROC:   {auc:.4f}")
print(f"Precision: {precision: .4f}")
print(f"Recall:    {recall:.4f}")
print(f"F1 Score:  {f1:.4f}")
print(classification_report(y_test, y_pred, target_names=['Legit', 'Fraud']))

os.makedirs("models", exist_ok = True)
with open("models/fraud_model.pkl", "wb") as f:
    pickle.dump(model, f)
with open("models/label_encoders.pkl", "wb") as f:
    pickle.dump({'merchant': le_merchant, 'category': le_category}, f)

print("\nModel saved to models\fraud_model.pkl")
