import boto3
import os
import pickle
from datetime import datetime

class S3Handler:
    def __init__(self):
        self.bucket_name = os.getenv("S3_BUCKET_NAME", "fraud-detection-pipeline")
        self.region = os.getenv("AWS_REGION", "ap-south-1")
        self.mock_mode = os.getenv("AWS_MOCK", "true").lower() == "true"

        if not self.mock_mode:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=self.region
            )

    def upload_model(self, local_path: str, s3_key: str):
        if self.mock_mode:
            print(f"[MOCK S3] Uploading {local_path} → s3://{self.bucket_name}/{s3_key}")
            return {"status": "success", "location": f"s3://{self.bucket_name}/{s3_key}"}
        else:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            return {"status": "success", "location": f"s3://{self.bucket_name}/{s3_key}"}

    def upload_features(self, df, partition_date: str = None):
        if partition_date is None:
            partition_date = datetime.now().strftime("%Y-%m-%d")
        s3_key = f"features/date={partition_date}/features.parquet"

        if self.mock_mode:
            os.makedirs("data/features", exist_ok=True)
            local_path = f"data/features/features_{partition_date}.parquet"
            df.to_parquet(local_path, index=False)
            print(f"[MOCK S3] Saving features → s3://{self.bucket_name}/{s3_key}")
            print(f"[MOCK S3] Saved locally at {local_path}")
            return {"status": "success", "location": f"s3://{self.bucket_name}/{s3_key}"}
        else:
            import io
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=buffer.getvalue())
            return {"status": "success", "location": f"s3://{self.bucket_name}/{s3_key}"}

    def download_model(self, s3_key: str, local_path: str):
        if self.mock_mode:
            print(f"[MOCK S3] Downloading s3://{self.bucket_name}/{s3_key} → {local_path}")
            return local_path
        else:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            return local_path

    def list_objects(self, prefix: str = ""):
        if self.mock_mode:
            print(f"[MOCK S3] Listing s3://{self.bucket_name}/{prefix}")
            return [
                f"s3://{self.bucket_name}/models/fraud_model.pkl",
                f"s3://{self.bucket_name}/models/label_encoders.pkl",
                f"s3://{self.bucket_name}/features/date=2026-04-29/features.parquet"
            ]
        else:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]


if __name__ == "__main__":
    s3 = S3Handler()

    print("=== Testing S3 Handler (Mock Mode) ===\n")

    # Upload model
    result = s3.upload_model("models/fraud_model.pkl", "models/fraud_model.pkl")
    print(f"Upload result: {result}\n")

    # Upload encoders
    result = s3.upload_model("models/label_encoders.pkl", "models/label_encoders.pkl")
    print(f"Upload result: {result}\n")

    # List objects
    objects = s3.list_objects()
    print(f"Objects in bucket:")
    for obj in objects:
        print(f"  {obj}")

    # Upload sample features
    import pandas as pd
    import numpy as np
    sample_df = pd.DataFrame({
        'transaction_id': [f'TXN_{i}' for i in range(100)],
        'amount': np.random.uniform(5, 500, 100),
        'is_fraud': np.random.choice([0, 1], 100, p=[0.95, 0.05])
    })
    result = s3.upload_features(sample_df)
    print(f"\nFeatures upload result: {result}")
    print("\n✅ S3 Handler working correctly in mock mode!")
    print("💡 Set AWS_MOCK=false and add real credentials to use actual S3")