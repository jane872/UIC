import joblib
import os
from google.cloud import storage

# 模型加载逻辑：优先从本地加载，无则从GCS下载
MODEL_FILENAME = "model.pkl"
model = None

def load_model():
    global model
    # 本地存在模型则直接加载
    if os.path.exists(MODEL_FILENAME):
        model = joblib.load(MODEL_FILENAME)
        return
    # 本地无模型则从GCS下载（需提前上传model.pkl到GCS桶）
    bucket_name = "your-gcs-bucket-name"  # 替换为你的GCS桶名
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(MODEL_FILENAME)
    blob.download_to_filename(MODEL_FILENAME)
    model = joblib.load(MODEL_FILENAME)

# Cloud Function入口函数
def predict(request):
    # 首次调用加载模型（冷启动）
    if model is None:
        load_model()
    
    # 解析请求数据
    request_json = request.get_json()
    if not request_json:
        return {"error": "No input data provided"}, 400
    
    # 提取特征
    features = [[
        request_json["sepal_length"],
        request_json["sepal_width"],
        request_json["petal_length"],
        request_json["petal_width"]
    ]]

    # 预测并返回结果
    pred_class = model.predict(features)[0]
    pred_conf = max(model.predict_proba(features)[0])
    return {
        "prediction": int(pred_class),
        "confidence": float(pred_conf)
    }