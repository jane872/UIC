from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import os

# 初始化FastAPI应用
app = FastAPI(title="Iris Classification Model API")

# 确定性加载模型：使用绝对路径避免路径问题
MODEL_PATH = os.path.join(os.path.dirname(__file__), "model.pkl")
model = joblib.load(MODEL_PATH)

# 定义请求数据模型（Schema验证）
class IrisRequest(BaseModel):
    sepal_length: float
    sepal_width: float
    petal_length: float
    petal_width: float

# 定义响应数据模型
class IrisResponse(BaseModel):
    prediction: int  # 预测类别：0,1,2
    confidence: float  # 预测置信度

# 预测接口
@app.post("/predict", response_model=IrisResponse)
def predict(request: IrisRequest):
    # 构造特征数组
    features = [[
        request.sepal_length,
        request.sepal_width,
        request.petal_length,
        request.petal_width
    ]]
    # 预测类别和置信度
    pred_class = model.predict(features)[0]
    pred_conf = max(model.predict_proba(features)[0])
    # 返回结果
    return {"prediction": int(pred_class), "confidence": float(pred_conf)}

# 健康检查接口
@app.get("/health")
def health_check():
    return {"status": "healthy", "model_loaded": True}