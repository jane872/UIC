import pandas as pd
import os
import mlflow
import mlflow.sklearn
from datetime import datetime
import dotenv
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
import numpy as np

# 加载环境变量
dotenv.load_dotenv()
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "iris_classification")
# 配置MLflow
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment(EXPERIMENT_NAME)

def train_model(data_path: str, hyperparams: dict = None) -> tuple:
    """
    模型训练，自动日志到MLflow
    :param data_path: 预处理后数据路径
    :param hyperparams: 超参数字典（默认None则用基础参数）
    :return: (模型路径, 训练指标字典, MLflow run_id)
    """
    # 加载数据
    df = pd.read_csv(data_path)
    X = df.drop("target", axis=1)
    y = df["target"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 超参数默认值（支持传入不同值做实验）
    default_hp = {"C": 1.0, "solver": "liblinear", "max_iter": 100, "random_state": 42}
    hp = hyperparams if hyperparams else default_hp

    # MLflow开始运行（自动记录环境、代码版本）
    with mlflow.start_run(run_name=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as run:
        run_id = run.info.run_id
        # 训练模型
        model = LogisticRegression(**hp)
        model.fit(X_train, y_train)

        # 预测并计算指标
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)
        # 处理多分类AUC（one-vs-rest）
        auc = roc_auc_score(y_test, y_pred_proba, multi_class="ovr")
        metrics = {
            "accuracy": round(accuracy_score(y_test, y_pred), 4),
            "f1_macro": round(f1_score(y_test, y_pred, average="macro"), 4),
            "auc_ovr": round(auc, 4),
            "train_loss": round(np.mean((model.predict(X_train) - y_train)**2), 4)
        }

        # 【关键】MLflow日志：超参数、指标、数据版本、模型、标签
        mlflow.log_params(hp)
        mlflow.log_metrics(metrics)
        mlflow.log_param("data_version", os.path.basename(data_path).split("_")[2].split(".")[0])
        mlflow.log_artifact(data_path, "preprocessed_data")
        mlflow.set_tag("experiment_type", "iris_classification")
        mlflow.set_tag("run_status", "completed")
        # 保存模型到MLflow
        model_path = os.path.join("models", f"lr_model_{run_id}")
        mlflow.sklearn.save_model(model, model_path)
        mlflow.log_artifact(model_path, "model_artifacts")

    return model_path, metrics, run_id

if __name__ == "__main__":
    # 测试训练：加载最新预处理数据
    with open("./data/latest_data.txt", "r") as f:
        latest_data = f.read()
    # 基础超参数训练
    model_path, metrics, run_id = train_model(latest_data)
    print(f"训练完成，模型路径：{model_path}，指标：{metrics}，Run ID：{run_id}")