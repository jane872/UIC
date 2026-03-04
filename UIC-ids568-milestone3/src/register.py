import mlflow
import mlflow.sklearn
import os
import dotenv
from mlflow.exceptions import MlflowException

# 加载环境变量
dotenv.load_dotenv()
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "Iris_Logistic_Regression")
# 配置MLflow
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

def register_model(run_id: str, model_stage: str = "Staging", model_desc: str = None) -> dict:
    """
    将MLflow运行中的模型注册到Model Registry，并设置阶段
    :param run_id: MLflow run_id
    :param model_stage: 初始阶段（Staging/Production/Archived）
    :param model_desc: 模型版本描述
    :return: 注册结果字典（版本号、阶段、名称）
    """
    try:
        # 从run_id获取模型并注册
        model_uri = f"runs:/{run_id}/model_artifacts/lr_model_{run_id}"
        registered_model = mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)
        model_version = registered_model.version

        # 为模型版本添加描述和标签
        client = mlflow.MlflowClient()
        if model_desc:
            client.update_model_version(
                name=MODEL_NAME,
                version=model_version,
                description=model_desc
            )
        # 设置模型阶段（None→Staging）
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=model_version,
            stage=model_stage,
            archive_existing_versions=True  # 归档同阶段旧版本
        )
        # 为版本添加标签
        client.set_model_version_tag(
            name=MODEL_NAME,
            version=model_version,
            key="model_type",
            value="LogisticRegression"
        )

        return {
            "model_name": MODEL_NAME,
            "model_version": model_version,
            "stage": model_stage,
            "run_id": run_id,
            "status": "success"
        }
    except MlflowException as e:
        raise Exception(f"模型注册失败：{str(e)}")
    except Exception as e:
        raise Exception(f"未知错误：{str(e)}")

def promote_model_to_production(model_name: str, model_version: str, promo_desc: str) -> dict:
    """
    将模型从Staging→Production（文档要求的阶段推进）
    """
    client = mlflow.MlflowClient()
    try:
        client.transition_model_version_stage(
            name=model_name,
            version=model_version,
            stage="Production",
            archive_existing_versions=True
        )
        client.update_model_version(
            name=model_name,
            version=model_version,
            description=promo_desc
        )
        return {
            "model_name": model_name,
            "model_version": model_version,
            "stage": "Production",
            "status": "promoted"
        }
    except MlflowException as e:
        raise Exception(f"模型升级失败：{str(e)}")

if __name__ == "__main__":
    # 测试注册：替换为实际的run_id
    test_run_id = "your_run_id_here"
    register_result = register_model(
        run_id=test_run_id,
        model_stage="Staging",
        model_desc="基础超参数C=1.0，solver=liblinear，鸢尾花分类模型"
    )
    print(f"模型注册结果：{register_result}")
    # 测试升级到生产环境
    # promote_result = promote_model_to_production(MODEL_NAME, register_result["model_version"], "线上验证通过，准确率96.67%")
    # print(f"模型升级结果：{promote_result}")