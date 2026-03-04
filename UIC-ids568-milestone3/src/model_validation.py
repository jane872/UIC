import argparse
import mlflow
import dotenv
from mlflow.exceptions import MlflowException

# 加载环境变量
dotenv.load_dotenv()
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# 【质量门阈值】可根据业务调整
QUALITY_THRESHOLDS = {
    "accuracy": 0.90,  # 准确率≥90%
    "f1_macro": 0.85,  # F1≥85%
    "auc_ovr": 0.95    # AUC≥95%
}

def validate_model(run_id: str, thresholds: dict = QUALITY_THRESHOLDS) -> bool:
    """
    模型验证：检查指标是否达到阈值，未达标则返回False
    :param run_id: MLflow run_id
    :param thresholds: 质量门阈值
    :return: 验证通过→True，失败→False
    """
    try:
        # 获取MLflow运行的指标
        client = mlflow.MlflowClient()
        run_data = client.get_run(run_id)
        metrics = run_data.data.metrics

        # 检查所有阈值
        print(f"模型验证阈值：{thresholds}")
        print(f"模型实际指标：{metrics}")
        fail_items = []
        for metric, threshold in thresholds.items():
            if metrics.get(metric, 0) < threshold:
                fail_items.append(f"{metric}({metrics.get(metric, 0)}) < {threshold}")

        if fail_items:
            raise Exception(f"模型验证失败：{'; '.join(fail_items)}")
        print("模型验证通过，所有指标达到质量门要求！")
        return True
    except MlflowException as e:
        raise Exception(f"获取MLflow运行数据失败：{str(e)}")
    except Exception as e:
        raise Exception(f"模型验证失败：{str(e)}")

if __name__ == "__main__":
    # 命令行传参：python src/model_validation.py --run_id <your_run_id>
    parser = argparse.ArgumentParser(description="Model Validation with Quality Gates")
    parser.add_argument("--run_id", required=True, help="MLflow Run ID to validate")
    args = parser.parse_args()

    # 执行验证，失败则退出程序（触发CI/CD失败）
    try:
        validate_model(args.run_id)
    except Exception as e:
        print(str(e))
        exit(1)