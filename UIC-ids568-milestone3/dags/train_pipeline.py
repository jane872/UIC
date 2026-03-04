from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys

# 将项目根目录加入Python路径（解决Airflow调度时的模块导入问题）
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.preprocess import preprocess_data
from src.train import train_model
from src.register import register_model
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()
default_args = {
    "owner": "IDS568_Milestone3",
    "depends_on_past": False,  # 不依赖过去的运行（幂等性）
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    # 文档要求：重试配置
    "retries": 3,  # 最大重试次数
    "retry_delay": timedelta(minutes=2),  # 重试间隔
}

# 失败回调函数（文档要求：失败时通知/清理）
def failure_callback(context):
    """任务失败时的回调：打印日志+记录失败信息"""
    task_instance = context["task_instance"]
    dag_run = context["dag_run"]
    fail_msg = f"DAG:{dag_run.dag_id} 任务:{task_instance.task_id} 运行失败，执行ID:{dag_run.run_id}"
    print(fail_msg)
    # 可扩展：添加邮件/钉钉/Slack通知
    with open("./airflow_failure_logs.txt", "a") as f:
        f.write(f"{fail_msg}\n")

# 定义超参数（可修改为不同值，执行多次实验）
TRAIN_HYPERPARAMS = {
    "C": 1.0,
    "solver": "liblinear",
    "max_iter": 100
}

# 定义DAG
with DAG(
    dag_id="iris_ml_pipeline",  # DAG唯一ID
    default_args=default_args,
    description="IDS568 Milestone3: Preprocess→Train→Register ML Pipeline",
    schedule_interval=None,  # 手动触发（可改为@daily等定时调度）
    catchup=False,  # 不追跑历史任务
    tags=["MLOps", "IDS568", "MLflow", "Airflow"],
) as dag:

    # 任务1：数据预处理
    preprocess_task = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data,
        op_kwargs={"data_path": None},  # 使用内置数据集，可替换为自定义路径
        on_failure_callback=failure_callback,
        do_xcom_push=True,  # 将结果推送到XCom，供后续任务获取
    )

    # 任务2：模型训练（从XCom获取预处理数据路径）
    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
        op_kwargs={
            "data_path": "{{ ti.xcom_pull(task_ids='preprocess_data') }}",
            "hyperparams": TRAIN_HYPERPARAMS
        },
        on_failure_callback=failure_callback,
        do_xcom_push=True,  # 推送(model_path, metrics, run_id)到XCom
    )

    # 任务3：模型注册（从XCom获取run_id）
    register_task = PythonOperator(
        task_id="register_model",
        python_callable=register_model,
        op_kwargs={
            "run_id": "{{ ti.xcom_pull(task_ids='train_model')[2] }}",
            "model_stage": "Staging",
            "model_desc": f"超参数：C={TRAIN_HYPERPARAMS['C']}, solver={TRAIN_HYPERPARAMS['solver']} | 鸢尾花分类模型-Staging阶段"
        },
        on_failure_callback=failure_callback,
    )

    # 任务依赖：preprocess → train → register（文档要求的执行顺序）
    preprocess_task >> train_task >> register_task