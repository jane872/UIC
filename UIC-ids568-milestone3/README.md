# IDS568 Milestone3: Workflow Automation & Experiment Tracking
Repository: ids568-milestone3
MLOps Module4 - End-to-End ML Pipeline via Airflow + CI/CD + MLflow

## Project Architecture
Airflow DAG: dags/train_pipeline.py → Orchestrates Preprocess → Train → Register.

Core Scripts: src/ → Modules for preprocessing, training, registration, and validation.

CI/CD: .github/workflows/ → GitHub Actions for automated training and Quality Gate verification.

MLflow: Experiment tracking and Model Registry (None → Staging → Production).

Dependencies: requirements.txt → Pinned versions to ensure reproducibility across environments.

## Key Design Principles
### DAG Idempotency & Lineage Assurance
Data Versioning: Preprocessed data is tagged with timestamps to prevent overwriting during reruns.

Stateless Tasks: Tasks are independent of previous runs (depends_on_past=False).

End-to-End Lineage: MLflow records the full association between code, data, hyperparameters, and metrics.

### CI-based Model Governance
Quality Gates: Hard thresholds (Accuracy ≥ 90%, F1 ≥ 85%, AUC ≥ 95%); pipeline fails if criteria are not met.

Automated Registration: Validated models are automatically registered to the MLflow "Staging" stage.

Environment Consistency: CI environments utilize the same Python versions and dependency packages as local development.

### Experiment Tracking Methodology
Consistent Naming: All runs are grouped under the iris_classification experiment.

Comprehensive Logging: Full capture of hyperparameters, evaluation metrics, data versions, model artifacts, and run tags.

Systematic Optimization: 5+ experimental iterations focused on the regularization parameter C and the solver type.

## Deployment & Execution Steps
### 1. Local Environment Setup
```bash
# Clone the repository
git clone https://github.com/jane872/ids568-milestone3.git
cd ids568-milestone3
# Install dependencies
pip install -r requirements.txt
# Initialize Airflow
airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
# Start Airflow and MLflow Services
airflow webserver --port 8080
airflow scheduler
mlflow server --host 0.0.0.0 --port 5000