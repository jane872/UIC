# MLOps Milestone4: Distributed & Streaming Pipeline
PySpark-based distributed feature engineering pipeline (required part), fully meets the assignment requirements: 10M+ synthetic data generation, local/distributed performance comparison, partition/shuffle optimization, deterministic output.

## Environment Requirements
- Python 3.9+/3.14 (tested on 3.14)
- PySpark 3.5.1
- OpenJDK 11 (Kafka/PySpark dependency)
- Other dependencies: numpy, matplotlib, json, shutil

## Quick Setup
### 1. Create Virtual Environment (avoid package conflict)
```bash
# Create virtual environment
python3 -m venv mlops-venv
# Activate virtual environment (macOS/Linux)
source mlops-venv/bin/activate
# Install all dependencies
pip install pyspark==3.5.1 numpy matplotlib