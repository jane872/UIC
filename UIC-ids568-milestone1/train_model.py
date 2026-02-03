from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
import joblib

# 加载数据集并训练模型
iris = load_iris()
X, y = iris.data, iris.target
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X, y)

# 保存模型为model.pkl
joblib.dump(model, "model.pkl")
print("Model saved as model.pkl")