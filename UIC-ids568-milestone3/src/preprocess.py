import pandas as pd
import numpy as np
import os
from datetime import datetime
import dotenv

# 加载环境变量
dotenv.load_dotenv()
DATA_DIR = os.getenv("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)

def preprocess_data(data_path: str = None, save_version: bool = True) -> str:
    """
    数据预处理：使用sklearn内置鸢尾花数据集（通用可替换为自定义数据）
    :param data_path: 自定义数据路径（None则用内置数据集）
    :param save_version: 是否保存版本化数据
    :return: 预处理后数据的文件路径
    """
    from sklearn.datasets import load_iris
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler

    # 加载数据
    if data_path:
        df = pd.read_csv(data_path)
    else:
        iris = load_iris()
        df = pd.DataFrame(data=np.c_[iris['data'], iris['target']],
                          columns=iris['feature_names'] + ['target'])
    
    # 数据清洗：删除空值、重复值
    df = df.dropna().drop_duplicates()
    
    # 特征工程：标准化特征
    scaler = StandardScaler()
    feature_cols = [col for col in df.columns if col != 'target']
    df[feature_cols] = scaler.fit_transform(df[feature_cols])
    
    # 版本化保存（幂等性关键：时间戳+数据版本）
    if save_version:
        data_version = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(DATA_DIR, f"preprocessed_iris_{data_version}.csv")
        df.to_csv(output_path, index=False)
        # 保存最新数据路径标识（方便后续调用）
        with open(os.path.join(DATA_DIR, "latest_data.txt"), "w") as f:
            f.write(output_path)
        return output_path
    return df

if __name__ == "__main__":
    # 测试预处理
    preprocess_path = preprocess_data()
    print(f"预处理完成，数据保存至：{preprocess_path}")