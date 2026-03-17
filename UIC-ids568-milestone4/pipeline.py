import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import argparse
import os
import logging
import time
import json
from datetime import datetime
import sys

# 配置日志（作业要求：错误处理和日志）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"pipeline_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("DistributedFeatureEngineering-Milestone4")

# 固定随机种子（作业要求：确定性输出+可复现）
SPARK_SEED = 42
# 特征工程常量
NUMERICAL_COLS = ["click_duration", "page_views", "purchase_amt"]
CATEGORICAL_COLS = ["device_type", "product_category"]
PARTITION_KEY = "user_id"  # 物理分区键（与窗口函数分区键一致，优化并行性）

def create_spark_session(mode: str, n_workers: int = 4):
    """创建Spark会话，Kryo序列化，优化并行度"""
    spark_builder = SparkSession.builder
    spark_builder = spark_builder.appName("DistributedFeatureEngineering-Milestone4")
    spark_builder = spark_builder.config("spark.sql.session.timeZone", "UTC")
    spark_builder = spark_builder.config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark_builder = spark_builder.config("spark.sql.randomSeed", SPARK_SEED)
    spark_builder = spark_builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark_builder = spark_builder.config("spark.kryo.registrationRequired", "false")
    spark_builder = spark_builder.config("spark.executor.memory", "4g")
    spark_builder = spark_builder.config("spark.driver.memory", "4g")
    spark_builder = spark_builder.config("spark.sql.adaptive.enabled", "true")
    spark_builder = spark_builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # 模式配置：本地（单核心）/分布式（多核心）
    if mode == "local":
        spark_builder = spark_builder.master("local[1]")
        logger.info(f"Spark Session - LOCAL MODE (1 core, baseline)")
    elif mode == "distributed":
        spark_builder = spark_builder.master(f"local[{n_workers}]")
        # Shuffle优化：分区数=2*Worker数（作业要求：Shuffle优化）
        spark_builder = spark_builder.config("spark.sql.shuffle.partitions", n_workers * 2)
        spark_builder = spark_builder.config("spark.default.parallelism", n_workers * 2)
        logger.info(f"Spark Session - DISTRIBUTED MODE ({n_workers} workers)")
    else:
        raise ValueError(f"Invalid mode: {mode} (must be 'local' or 'distributed')")

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_data(spark, input_path: str, n_partitions: int):
    """加载合成数据，添加物理分区（作业要求：分区优化）"""
    try:
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Synthetic data not found: {input_path}")
        df = spark.read.parquet(input_path)
        df_count = df.count()
        if df_count == 0:
            raise ValueError(f"Input data is empty: {input_path}")
        
        # 核心修复：按PARTITION_KEY重分区，确保窗口函数分布式执行
        df_repartitioned = df.repartition(n_partitions, PARTITION_KEY)
        logger.info(
            f"Loaded & Repartitioned Data | "
            f"Path: {input_path} | "
            f"Rows: {df_count:,} | "
            f"Partitions: {df_repartitioned.rdd.getNumPartitions()} | "
            f"Partition Key: {PARTITION_KEY}"
        )
        return df_repartitioned
    except Exception as e:
        logger.error(f"Data load failed: {str(e)}", exc_info=True)
        sys.exit(1)

def data_cleaning(df):
    """数据清洗：过滤异常值、去重（作业要求）"""
    logger.info("Starting data cleaning...")
    df_clean = df.filter((F.col("purchase_amt") > 0) & (F.col("click_duration") > 0))
    df_clean = df_clean.dropDuplicates(["user_id", "product_id", "click_ts"])
    df_clean = df_clean.filter(F.col("page_views") >= 0)
    
    raw_count = df.count()
    clean_count = df_clean.count()
    logger.info(f"Cleaning completed | Remaining: {clean_count:,} | Removed: {raw_count - clean_count:,} ({(raw_count - clean_count)/raw_count*100:.2f}%)")
    return df_clean

def feature_engineering(df, n_partitions: int):
    """
    纯Spark SQL原生特征工程（无pyspark.ml依赖，无distutils）
    包含：物理分区优化+窗口函数并行执行（作业核心要求）
    """
    logger.info("Starting distributed feature engineering...")
    # 窗口函数（与物理分区键一致，确保分布式并行执行）
    w_user = Window.partitionBy(PARTITION_KEY)  # 与物理分区键统一
    w_user_month = Window.partitionBy(PARTITION_KEY, "click_month")
    w_product = Window.partitionBy("product_id")
    w_category = Window.partitionBy("product_category")
    w_device_cat = Window.partitionBy("device_type", "product_category")

    # 1. 聚合特征（用户/产品/品类/交叉维度）
    df_feat = df.withColumn("user_total_views", F.sum("page_views").over(w_user))
    df_feat = df_feat.withColumn("user_avg_purchase", F.avg("purchase_amt").over(w_user))
    df_feat = df_feat.withColumn("user_click_count", F.count("click_ts").over(w_user))
    df_feat = df_feat.withColumn("user_month_views", F.sum("page_views").over(w_user_month))
    df_feat = df_feat.withColumn("user_month_purchase", F.avg("purchase_amt").over(w_user_month))
    df_feat = df_feat.withColumn("product_avg_purchase", F.avg("purchase_amt").over(w_product))
    df_feat = df_feat.withColumn("product_click_count", F.count("click_ts").over(w_product))
    df_feat = df_feat.withColumn("category_click_count", F.count("click_ts").over(w_category))
    df_feat = df_feat.withColumn("device_cat_click_count", F.count("click_ts").over(w_device_cat))

    # 2. 数值交叉特征
    df_feat = df_feat.withColumn("click_views_product", F.col("click_duration") * F.col("page_views"))
    df_feat = df_feat.withColumn("purchase_views_ratio", F.col("purchase_amt") / (F.col("page_views") + 1))  # 避免除0

    # 3. 类别特征编码（StringIndexer替代，确定性）
    for cat_col in CATEGORICAL_COLS:
        freq_window = Window.orderBy(F.col(cat_col))
        df_feat = df_feat.withColumn(f"{cat_col}_idx", F.dense_rank().over(freq_window) - 1)

    # 4. 数值特征归一化（Spark SQL原生实现，确定性）
    for num_col in NUMERICAL_COLS:
        stats = df_feat.select(F.min(num_col).alias("min_val"), F.max(num_col).alias("max_val")).collect()[0]
        min_val = stats["min_val"]
        max_val = stats["max_val"]
        # 用F.lit()确保Column类型匹配
        df_feat = df_feat.withColumn(
            f"{num_col}_norm",
            F.when(F.lit(max_val != min_val), (F.col(num_col) - F.lit(min_val)) / (F.lit(max_val) - F.lit(min_val))).otherwise(F.lit(0.0))
        )

    # 5. 最终特征列选择（作业要求：确定性输出）
    final_feature_cols = [
        "user_id", "product_id",
        "device_type_idx", "product_category_idx",
        "click_duration_norm", "page_views_norm", "purchase_amt_norm",
        "user_total_views", "user_avg_purchase", "user_click_count",
        "user_month_views", "user_month_purchase",
        "product_avg_purchase", "product_click_count",
        "category_click_count", "device_cat_click_count",
        "click_views_product", "purchase_views_ratio"
    ]
    df_final = df_feat.select(final_feature_cols).distinct()
    
    # 保持输出分区数与输入一致，优化后续使用
    df_final = df_final.repartition(n_partitions, PARTITION_KEY)
    logger.info(f"Feature engineering completed | Features: {len(final_feature_cols)} | Rows: {df_final.count():,} | Output Partitions: {df_final.rdd.getNumPartitions()}")
    return df_final

def save_features(df, output_path: str):
    """保存特征集，Parquet格式（作业要求）"""
    try:
        if os.path.exists(output_path):
            import shutil
            shutil.rmtree(output_path)
        # 按物理分区键保存，优化后续读取
        df.write.mode("overwrite").option("compression", "snappy").parquet(output_path)
        logger.info(f"Features saved | Path: {output_path} | Partitions: {df.rdd.getNumPartitions()}")
    except Exception as e:
        logger.error(f"Save failed: {str(e)}", exc_info=True)
        sys.exit(1)

def collect_metrics(spark, start_time: float, end_time: float, df):
    """收集性能指标（作业要求：本地/分布式定量对比）"""
    df_count = df.count()
    master = spark.sparkContext.master
    n_workers = master.split("[")[-1].split("]")[0] if "local[" in master else 1
    runtime = round(end_time - start_time, 2)
    shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")

    metrics = {
        "execution_mode": mode,
        "n_workers": int(n_workers),
        "total_runtime_seconds": runtime,
        "total_rows_processed": df_count,
        "total_partitions": df.rdd.getNumPartitions(),
        "shuffle_partitions": shuffle_partitions,
        "partition_key": PARTITION_KEY,
        "executor_memory": spark.conf.get("spark.executor.memory"),
        "driver_memory": spark.conf.get("spark.driver.memory"),
        "throughput_rows_per_second": round(df_count / runtime, 2),
        "shuffle_volume_mb": 0.0 if mode == "local" else f"See Spark UI (Shuffle Partitions: {shuffle_partitions})",
        "execution_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    logger.info(f"Metrics collected | {json.dumps(metrics, indent=2)}")
    return metrics

def save_metrics(metrics, output_path: str):
    """保存指标到JSON（作业要求：性能对比）"""
    try:
        with open(output_path, "w") as f:
            json.dump(metrics, f, indent=4)
        logger.info(f"Metrics saved | Path: {output_path}")
    except Exception as e:
        logger.error(f"Metrics save failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    # 命令行参数解析
    parser = argparse.ArgumentParser(description="Distributed Feature Engineering (PySpark SQL Native)")
    parser.add_argument("--mode", type=str, required=True, choices=["local", "distributed"], help="Execution mode")
    parser.add_argument("--n-workers", type=int, default=4, help="Workers for distributed mode")
    parser.add_argument("--input-path", type=str, default="./synthetic_data", help="Synthetic data path")
    parser.add_argument("--output-path", type=str, default="./feature_output", help="Features output path")
    parser.add_argument("--metrics-path", type=str, default="./pipeline_metrics.json", help="Metrics output path")
    parser.add_argument("--n-partitions", type=int, default=8, help="Number of physical partitions (match data generation)")
    args = parser.parse_args()
    mode = args.mode

    # 主执行流程（作业要求：分布式转换作业）
    try:
        logger.info("=== Starting Distributed Feature Engineering Pipeline ===")
        spark = create_spark_session(mode, args.n_workers)
        start_time = time.time()

        # 加载数据并按PARTITION_KEY重分区（核心优化）
        df_raw = load_data(spark, args.input_path, args.n_partitions)
        df_clean = data_cleaning(df_raw)
        df_features = feature_engineering(df_clean, args.n_partitions)
        save_features(df_features, args.output_path)

        end_time = time.time()
        metrics = collect_metrics(spark, start_time, end_time, df_features)
        save_metrics(metrics, args.metrics_path)

        logger.info("=== Pipeline Completed Successfully ===")
        logger.info(f"Runtime: {metrics['total_runtime_seconds']}s | Throughput: {metrics['throughput_rows_per_second']} rows/s")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark Session Closed")