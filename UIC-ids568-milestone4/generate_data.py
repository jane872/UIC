import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import argparse
import os
import shutil
from datetime import datetime, timedelta

# 固定随机种子，保证数据生成可复现（作业核心要求）
SPARK_SEED = 42
# 作业默认配置：1000万行、8分区、无数据倾斜
DEFAULT_ROWS = 10_000_000
DEFAULT_PARTITIONS = 8
DEFAULT_SKEW_RATIO = 0.0

def create_spark_session(n_partitions: int):
    """创建Spark会话，配置Kryo序列化解决Python3.14序列化问题"""
    spark = SparkSession.builder
    spark = spark.appName("MLOps-Milestone4-DataGen")
    spark = spark.master("local[*]")
    spark = spark.config("spark.sql.session.timeZone", "UTC")
    spark = spark.config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark = spark.config("spark.sql.randomSeed", SPARK_SEED)
    spark = spark.config("spark.default.parallelism", n_partitions)
    spark = spark.config("spark.sql.shuffle.partitions", n_partitions)
    spark = spark.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark = spark.config("spark.kryo.registrationRequired", "false")
    spark = spark.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # 关闭冗余日志，只显示关键信息
    return spark

def generate_synthetic_data(spark, n_rows: int, n_partitions: int, skew_ratio: float = 0.0):
    """
    Spark原生API生成合成数据，无Python本地大数组/嵌套函数
    解决：序列化失败、栈溢出、缩进错误三大核心问题
    """
    # 1. 分布式生成行ID，无本地内存占用，避免栈溢出
    df = spark.range(n_rows, numPartitions=n_partitions)
    df = df.withColumnRenamed("id", "row_id")

    # 2. 业务配置：模拟电商用户行为数据（与作业特征工程输入匹配）
    USER_MIN, USER_MAX = 1, 100_000
    PROD_MIN, PROD_MAX = 1, 1_000_000
    START_TS = datetime(2024, 1, 1).timestamp()
    END_TS = datetime(2024, 12, 31).timestamp()
    TS_RANGE = END_TS - START_TS
    DEVICE_TYPES = ["mobile", "pc", "tablet", "smart_tv"]
    PROD_CATS = ["electronics", "clothing", "home", "beauty", "food"]

    # 3. 生成核心数值/标识特征（固定种子，保证可复现）
    df = df.withColumn("user_id", F.floor(F.rand(seed=SPARK_SEED) * (USER_MAX - USER_MIN)) + USER_MIN)
    df = df.withColumn("product_id", F.floor(F.rand(seed=SPARK_SEED+1) * (PROD_MAX - PROD_MIN)) + PROD_MIN)
    df = df.withColumn("click_ts", F.to_timestamp(START_TS + F.rand(seed=SPARK_SEED+2) * TS_RANGE))
    df = df.withColumn("click_duration", F.round(F.exp(F.rand(seed=SPARK_SEED+3) * 2.3) - 1, 2))
    df = df.withColumn("page_views", F.floor(F.rand(seed=SPARK_SEED+4) * 15).cast("int"))
    df = df.withColumn("purchase_amt", F.round(F.exp(F.rand(seed=SPARK_SEED+5) * 2) + 1, 2))

    # 4. 生成设备类型特征（均匀分布）
    df = df.withColumn("device_rand", F.floor(F.rand(seed=SPARK_SEED+6) * len(DEVICE_TYPES)))
    df = df.withColumn("device_type", F.array(*[F.lit(d) for d in DEVICE_TYPES])[F.col("device_rand")])
    df = df.drop("device_rand")

    # 5. 生成可配置倾斜的产品品类特征（作业要求：支持数据倾斜配置）
    if 0 < skew_ratio < 1:
        df = df.withColumn("cat_rand", F.rand(seed=SPARK_SEED+7))
        # 倾斜逻辑：第一个品类占skew_ratio比例，其余均分剩余比例
        df = df.withColumn(
            "product_category",
            F.when(F.col("cat_rand") <= skew_ratio, F.lit(PROD_CATS[0]))
            .otherwise(F.array(*[F.lit(c) for c in PROD_CATS[1:]])[F.floor(F.rand(seed=SPARK_SEED+8) * (len(PROD_CATS)-1))])
        )
        df = df.drop("cat_rand")
    else:
        df = df.withColumn("cat_rand", F.floor(F.rand(seed=SPARK_SEED+7) * len(PROD_CATS)))
        df = df.withColumn("product_category", F.array(*[F.lit(c) for c in PROD_CATS])[F.col("cat_rand")])
        df = df.drop("cat_rand")

    # 6. 生成衍生时间特征（作业要求，为后续特征工程提供基础）
    df = df.withColumn("click_hour", F.hour(F.col("click_ts")))
    df = df.withColumn("click_weekday", F.dayofweek(F.col("click_ts")))
    df = df.withColumn("click_month", F.month(F.col("click_ts")))
    df = df.drop("row_id")  # 移除临时行ID

    # 7. 强制指定分区数，保证作业配置生效
    df = df.repartition(n_partitions)
    return df

def main():
    # 命令行参数解析（作业要求：可配置数据特征）
    parser = argparse.ArgumentParser(description="Synthetic Data Generator for MLOps Milestone4 (PySpark)")
    parser.add_argument("--n-rows", type=int, default=DEFAULT_ROWS, help=f"Total rows (default: {DEFAULT_ROWS:,})")
    parser.add_argument("--n-partitions", type=int, default=DEFAULT_PARTITIONS, help=f"Output partitions (default: {DEFAULT_PARTITIONS})")
    parser.add_argument("--skew-ratio", type=float, default=DEFAULT_SKEW_RATIO, help="Product category skew ratio (0-1, default: 0)")
    parser.add_argument("--output-path", type=str, default="./synthetic_data", help="Output Parquet directory path")
    args = parser.parse_args()

    # 参数合法性校验（避免无效输入，符合作业严谨性要求）
    if args.n_rows < 100_000:
        raise ValueError("n-rows must be at least 100,000 (per Milestone4 requirement)")
    if args.n_partitions < 1:
        raise ValueError("n-partitions must be at least 1")
    if not (0 <= args.skew_ratio <= 1):
        raise ValueError("skew-ratio must be between 0 and 1")

    # 删除已存在的输出路径，避免文件冲突
    if os.path.exists(args.output_path):
        shutil.rmtree(args.output_path)
        print(f"Deleted existing output directory: {args.output_path}")

    # 核心执行流程
    print(f"=== MLOps Milestone4 - Synthetic Data Generation ===")
    print(f"Config: Rows={args.n_rows:,} | Partitions={args.n_partitions} | Skew={args.skew_ratio}")
    spark = create_spark_session(args.n_partitions)
    df = generate_synthetic_data(spark, args.n_rows, args.n_partitions, args.skew_ratio)

    # 保存为Parquet格式（Spark最优格式，作业要求分布式读取）
    df.write.mode("overwrite").option("compression", "snappy").parquet(args.output_path)

    # 打印数据概览，验证生成结果（作业要求：可复现、可验证）
    print(f"\n=== Data Generation Succeeded ===")
    print(f"Output Path: {args.output_path}")
    print(f"Final Partitions: {df.rdd.getNumPartitions()}")
    print(f"Total Rows: {df.count():,}")
    print(f"\n=== Data Schema ===")
    df.printSchema()
    print(f"\n=== Sample Data (5 Rows) ===")
    df.show(5, truncate=False)

    # 关闭Spark会话，释放资源
    spark.stop()
    print(f"\n=== Spark Session Closed ===")

if __name__ == "__main__":
    main()