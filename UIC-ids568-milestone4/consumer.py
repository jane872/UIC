from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse
import logging
import sys
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"consumer_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("PySparkStreamingConsumer")

# 定义Kafka事件Schema（与Producer一致）
EVENT_SCHEMA = StructType() \
    .add("event_id", LongType()) \
    .add("user_id", IntegerType()) \
    .add("product_id