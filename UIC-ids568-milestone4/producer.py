from kafka import KafkaProducer
import json
import numpy as np
import time
import argparse
import logging
from datetime import datetime, timedelta
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("KafkaMLProducer")

# 固定随机种子，保证可复现
np.random.seed(42)

# 事件配置（与合成数据一致）
USER_IDS = np.arange(1, 100001)
PRODUCT_IDS = np.arange(1, 1000001)
PRODUCT_CATEGORIES = ["electronics", "clothing", "home", "beauty", "food"]
DEVICE_TYPES = ["mobile", "pc", "tablet", "smart_tv"]
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)
DATE_RANGE = (END_DATE - START_DATE).days

def create_kafka_producer(bootstrap_servers: str = "localhost:9092"):
    """创建Kafka Producer，JSON序列化"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="1",  # 确认leader写入成功，平衡可靠性和性能
            retries=3,  # 重试3次
            batch_size=16384,  # 批处理大小
            linger_ms=5,  # 批处理延迟
            buffer_memory=33554432  # 缓冲区内存
        )
        logger.info(f"Kafka Producer created successfully, bootstrap_servers: {bootstrap_servers}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {str(e)}", exc_info=True)
        raise SystemExit(1)

def generate_ml_event():
    """生成单条ML流事件（电商用户行为）"""
    # 业务时间（模拟事件发生时间）
    business_ts = START_DATE + timedelta(days=np.random.randint(0, DATE_RANGE))
    # 生成事件
    event = {
        "event_id": np.random.randint(1, 10**12),  # 唯一事件ID
        "user_id": int(np.random.choice(USER_IDS)),
        "product_id": int(np.random.choice(PRODUCT_IDS)),
        "product_category": np.random.choice(PRODUCT_CATEGORIES),
        "device_type": np.random.choice(DEVICE_TYPES),
        "click_duration": round(np.random.exponential(scale=10), 2),
        "page_views": int(np.random.poisson(lam=5)),
        "purchase_amt": round(np.random.lognormal(mean=2, sigma=1), 2),
        "business_ts": business_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "producer_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 生产者生成时间
    }
    return event

def produce_events(producer, topic: str, rate: int, duration: int, pattern: str = "steady"):
    """
    生产流事件
    :param producer: Kafka Producer
    :param topic: Kafka主题
    :param rate: 基础速率（条/秒）
    :param duration: 生产时长（秒）
    :param pattern: 事件模式：steady（稳态）/burst（突发）/random（随机）
    """
    start_time = time.time()
    total_events = 0
    burst_start = int(duration * 0.3)  # 突发开始时间（30%时长）
    burst_end = int(duration * 0.5)    # 突发结束时间（50%时长）
    burst_rate = rate * 10             # 突发速率（基础速率的10倍）

    logger.info(f"Starting event production: topic={topic}, base_rate={rate}eps, duration={duration}s, pattern={pattern}")
    logger.info(f"Burst pattern: start={burst_start}s, end={burst_end}s, burst_rate={burst_rate}eps")

    while time.time() - start_time < duration:
        current_time = time.time() - start_time
        # 根据模式选择当前速率
        if pattern == "burst":
            if burst_start <= current_time <= burst_end:
                current_rate = burst_rate
            else:
                current_rate = rate
        elif pattern == "random":
            current_rate = int(rate * np.random.uniform(0.5, 2.0))  # 0.5~2倍基础速率
        else:  # steady
            current_rate = rate

        # 生产当前速率的事件
        events_per_second = current_rate
        start_loop = time.time()
        for _ in range(events_per_second):
            event = generate_ml_event()
            # 发送到Kafka，按user_id分区（避免数据倾斜）
            producer.send(
                topic=topic,
                value=event,
                key=str(event["user_id"]).encode("utf-8")
            )
            total_events += 1
            if total_events % 1000 == 0:
                logger.info(f"Produced {total_events:,} events, current rate: {current_rate}eps, elapsed time: {current_time:.1f}s")

        # 控制速率（避免循环过快）
        elapsed_loop = time.time() - start_loop
        if elapsed_loop < 1:
            time.sleep(1 - elapsed_loop)

    # 刷新生产者，确保所有事件发送完成
    producer.flush()
    end_time = time.time()
    avg_rate = total_events / (end_time - start_time)
    logger.info(f"Event production completed! Total events: {total_events:,}, Avg rate: {avg_rate:.1f}eps, Total time: {end_time - start_time:.1f}s")

def main():
    # 命令行参数解析
    parser = argparse.ArgumentParser(description="Kafka Producer for ML Streaming Events")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default="ml_stream", help="Kafka topic to produce")
    parser.add_argument("--rate", type=int, default=100, help="Base event rate (events/second)")
    parser.add_argument("--duration", type=int, default=60, help="Production duration (seconds)")
    parser.add_argument("--pattern", type=str, default="steady", choices=["steady", "burst", "random"], help="Event pattern")
    args = parser.parse_args()

    # 主执行流程
    try:
        producer = create_kafka_producer(args.bootstrap_servers)
        produce_events(producer, args.topic, args.rate, args.duration, args.pattern)
        producer.close()
        logger.info("Kafka Producer closed successfully")
    except Exception as e:
        logger.error(f"Producer execution failed: {str(e)}", exc_info=True)
        raise SystemExit(1)

if __name__ == "__main__":
    main()