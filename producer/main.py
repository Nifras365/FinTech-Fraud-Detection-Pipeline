import json
import logging
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import get_config
from generator import TransactionGenerator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("producer")


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        linger_ms=100,
        acks="all",
    )


def run() -> None:
    cfg = get_config()
    generator = TransactionGenerator(cfg)
    producer = build_producer(cfg.kafka_bootstrap_servers)
    sleep_seconds = max(0.01, 1.0 / cfg.tx_per_second)

    logger.info("Producer started. topic=%s bootstrap=%s", cfg.kafka_topic, cfg.kafka_bootstrap_servers)

    try:
        while True:
            tx = generator.generate()
            future = producer.send(cfg.kafka_topic, tx)
            result = future.get(timeout=10)
            logger.info(
                "published topic=%s partition=%s offset=%s payload=%s",
                result.topic,
                result.partition,
                result.offset,
                tx,
            )
            time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    except KafkaError as err:
        logger.exception("Kafka error: %s", err)
        raise
    except Exception as err:
        logger.exception("Unexpected producer failure: %s", err)
        raise
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)
        logger.info("Producer shut down cleanly.")


if __name__ == "__main__":
    run()
