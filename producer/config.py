import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProducerConfig:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "transactions")
    tx_per_second: float = float(os.getenv("TX_PER_SECOND", "5"))
    fraud_probability: float = float(os.getenv("FRAUD_PROBABILITY", "0.08"))
    high_value_threshold: float = float(os.getenv("HIGH_VALUE_THRESHOLD", "5000"))
    impossible_travel_window_minutes: int = int(os.getenv("IMPOSSIBLE_TRAVEL_WINDOW_MINUTES", "10"))
    random_seed: int = int(os.getenv("RANDOM_SEED", "42"))


def get_config() -> ProducerConfig:
    return ProducerConfig()
