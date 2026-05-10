#!/bin/bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TRANSACTIONS_TOPIC="${KAFKA_TOPIC:-transactions}"
FRAUD_ALERTS_TOPIC="${FRAUD_ALERTS_TOPIC:-fraud_alerts}"

kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic "$TRANSACTIONS_TOPIC" --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic "$FRAUD_ALERTS_TOPIC" --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list
