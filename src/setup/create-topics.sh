#!/bin/bash
set -euo pipefail

TOPIC_NAME="ais-stream"
PARTITIONS=3
REPLICATION=1
BOOTSTRAP_SERVER="kafka:29092"

MAX_RETRIES=20
RETRY=0

KAFKA_TOPICS="/usr/bin/kafka-topics"

echo "Waiting for Kafka broker at $BOOTSTRAP_SERVER ..."

until $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; do
    RETRY=$((RETRY+1))
    if [ "$RETRY" -gt "$MAX_RETRIES" ]; then
        echo "❌ Kafka broker did not start in time. Exiting."
        exit 1
    fi
    echo "⏳ Waiting for Kafka ($RETRY/$MAX_RETRIES)..."
    sleep 2
done

echo "✅ Kafka is up. Creating topic: $TOPIC_NAME ..."

$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION"

echo "✅ Topic '$TOPIC_NAME' created or already exists."

echo "📜 Listing all topics:"
$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list
