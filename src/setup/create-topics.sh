#!/bin/bash
set -e

TOPIC_NAME="ais-stream"
PARTITIONS=3
REPLICATION=1
BOOTSTRAP_SERVER="kafka:29092"

# Wait until Kafka is ready
MAX_RETRIES=20
RETRY=0
until kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; do
    RETRY=$((RETRY+1))
    if [ "$RETRY" -gt "$MAX_RETRIES" ]; then
        echo "Kafka broker did not start in time. Exiting."
        exit 1
    fi
    echo "Waiting for Kafka ($RETRY/$MAX_RETRIES)..."
    sleep 2
done

# Create topic
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
--create \
--if-not-exists \
--topic "$TOPIC_NAME" \
--partitions "$PARTITIONS" \
--replication-factor "$REPLICATION"

echo "Topic $TOPIC_NAME created successfully!"

# list topics to verify
echo "Listing all topics:"
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list
