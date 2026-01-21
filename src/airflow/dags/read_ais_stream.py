"""
## Read AIS Stream and Store in HBase

- **Purpose**: Consume messages from Kafka topic *ais-stream* and insert each message into HBase using *happybase*.
- **Operator**: *ConsumeFromTopicOperator* from the Apache Kafka provider.
- **Processing Function**: `process_message(message)` decodes JSON, extracts `MessageID` and writes the full payload to HBase.
- **Kafka**:
    - Topic: *ais-stream*
    - Bootstrap server: *kafka:29092*
    - Consumer group: *airflow-ais-reader* (configurable via *CONSUMER_GROUP*).
- **HBase**:
    - Host: *HBASE_HOST* (default: `hbase-master`)
    - Port: *HBASE_PORT* (default: `9090`, Thrift)
    - Table: *HBASE_TABLE* (default: `ais_messages`)
    - Column Family: *HBASE_CF* (default: `cf`)
    - Row Key: value of `MessageID` in the message payload
    - Columns written:
        - `cf:json`: full JSON payload
        - `cf:ts`: ingestion timestamp in milliseconds (UTC)
- **Batching**:
    - *MAX_MESSAGES* (default: 100)
    - *MAX_BATCH_SIZE* (default: 50)
    - *POLL_TIMEOUT* seconds (default: 5)
- **Connection Caching**: HBase connection and table are cached across invocations to avoid reconnect overhead.
- **Error Handling**: Messages without `MessageID` or invalid JSON are skipped and reported in task logs.

Env vars can override defaults: *TOPIC_NAME*, *MAX_MESSAGES*, *MAX_BATCH_SIZE*, *POLL_TIMEOUT*, *CONSUMER_GROUP*, *HBASE_HOST*, *HBASE_PORT*, *HBASE_TABLE*, *HBASE_CF*.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict

import happybase
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

TOPIC_NAME: str = os.getenv("TOPIC_NAME", "ais-stream")
HBASE_HOST: str = os.getenv("HBASE_HOST", "hbase-thrift")
HBASE_PORT: int = int(os.getenv("HBASE_PORT", "9040"))
HBASE_TABLE: str = os.getenv("HBASE_TABLE", "ais_messages")
FAMILIES: Dict[str, Dict[str, Any]] = {
    "message": dict(),  # actual message JSON
    "meta_data": dict(),  # meta data JSON and message_type
}


# Cache HBase connection/table across calls
_hb_connection = None  # type: ignore
_hb_table = None  # type: ignore


def get_hbase_table() -> happybase.Table:
    """Get or create HBase table connection."""
    global _hb_connection, _hb_table
    if _hb_table is not None:
        return _hb_table

    if _hb_connection is None:
        print("Getting HBase table connection...")
        _hb_connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)
        print("Connected to HBase at", HBASE_HOST, HBASE_PORT)

    tables = [
        t.decode() if isinstance(t, bytes) else t for t in _hb_connection.tables()
    ]
    print("Existing HBase tables:", tables)
    if HBASE_TABLE not in tables:
        _hb_connection.create_table(HBASE_TABLE, FAMILIES)
        print("Created table", HBASE_TABLE)
    else:
        print("Table already exists:", HBASE_TABLE)

    _hb_table = _hb_connection.table(HBASE_TABLE)
    return _hb_table


def process_message(message) -> None:
    """Apply function for ConsumeFromTopicOperator: insert into HBase via happybase.

    Assumptions:
    - `message` is a confluent_kafka.Message-like object.
    - The message value is a JSON string containing at least a "MessageID" key.
    - The HBase table `HBASE_TABLE` with column family `HBASE_CF` exists.
    - Connection is cached across invocations to avoid reconnect overhead.
    """
    raw_value = message.value()
    if raw_value is None:
        return {"error": "Message has no value"}

    try:
        payload = json.loads(raw_value.decode("utf-8"))
    except Exception:
        print("Failed to decode JSON:", raw_value)
        return

    message_type = payload.get("MessageType")
    message_obj = payload.get("Message")
    meta_data = payload.get("MetaData")

    # Validate minimal structure
    if not isinstance(message_obj, dict) or not isinstance(meta_data, dict):
        raise ValueError(
            "Invalid message structure: expected Message and MetaData dicts"
        )

    # Resolve/construct row key
    message_id = message_obj.get("MessageID")
    if not message_id:
        message_id = f"unknown-{hash(json.dumps(payload, sort_keys=True))}"

    message_id = str(message_id) + f"-{int(datetime.utcnow().timestamp() * 1000)}"

    # Add derived fields
    meta_data["message_type"] = message_type

    # Build columns safely: skip None keys/values, ensure bytes for qualifier and value
    data: Dict[bytes, bytes] = {}
    for cf, cols in (
        ("message", message_obj),
        ("meta_data", meta_data),
    ):
        for k, v in cols.items():
            if k is None:
                continue
            # Skip None values to avoid server-side NPE
            if v is None:
                continue
            qualifier = f"{cf}:{str(k)}".encode("utf-8")
            value = str(v).encode("utf-8")
            data[qualifier] = value

    table = get_hbase_table()
    try:
        table.put(
            row=str(message_id).encode("utf-8"),
            data=data,
            timestamp=int(datetime.utcnow().timestamp() * 1000),
        )
        print(f"Inserted message into HBase (id: {message_id})")

    except Exception as e:
        raise RuntimeError(f"Failed to insert into HBase (id: {message_id}): {e}")


with DAG(
    dag_id="read_ais_stream",
    default_args={
        "owner": "ppz",
        "depends_on_past": False,
        "retries": 0,
    },
    description="Consume messages from Kafka topic ais-stream",
    start_date=datetime(2025, 12, 1),
    # schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    ConsumeFromTopicOperator(
        task_id="consume_kafka_ais_stream",
        kafka_config_id="kafka",
        topics=[TOPIC_NAME],
        apply_function=process_message,
        max_messages=int(os.getenv("MAX_MESSAGES", "10000")),
        max_batch_size=int(os.getenv("MAX_BATCH_SIZE", "50")),
        poll_timeout=int(os.getenv("POLL_TIMEOUT", "5")),
    )
