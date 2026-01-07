from airflow.sdk import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit
import happybase
import datetime as dt
import os
import requests
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

HBASE_HOST = "hbase-thrift"
HBASE_PORT = 9040
AIS_HBASE_TABLE = "ais_messages"
VESSEL_INFO_TABLE = "vessel_info"
PORTS_INFO_TABLE = "ports_info"
ZOOKEEPER_QUORUM = "zookeeper"
API_KEY_MARINESIA = os.getenv("API_KEY_MARINESIA", "tBJsUHneEMoqQFLhpLYtHUHtZ")
BBOX = os.getenv("BBOX", "[[[-25.9973998712, 25.3439083708], [44.6049090453, 71.2982931893]]]")

AIS_MESSAGE_CATALOG = """{
    "table": {"namespace": "default", "name": "ais_messages"},
    "rowkey": "id",
    "columns": {
        "id": {"cf": "rowkey", "col": "id", "type": "string"},
        "mmsi": {"cf": "meta_data", "col": "MMSI", "type": "string"},
        "time_utc": {"cf": "meta_data", "col": "time_utc", "type": "string"}
    }
}"""

SPARK_CONF = {
    "spark.master": "spark://spark-master:7077",
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.pyspark.python": "python3",
    "spark.pyspark.driver.python": "python3",
    "spark.jars.packages": "org.apache.hbase.connectors.spark:hbase-spark:1.0.1",
    # HBase/Zookeeper Connectivity
    "spark.hadoop.hbase.zookeeper.quorum": ZOOKEEPER_QUORUM,
    "spark.hadoop.hbase.zookeeper.property.clientPort": "2181",
    # Resource Allocation
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "1",
    "spark.worker.cleanup.enabled": "true",
    "spark.worker.cleanup.interval": "1800",  # Clean every 30 mins
    "spark.worker.cleanup.appDataTtl": "3600",  # Remove app data older than 1 hour
}

_hb_connection = None  # type: ignore

@dag(
    dag_id="daily_weather_dag",
    # schedule="@daily",
    schedule=None,
    start_date=dt.datetime(2025, 12, 1),
    catchup=False,
    tags=["vessel_info"],
)
def daily_weather_dag():

    def get_hbase_table(table_name, families) -> happybase.Table:
        """Get or create HBase table connection."""
        global _hb_connection

        if _hb_connection is None:
            print("Getting HBase table connection...")
            _hb_connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)
            print("Connected to HBase at", HBASE_HOST, HBASE_PORT)

        tables = [
            t.decode() if isinstance(t, bytes) else t for t in _hb_connection.tables()
        ]
        print("Existing HBase tables:", tables)
        if table_name not in tables:
            _hb_connection.create_table(table_name, families)
            print("Created table", table_name)
        else:
            print("Table already exists:", table_name)

        hb_table = _hb_connection.table(table_name)
        return hb_table

daily_weather_dag()