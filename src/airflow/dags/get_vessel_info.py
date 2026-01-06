import time
import datetime
from airflow.sdk import dag, task
from pyspark.sql import SparkSession
import json
import happybase


HBASE_HOST = "hbase-thrift"
HBASE_PORT = 9040
HBASE_TABLE = "ais_messages"
ZOOKEEPER_QUORUM = "zookeeper"

AIS_MESSAGE_CATALOG = json.dumps(
    {
        "table": {"namespace": "default", "name": HBASE_TABLE},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "id", "type": "string"},
            "json_payload": {"cf": "cf", "col": "json", "type": "string"},
            "ingestion_ts": {"cf": "cf", "col": "ts", "type": "bigint"},
        },
    }
)

SPARK_CONF = {
    "spark.master": "spark://spark-master:7077",
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    # Python Version Enforcement (Crucial for your 3.10 vs 3.8 mismatch)
    "spark.pyspark.python": "python3",
    "spark.pyspark.driver.python": "python3",
    # Spark 3.5.5 / Scala 2.12 compatible package
    "spark.jars.packages": "org.apache.hbase.connectors.spark:hbase-spark:1.0.1",
    # HBase/Zookeeper Connectivity
    "spark.hadoop.hbase.zookeeper.quorum": ZOOKEEPER_QUORUM,
    "spark.hadoop.hbase.zookeeper.property.clientPort": "2181",
    # Resource Allocation
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "1",
}


@dag(
    dag_id="get_vessel_info_dag",
    schedule=None,
    start_date=datetime.datetime(2025, 12, 1),
    catchup=False,
    tags=["vessel_info"],
)
def get_vessel_info():

    @task()
    def check_table_exist() -> None:

        print("Getting HBase table connection...")
        hb_connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)
        print("Connected to HBase at", HBASE_HOST, HBASE_PORT)

        tables = [
            t.decode() if isinstance(t, bytes) else t for t in hb_connection.tables()
        ]
        hb_connection.close()

        if HBASE_TABLE in tables:
            print(f"Success: Table '{HBASE_TABLE}' exists.")
        else:
            raise ValueError(f"Error: Table '{HBASE_TABLE}' was not found!")

    @task.pyspark(
        conn_id="spark",
        config_kwargs=SPARK_CONF,
    )
    def fetch_vessel_info(spark: SparkSession) -> None:
        # 1. Gain access to the JVM and the Hadoop Config
        jvm = spark._jvm
        jsc = spark._jsc
        
        print(jvm)
        print(jsc)
        print(jsc.sc())

        # 2. Build the HBase Configuration
        # We explicitly use the HBaseConfiguration class to create it
        # hconf = jvm.org.apache.hadoop.hbase.HBaseConfiguration.create()
        # hconf.set("hbase.zookeeper.quorum", "zookeeper")
        # hconf.set("hbase.zookeeper.property.clientPort", "2181")

        # print(hconf)
        # print(hconf.get("hbase.zookeeper.quorum"))
        # print(hconf.get("hbase.zookeeper.property.clientPort"))
        
        # 3. THE FIX: Initialize HBaseContext using the Gateway
        # If the direct call failed, we use the 'new' keyword via the JVM gateway
        # This registers the context so DefaultSource.scala:153 doesn't return null
        # print("Registering HBaseContext in JVM...")
        # hcontext = jvm.org.apache.hadoop.hbase.spark.HBaseContext(jsc.sc(), hconf)
        
        # print(hcontext)
        # base_conf = jvm.org.apache.hadoop.conf.Configuration()
        # iterator = hconf.iterator()
        # while iterator.hasNext():
        #     item = iterator.next()
        #     base_conf.set(item.getKey(), item.getValue())

        # # Use the base_conf for the constructor
        # jvm.org.apache.hadoop.hbase.spark.HBaseContext(jsc, base_conf)

        # print("--- HBase Context Initialized ---")

        # # 4. Perform the load with the pushdown disabled to avoid the server-side error
        df = (
            spark.read.format("org.apache.hadoop.hbase.spark")
            .options(catalog=AIS_MESSAGE_CATALOG)
            .option("hbase.spark.use.hbasecontext", "false")
            .load()
        )

        # Proceed with count
        print(f"Row count: {df.count()}")

    # check_table_exist() >> fetch_vessel_info()
    fetch_vessel_info()

get_vessel_info()
