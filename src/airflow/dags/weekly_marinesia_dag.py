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
    dag_id="weekly_marinesia_dag",
    # schedule="@weekly",
    schedule=None,
    start_date=dt.datetime(2025, 12, 1),
    catchup=False,
    tags=["vessel_info"],
)
def weekly_marinesia_dag():

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

    @task.pyspark(
        conn_id="spark",
        config_kwargs=SPARK_CONF,
    )
    def fetch_vessel_info(spark: SparkSession) -> None:
        df = (
            spark.read.format("org.apache.hadoop.hbase.spark")
            .options(catalog=AIS_MESSAGE_CATALOG)
            .option("hbase.spark.use.hbasecontext", "false")
            .option("hbase.spark.pushdown.columnfilter", "false")
            .load()
        )
        # Filter messages from the last week
        one_week_ago_str = (dt.datetime.utcnow() - dt.timedelta(days=7)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        df_filtered = df.filter(col("time_utc") >= one_week_ago_str)

        print(f"Filtering messages since: {one_week_ago_str}")
        print(f"Row count: {df_filtered.count()}")

        mmsi_df = df_filtered.select("mmsi").distinct()
        print("Sample MMSI")
        print("Count of distinct MMSI:", mmsi_df.count())
        mmsi_df.show(5)

        # Define schema for vessel info
        vessel_info_schema = StructType(
            [
                StructField("imo", IntegerType(), True),
                StructField("mmsi", IntegerType(), True),
                StructField("callsign", StringType(), True),
                StructField("name", StringType(), True),
                StructField("ship_type", StringType(), True),
                StructField("country", StringType(), True),
                StructField("dimension_a", IntegerType(), True),
                StructField("dimension_b", IntegerType(), True),
                StructField("dimension_c", IntegerType(), True),
                StructField("dimension_d", IntegerType(), True),
                StructField("length", IntegerType(), True),
                StructField("width", IntegerType(), True),
                StructField("image", StringType(), True),
            ]
        )

        vessel_info_df = spark.createDataFrame([], schema=vessel_info_schema)

        # Fetch vessel info for each MMSI
        print("Fetching vessel info for each MMSI.")
        mmsi_list = [row[0] for row in mmsi_df.select("mmsi").collect()]

        all_vessel_data = []
        for mmsi_value in mmsi_list:
            url = f"https://api.marinesia.com/api/v1/vessel/{mmsi_value}/profile?key={API_KEY_MARINESIA}"
            try:
                resp = requests.get(url, timeout=10).json()
                if "data" in resp:
                    all_vessel_data.append(resp["data"])
            except Exception as e:
                print(f"Failed to fetch {mmsi_value}: {e}")

        print(f"Fetched rows: {vessel_info_df.count()}.")
        if not all_vessel_data:
            print("No vessel data found.")
            return

        vessel_info_df = spark.createDataFrame(all_vessel_data)
        curr_time = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        vessel_info_df = vessel_info_df.withColumn(
            "timestamp", to_timestamp(lit(curr_time))
        )

        # Put data into HBase
        print("Loading data into HBase.")
        hbase_table = get_hbase_table(VESSEL_INFO_TABLE, {"info": {}, "meta_data": {}})
        with hbase_table.batch(batch_size=50) as b:
            for row in vessel_info_df.collect():
                rk = f"{row['mmsi']}-{curr_time}".encode()
                payload = {
                    f"info:{k}".encode(): str(v).encode()
                    for k, v in row.asDict().items()
                    if v is not None and k != "timestamp"
                }
                payload[b"meta_data:timestamp"] = str(row["timestamp"]).encode()
                b.put(rk, payload)

        print("HBase Load Complete.")

    @task.pyspark(
        conn_id="spark",
        config_kwargs=SPARK_CONF,
    )
    def fetch_ports_info(spark: SparkSession) -> None:
        min_long = float(BBOX.split(", ")[0].strip("[[["))
        max_long = float(BBOX.split(", ")[1].strip("]"))
        min_lat = float(BBOX.split(", ")[2].strip("["))
        max_lat = float(BBOX.split(", ")[3].strip("]]]"))

        # Fetch ports info within the BBOX
        print("Fetching ports info within the BBOX.")
        url = (
            "https://api.marinesia.com/api/v1/port/nearby?"
            + f"lat_min={min_lat}&lat_max={max_lat}&long_min={min_long}&long_max={max_long}"
            + f"&key={API_KEY_MARINESIA}"
        )

        response = requests.get(url).json()
        if response.get("error"):
            raise RuntimeError(f"API Error: {response['error']}")

        ports_df = spark.createDataFrame(response["data"])

        curr_time = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        ports_df = ports_df.withColumn("timestamp", to_timestamp(lit(curr_time)))
        print(f"Fetched rows: {ports_df.count()}.")

        # Putting data into HBase
        print("Loading data into HBase")
        hbase_table = get_hbase_table(PORTS_INFO_TABLE, {"info": {}, "meta_data": {}})
        with hbase_table.batch(batch_size=50) as b:
            for row in ports_df.collect():
                rk = f"{row['port_id']}-{curr_time}".encode()
                payload = {
                    f"info:{k}".encode(): str(v).encode()
                    for k, v in row.asDict().items()
                    if v is not None and k != "timestamp"
                }
                payload[b"meta_data:timestamp"] = str(row["timestamp"]).encode()
                b.put(rk, payload)

        print("HBase Load Complete.")

    fetch_vessel_info() 
    # fetch_ports_info()


weekly_marinesia_dag()
