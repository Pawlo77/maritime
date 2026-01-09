import happybase
import pyspark
from pyspark.sql import SparkSession


HBASE_HOST = "hbase-thrift"
HBASE_PORT = 9040


def get_hbase_table(
    table_name: str, families: dict, hb_connection: happybase.Connection = None
) -> happybase.Table:
    """Get or create HBase table connection."""
    if hb_connection is None:
        print("Getting HBase table connection...")
        hb_connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)
        print("Connected to HBase at", HBASE_HOST, HBASE_PORT)

    tables = [t.decode() if isinstance(t, bytes) else t for t in hb_connection.tables()]
    print("Existing HBase tables:", tables)
    if table_name not in tables:
        hb_connection.create_table(table_name, families)
        print("Created table", table_name)
    else:
        print("Table already exists:", table_name)

    hb_table = hb_connection.table(table_name)
    return hb_table


def fetch_hbase_table(spark: SparkSession, catalog: str) -> pyspark.sql.DataFrame:
    """Fetch data frame from HBase."""
    df = (
        spark.read.format("org.apache.hadoop.hbase.spark")
        .options(catalog=catalog)
        .option("hbase.spark.use.hbasecontext", "false")
        .option("hbase.spark.pushdown.columnfilter", "false")
        .load()
    )

    return df
