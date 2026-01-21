""" "Utility functions for HBase and Spark interactions."""

import json

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


def parse_bbox(bbox_str):
    """Parse [[[min_long, max_long],[min_lat, max_lat]]]"""
    try:
        data = json.loads(bbox_str)
        # data[0][0] is [min_long, max_long], data[0][1] is [min_lat, max_lat]
        return data[0][0][0], data[0][0][1], data[0][1][0], data[0][1][1]
    except Exception as e:
        raise ValueError(f"Failed to parse BBOX: {e}")
