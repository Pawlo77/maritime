"""Spark Ports and Vessels Analysis DAG."""

import ast
import datetime as dt

from airflow.sdk import dag, task
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    count_distinct,
    dayofmonth,
    desc,
    hour,
    max,
    min,
    month,
    unix_timestamp,
    when,
    year,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    round as spark_round,
)
from utils import fetch_hbase_table

SPARK_CONF = {
    "spark.master": "spark://spark-master:7077",
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.pyspark.python": "python3",
    "spark.pyspark.driver.python": "python3",
    "spark.jars.packages": "org.apache.hbase.connectors.spark:hbase-spark:1.0.1",
    "spark.hadoop.hbase.zookeeper.quorum": "zookeeper",
    "spark.hadoop.hbase.zookeeper.property.clientPort": "2181",
    "spark.executor.memory": "2g",
    "spark.driver.memory": "2g",
    "spark.executor.cores": "2",
}

PORTS_INFO_CATALOG = """{
    "table": {"namespace": "default", "name": "ports_info"},
    "rowkey": "id",
    "columns": {
        "id": {"cf": "rowkey", "col": "id", "type": "string"},
        "port_id": {"cf": "info", "col": "port_id", "type": "string"},
        "port_name": {"cf": "info", "col": "name", "type": "string"},
        "country": {"cf": "info", "col": "country", "type": "string"},
        "berths": {"cf": "info", "col": "berths", "type": "string"},
        "un_locode": {"cf": "info", "col": "un_locode", "type": "string"},
        "latitude": {"cf": "info", "col": "lat", "type": "string"},
        "longitude": {"cf": "info", "col": "long", "type": "string"},
        "timestamp": {"cf": "meta_data", "col": "timestamp", "type": "string"}
    }
}"""

VESSEL_INFO_CATALOG = """{
    "table": {"namespace": "default", "name": "vessel_info"},
    "rowkey": "id",
    "columns": {
        "id": {"cf": "rowkey", "col": "id", "type": "string"},
        "mmsi": {"cf": "info", "col": "mmsi", "type": "string"},
        "ship_name": {"cf": "info", "col": "name", "type": "string"},
        "call_sign": {"cf": "info", "col": "call_sign", "type": "string"},
        "ship_type": {"cf": "info", "col": "type", "type": "string"},
        "flag": {"cf": "info", "col": "flag", "type": "string"},
        "length": {"cf": "info", "col": "a", "type": "string"},
        "width": {"cf": "info", "col": "b", "type": "string"},
        "draft": {"cf": "info", "col": "c", "type": "string"},
        "beam": {"cf": "info", "col": "d", "type": "string"},
        "imo": {"cf": "info", "col": "imo", "type": "string"},
        "status": {"cf": "info", "col": "status", "type": "string"},
        "destination": {"cf": "info", "col": "dest", "type": "string"},
        "eta": {"cf": "info", "col": "eta", "type": "string"},
        "latitude": {"cf": "info", "col": "lat", "type": "string"},
        "longitude": {"cf": "info", "col": "lng", "type": "string"},
        "speed_over_ground": {"cf": "info", "col": "sog", "type": "string"},
        "course_over_ground": {"cf": "info", "col": "cog", "type": "string"},
        "heading": {"cf": "info", "col": "hdt", "type": "string"},
        "rate_of_turn": {"cf": "info", "col": "rot", "type": "string"},
        "timestamp": {"cf": "meta_data", "col": "timestamp", "type": "string"}
    }
}"""

AIS_MESSAGES_CATALOG = """{
    "table": {"namespace": "default", "name": "ais_messages"},
    "rowkey": "id",
    "columns": {
        "id": {"cf": "rowkey", "col": "id", "type": "string"},
        "mmsi": {"cf": "meta_data", "col": "MMSI", "type": "string"},
        "ship_name": {"cf": "meta_data", "col": "ShipName", "type": "string"},
        "latitude": {"cf": "meta_data", "col": "latitude", "type": "string"},
        "longitude": {"cf": "meta_data", "col": "longitude", "type": "string"},
        "message_type": {"cf": "meta_data", "col": "message_type", "type": "string"},
        "time_utc": {"cf": "meta_data", "col": "time_utc", "type": "string"},
        "position_report": {"cf": "message", "col": "PositionReport", "type": "string"}
    }
}"""


@dag(
    dag_id="spark_ports_vessels_analysis_dag",
    schedule=None,
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    tags=["Spark", "Analysis", "Ports", "Vessels"],
)
def spark_ports_vessels_analysis_dag():
    """
    Comprehensive DAG for analyzing ports and vessels data.
    Provides insights on:
    - Port characteristics and distribution
    - Vessel fleet composition
    - Vessel traffic patterns
    - Port activity metrics
    - Vessel movements and routes
    """

    @task.pyspark(conn_id="spark", config_kwargs=SPARK_CONF)
    def analyze_ports(spark: SparkSession) -> None:
        """
        Analyze ports: distribution, types, geographic clustering.
        """
        print("=== Starting Ports Analysis ===")

        try:
            ports_df = fetch_hbase_table(spark, PORTS_INFO_CATALOG)
            print(f"Loaded {ports_df.count()} port records")
        except Exception as e:
            print(f"Error loading ports data: {e}")
            return

        ports_df = ports_df.na.replace("nan", None)

        port_stats = ports_df.select(
            "port_id",
            "port_name",
            "country",
            "berths",
            "un_locode",
            col("latitude").cast("double").alias("latitude"),
            col("longitude").cast("double").alias("longitude"),
        ).dropDuplicates(["port_id"])

        print(f"\n=== Total Unique Ports: {port_stats.count()} ===")

        ports_by_country = (
            port_stats.groupBy("country")
            .agg(count("port_id").alias("port_count"))
            .orderBy(desc("port_count"))
        )

        print("\n=== Ports Distribution by Country ===")
        ports_by_country.show(15)

        ports_with_region = port_stats.withColumn(
            "region",
            when(col("latitude") > 60, "Northern Europe")
            .when(col("latitude") > 50, "Central Europe")
            .when(col("latitude") > 40, "Mediterranean")
            .otherwise("Other"),
        )

        regional_ports = (
            ports_with_region.groupBy("region")
            .agg(
                count("port_id").alias("port_count"),
                spark_round(avg("latitude"), 2).alias("avg_latitude"),
                spark_round(avg("longitude"), 2).alias("avg_longitude"),
            )
            .orderBy(desc("port_count"))
        )

        print("\n=== Regional Port Distribution ===")
        regional_ports.show()

        try:
            port_stats.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/port_statistics"
            )
            ports_by_country.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/ports_by_country"
            )
            regional_ports.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/regional_ports"
            )
            print("✓ Ports analysis saved to HDFS")
        except Exception as e:
            print(f"Warning: Could not save results: {e}")

        print("=== Ports Analysis Complete ===\n")

    @task.pyspark(conn_id="spark", config_kwargs=SPARK_CONF)
    def analyze_vessel_fleet(spark: SparkSession) -> None:
        """
        Analyze vessel fleet composition, types, sizes, and nationalities.
        """
        print("=== Starting Vessel Fleet Analysis ===")

        try:
            vessels_df = fetch_hbase_table(spark, VESSEL_INFO_CATALOG)
            print(f"Loaded {vessels_df.count()} vessel records")
        except Exception as e:
            print(f"Error loading vessels data: {e}")
            return

        vessels_df = vessels_df.na.replace("nan", None)

        unique_vessels = vessels_df.select(
            "mmsi",
            "ship_name",
            "ship_type",
            "flag",
            col("length").cast("double").alias("length"),
            col("width").cast("double").alias("width"),
            col("draft").cast("double").alias("draft"),
            col("beam").cast("double").alias("beam"),
            "imo",
            "status",
        ).dropDuplicates(["mmsi"])

        print(f"\n=== Total Unique Vessels: {unique_vessels.count()} ===")

        vessels_by_type = (
            unique_vessels.groupBy("ship_type")
            .agg(count("mmsi").alias("vessel_count"))
            .orderBy(desc("vessel_count"))
        )

        print("\n=== Fleet Composition by Ship Type ===")
        vessels_by_type.show(20)

        vessels_by_flag = (
            unique_vessels.groupBy("flag")
            .agg(count("mmsi").alias("vessel_count"))
            .orderBy(desc("vessel_count"))
        )

        print("\n=== Vessels by Flag (Top 20) ===")
        vessels_by_flag.show(20)

        size_analysis = (
            unique_vessels.filter(col("length").isNotNull() & col("beam").isNotNull())
            .select(
                col("length").alias("length"),
                col("beam").alias("beam"),
                col("draft").alias("draft"),
                "ship_type",
            )
            .groupBy("ship_type")
            .agg(
                spark_round(avg("length"), 2).alias("avg_length"),
                spark_round(max("length"), 2).alias("max_length"),
                spark_round(avg("beam"), 2).alias("avg_beam"),
                spark_round(avg("draft"), 2).alias("avg_draft"),
                count("*").alias("vessel_count"),
            )
            .orderBy(desc("vessel_count"))
        )

        print("\n=== Vessel Size by Type ===")
        size_analysis.show()

        largest_vessels = (
            unique_vessels.filter(col("length").isNotNull())
            .select(
                "ship_name",
                "ship_type",
                "flag",
                "status",
                "imo",
                col("length").alias("length"),
                col("beam").alias("beam"),
            )
            .orderBy(desc("length"))
            .limit(10)
        )

        print("\n=== Top 10 Largest Vessels ===")
        largest_vessels.show()

        try:
            unique_vessels.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/vessel_fleet"
            )
            vessels_by_type.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/vessels_by_type"
            )
            vessels_by_flag.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/vessels_by_flag"
            )
            size_analysis.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/vessel_size_analysis"
            )
            print("✓ Vessel fleet analysis saved to HDFS")
        except Exception as e:
            print(f"Warning: Could not save results: {e}")

        print("=== Vessel Fleet Analysis Complete ===\n")

    @task.pyspark(conn_id="spark", config_kwargs=SPARK_CONF)
    def analyze_vessel_movements(spark: SparkSession) -> None:
        """
        Analyze vessel movements: speed patterns, course distributions, activity hotspots.
        Extracts Sog (Speed Over Ground), Cog (Course Over Ground), TrueHeading from AIS PositionReport data.
        """

        print("=== Starting Vessel Movements Analysis ===")

        try:
            ais_df = fetch_hbase_table(spark, AIS_MESSAGES_CATALOG)
            print(f"Loaded {ais_df.count()} AIS messages")
        except Exception as e:
            print(f"Error loading AIS data: {e}")
            return

        # Focus on recent data (last hour), commented due to limited test data.
        # max_timestamp = ais_df.agg(spark_max(unix_timestamp(col("time_utc")))).collect()[0][0]
        # one_hour_ago = max_timestamp - 3600
        # ais_df = ais_df.filter(unix_timestamp(col("time_utc")) >= one_hour_ago)

        position_df = ais_df.filter(col("message_type") == "PositionReport").limit(500)
        position_count = position_df.count()
        print(f"Filtered to {position_count} PositionReport messages")

        if position_count == 0:
            print("No position reports found")
            return

        def extract_sog(dict_str):
            try:
                if not dict_str:
                    return None
                data = (
                    ast.literal_eval(dict_str)
                    if isinstance(dict_str, str)
                    else dict_str
                )
                sog = data.get("Sog", None)
                return float(sog) if sog is not None else None
            except Exception as e:
                print(f"Error parsing Sog: {e}")
                return None

        def extract_cog(dict_str):
            try:
                if not dict_str:
                    return None
                data = (
                    ast.literal_eval(dict_str)
                    if isinstance(dict_str, str)
                    else dict_str
                )
                cog = data.get("Cog", None)
                return float(cog) if cog is not None else None
            except Exception as e:
                print(f"Error parsing Cog: {e}")
                return None

        def extract_heading(dict_str):
            try:
                if not dict_str:
                    return None
                data = (
                    ast.literal_eval(dict_str)
                    if isinstance(dict_str, str)
                    else dict_str
                )
                heading = data.get("TrueHeading", None)
                return float(heading) if heading is not None else None
            except Exception as e:
                print(f"Error parsing TrueHeading: {e}")
                return None

        spark.udf.register("extract_sog", extract_sog, "double")
        spark.udf.register("extract_cog", extract_cog, "double")
        spark.udf.register("extract_heading", extract_heading, "double")

        print("\n=== Sample Position Report Data ===")
        try:
            position_df.select(
                "mmsi", "ship_name", "latitude", "longitude", "position_report"
            ).coalesce(1).show(3, truncate=False)
        except Exception as e:
            print(f"Error showing sample: {e}")

        ais_clean = position_df.select(
            "mmsi",
            "ship_name",
            col("latitude").cast("double").alias("latitude"),
            col("longitude").cast("double").alias("longitude"),
            F.expr("extract_sog(position_report)").alias("speed_over_ground"),
            F.expr("extract_cog(position_report)").alias("course_over_ground"),
            F.expr("extract_heading(position_report)").alias("heading"),
        )

        ais_clean = ais_clean.filter(
            col("speed_over_ground").isNotNull()
            | col("course_over_ground").isNotNull()
            | col("heading").isNotNull()
        )

        print(f"\n=== Valid Movement Records: {ais_clean.count()} ===")

        speed_stats = ais_clean.agg(
            spark_round(avg("speed_over_ground"), 2).alias("avg_speed_kts"),
            spark_round(max("speed_over_ground"), 2).alias("max_speed_kts"),
            spark_round(min("speed_over_ground"), 2).alias("min_speed_kts"),
        )

        print("\n=== Speed Statistics (knots) ===")
        speed_stats.show()

        vessel_speed_patterns = (
            ais_clean.groupBy("mmsi")
            .agg(
                spark_round(avg("speed_over_ground"), 2).alias("avg_speed_kts"),
                spark_round(max("speed_over_ground"), 2).alias("max_speed_kts"),
                count("*").alias("position_reports"),
            )
            .orderBy(desc("avg_speed_kts"))
            .limit(20)
        )

        print("\n=== Fastest Vessels (by Average Speed) ===")
        vessel_speed_patterns.show()

        navigation_hotspots = (
            ais_clean.select(
                spark_round(col("latitude"), 1).alias("lat_zone"),
                spark_round(col("longitude"), 1).alias("lon_zone"),
                "speed_over_ground",
            )
            .groupBy("lat_zone", "lon_zone")
            .agg(
                count("*").alias("vessel_count"),
                spark_round(avg("speed_over_ground"), 2).alias("avg_speed"),
            )
            .orderBy(desc("vessel_count"))
            .limit(20)
        )

        print("\n=== Top Navigation Hotspots ===")
        navigation_hotspots.show()

        movement_status = (
            ais_clean.groupBy(
                when(col("speed_over_ground") < 0.5, "Anchored/Stationary")
                .when(col("speed_over_ground") < 5, "Slow Moving")
                .when(col("speed_over_ground") < 15, "Normal Transit")
                .otherwise("High Speed")
                .alias("movement_class")
            )
            .agg(
                count("*").alias("position_reports"),
                count_distinct("mmsi").alias("unique_vessels"),
            )
            .orderBy(desc("position_reports"))
        )

        print("\n=== Vessel Movement Status Distribution ===")
        movement_status.show()

        try:
            vessel_speed_patterns.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/vessel_speed_patterns"
            )
            navigation_hotspots.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/navigation_hotspots"
            )
            movement_status.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/movement_status"
            )
            print("✓ AIS movements saved to HDFS")
        except Exception as e:
            print(f"Warning: Could not save results: {e}")

        print("=== Vessel Movements Analysis Complete ===\n")

    # DAG workflow
    # analyze_ports() >> analyze_vessel_fleet() >> analyze_vessel_movements()
    analyze_vessel_movements()


spark_ports_vessels_analysis_dag()
