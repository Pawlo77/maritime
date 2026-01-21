"""Spark Weather Analysis DAG."""

import datetime as dt

from airflow.sdk import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    desc,
    from_unixtime,
    max,
    min,
    stddev,
    window,
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

WEATHER_FORECAST_CATALOG = """{
    "table": {"namespace": "default", "name": "port_weather_forecast"},
    "rowkey": "id",
    "columns": {
        "id": {"cf": "rowkey", "col": "id", "type": "string"},
        "port_id": {"cf": "meta_data", "col": "port_id", "type": "string"},
        "latitude": {"cf": "meta_data", "col": "lat", "type": "string"},
        "longitude": {"cf": "meta_data", "col": "long", "type": "string"},
        "forecast_timestamp": {"cf": "meta_data", "col": "forecast_timestamp", "type": "string"},
        "ref_time": {"cf": "meta_data", "col": "ref_time", "type": "string"},
        "wave_height": {"cf": "weather_data", "col": "wave_height", "type": "string"},
        "wave_direction": {"cf": "weather_data", "col": "wave_direction", "type": "string"},
        "wave_period": {"cf": "weather_data", "col": "wave_period", "type": "string"},
        "wind_wave_height": {"cf": "weather_data", "col": "wind_wave_height", "type": "string"},
        "wind_wave_direction": {"cf": "weather_data", "col": "wind_wave_direction", "type": "string"},
        "wind_wave_period": {"cf": "weather_data", "col": "wind_wave_period", "type": "string"},
        "swell_wave_height": {"cf": "weather_data", "col": "swell_wave_height", "type": "string"},
        "swell_wave_direction": {"cf": "weather_data", "col": "swell_wave_direction", "type": "string"},
        "swell_wave_period": {"cf": "weather_data", "col": "swell_wave_period", "type": "string"},
        "sea_surface_temperature": {"cf": "weather_data", "col": "sea_surface_temperature", "type": "string"},
        "sea_level_height_msl": {"cf": "weather_data", "col": "sea_level_height_msl", "type": "string"},
        "ocean_current_velocity": {"cf": "weather_data", "col": "ocean_current_velocity", "type": "string"},
        "ocean_current_direction": {"cf": "weather_data", "col": "ocean_current_direction", "type": "string"}
    }
}"""

PORT_INFO_CATALOG = """{
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


@dag(
    dag_id="spark_weather_analysis_dag",
    schedule=None,
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    tags=["Spark", "Analysis", "Weather"],
)
def spark_weather_analysis_dag():
    """
    DAG to perform advanced weather data analysis using Spark.
    Analyzes ocean conditions across ports: wave heights, temperatures, currents.
    Stores aggregated statistics back to HBase.
    """

    @task.pyspark(conn_id="spark", config_kwargs=SPARK_CONF)
    def analyze_weather_conditions(spark: SparkSession) -> None:
        """
        Analyze weather conditions:
        - Average wave heights per port
        - Temperature ranges
        - Storm detection (high wave heights)
        - Current patterns
        """
        print("=== Starting Weather Analysis ===")

        # Load weather data
        try:
            weather_df = fetch_hbase_table(spark, WEATHER_FORECAST_CATALOG)
            port_df = fetch_hbase_table(spark, PORT_INFO_CATALOG)
        except Exception as e:
            print(f"Error loading weather data: {e}")
            return

        weather_df = weather_df.na.replace(["nan", "NaN", "Nan", "NAN", "None"], None)
        weather_df = weather_df.join(
            port_df.select("port_id", "port_name"), on="port_id", how="left"
        )

        last_date = weather_df.agg(max(col("ref_time").cast("long"))).collect()[0][0]
        weather_df = weather_df.filter(col("ref_time") == last_date)
        print(
            f"Data loaded from latest timestamp: {last_date}, count: {weather_df.count()}"
        )

        # Count NULL/NaN values for each column
        print("\n=== NULL/NaN Count by Column ===")
        for column in weather_df.columns:
            null_count = weather_df.filter(col(column).isNull()).count()
            print(f"{column}: {null_count}")

        # 1. Port-level aggregations with location data
        port_analysis = (
            weather_df.groupBy("port_id", "port_name", "latitude", "longitude")
            .agg(
                spark_round(avg(col("wave_height").cast("float")), 2).alias(
                    "avg_wave_height"
                ),
                spark_round(max(col("wave_height").cast("float")), 2).alias(
                    "max_wave_height"
                ),
                spark_round(avg(col("wave_period").cast("float")), 2).alias(
                    "avg_wave_period"
                ),
                spark_round(avg(col("wind_wave_height").cast("float")), 2).alias(
                    "avg_wind_wave_height"
                ),
                spark_round(avg(col("wind_wave_direction").cast("float")), 2).alias(
                    "avg_wind_direction"
                ),
                spark_round(avg(col("swell_wave_height").cast("float")), 2).alias(
                    "avg_swell_wave_height"
                ),
                spark_round(avg(col("swell_wave_period").cast("float")), 2).alias(
                    "avg_swell_period"
                ),
                spark_round(avg(col("sea_surface_temperature").cast("float")), 2).alias(
                    "avg_temperature"
                ),
                spark_round(min(col("sea_surface_temperature").cast("float")), 2).alias(
                    "min_temperature"
                ),
                spark_round(max(col("sea_surface_temperature").cast("float")), 2).alias(
                    "max_temperature"
                ),
                spark_round(avg(col("sea_level_height_msl").cast("float")), 2).alias(
                    "avg_sea_level"
                ),
                spark_round(avg(col("ocean_current_velocity").cast("float")), 2).alias(
                    "avg_current_velocity"
                ),
                spark_round(avg(col("ocean_current_direction").cast("float")), 2).alias(
                    "avg_current_direction"
                ),
                count("*").alias("forecast_records"),
            )
            .orderBy(desc("avg_wave_height"))
        )

        print("\n=== Port-Level Weather Summary ===")
        port_analysis.filter(col("avg_wave_height").isNotNull()).show(10)

        # 2. Storm detection (high wave heights)
        storm_threshold = 3.0  # 3 meters
        storms = (
            weather_df.filter(
                (col("wave_height").isNotNull())
                & (col("wave_height") > storm_threshold)
            )
            .select(
                "port_id",
                "port_name",
                spark_round(col("wave_height").cast("float"), 2).alias("wave_height"),
                spark_round(col("swell_wave_height").cast("float"), 2).alias(
                    "swell_height"
                ),
                from_unixtime(col("forecast_timestamp").cast("long")).alias(
                    "forecast_time"
                ),
            )
            .orderBy(desc("wave_height"))
        )

        print(f"\n=== Storm Detection (Wave Height > {storm_threshold}m) ===")
        storms.show(10)

        # 3. Temperature analysis
        temp_analysis = (
            weather_df.groupBy("port_id", "port_name")
            .agg(
                spark_round(avg(col("sea_surface_temperature").cast("float")), 2).alias(
                    "avg_temp"
                ),
                spark_round(
                    stddev(col("sea_surface_temperature").cast("float")), 2
                ).alias("temp_stddev"),
                spark_round(min(col("sea_surface_temperature").cast("float")), 2).alias(
                    "min_temp"
                ),
                spark_round(max(col("sea_surface_temperature").cast("float")), 2).alias(
                    "max_temp"
                ),
            )
            .filter(col("avg_temp").isNotNull())
            .orderBy("avg_temp")
        )

        print("\n=== Sea Surface Temperature Analysis ===")
        temp_analysis.show(10)

        # 4. Ocean current analysis
        current_analysis = (
            weather_df.groupBy("port_id", "port_name")
            .agg(
                spark_round(avg(col("ocean_current_velocity").cast("float")), 2).alias(
                    "avg_current_velocity"
                ),
                spark_round(max(col("ocean_current_velocity").cast("float")), 2).alias(
                    "max_current_velocity"
                ),
                spark_round(
                    stddev(col("ocean_current_velocity").cast("float")), 2
                ).alias("current_stddev"),
                count("*").alias("measurements"),
            )
            .filter(col("avg_current_velocity").isNotNull())
            .orderBy(desc("avg_current_velocity"))
        )

        print("\n=== Ocean Current Analysis ===")
        current_analysis.show(10)

        # 5. Wave type comparison
        wave_comparison = weather_df.select(
            "port_id",
            "port_name",
            spark_round(col("wave_height").cast("float"), 2).alias("total_waves"),
            spark_round(col("wind_wave_height").cast("float"), 2).alias("wind_waves"),
            spark_round(col("swell_wave_height").cast("float"), 2).alias("swell_waves"),
        ).filter(col("total_waves").isNotNull())

        print("\n=== Wave Type Comparison (Sample) ===")
        wave_comparison.show(5)

        # Save results
        try:
            port_analysis.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/weather_port_summary"
            )
            storms.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/storm_detection"
            )
            temp_analysis.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/temperature_analysis"
            )
            current_analysis.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/current_analysis"
            )
            print("✓ Weather analysis saved to HDFS")
        except Exception as e:
            print(f"Warning: Could not save results: {e}")

        print("=== Weather Analysis Complete ===\n")

    @task.pyspark(conn_id="spark", config_kwargs=SPARK_CONF)
    def identify_hazardous_ports(spark: SparkSession) -> None:
        """
        Identify ports with hazardous conditions:
        - High wave heights
        - Extreme temperatures
        - Strong currents
        """
        print("=== Identifying Hazardous Ports ===")

        try:
            weather_df = fetch_hbase_table(spark, WEATHER_FORECAST_CATALOG)
            port_df = fetch_hbase_table(spark, PORT_INFO_CATALOG)
        except Exception as e:
            print(f"Error loading weather data: {e}")
            return

        weather_df = weather_df.na.replace(["nan", "NaN", "Nan", "NAN", "None"], None)
        last_date = weather_df.agg(max(col("ref_time").cast("long"))).collect()[0][0]
        weather_df = weather_df.filter(col("ref_time") == last_date)
        print(
            f"Data loaded from latest timestamp: {last_date}, records now: {weather_df.count()}"
        )

        weather_df = weather_df.join(
            port_df.select("port_id", "port_name"), on="port_id", how="left"
        )
        # Define thresholds
        high_wave_threshold = 4.0  # meters
        high_current_threshold = 1.5  # m/s
        temp_extremes = (0, 35)  # Celsius

        hazardous = (
            weather_df.filter(
                (
                    (
                        (col("wave_height").cast("float") > high_wave_threshold)
                        & (col("wave_height").isNotNull())
                    )
                    | (
                        (
                            col("ocean_current_velocity").cast("float")
                            > high_current_threshold
                        )
                        & (col("ocean_current_velocity").isNotNull())
                    )
                    | (
                        (
                            col("sea_surface_temperature").cast("float")
                            < temp_extremes[0]
                        )
                        & (col("sea_surface_temperature").isNotNull())
                    )
                    | (
                        (
                            col("sea_surface_temperature").cast("float")
                            > temp_extremes[1]
                        )
                        & (col("sea_surface_temperature").isNotNull())
                    )
                )
            )
            .groupBy("port_id", "port_name")
            .agg(
                count("*").alias("hazard_count"),
                spark_round(max(col("wave_height").cast("float")), 2).alias(
                    "max_wave_height"
                ),
                spark_round(max(col("ocean_current_velocity").cast("float")), 2).alias(
                    "max_current_velocity"
                ),
            )
            .orderBy(desc("hazard_count"))
        )

        print("\n=== Hazardous Ports Summary ===")
        hazardous.show()

        try:
            hazardous.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/hazardous_ports"
            )
            print("Hazardous ports analysis saved to HDFS")
        except Exception as e:
            print(f"Warning: Could not save results: {e}")

    @task.pyspark(conn_id="spark", config_kwargs=SPARK_CONF)
    def compare_ports_weather(spark: SparkSession) -> None:
        """
        Comparative analysis between ports to identify regional patterns.
        """
        print("=== Comparing Ports Weather Patterns ===")

        try:
            weather_df = fetch_hbase_table(spark, WEATHER_FORECAST_CATALOG)
            ports_df = fetch_hbase_table(spark, PORT_INFO_CATALOG)
        except Exception as e:
            print(f"Error loading data: {e}")
            return

        weather_df = weather_df.na.replace(["nan", "NaN", "Nan", "NAN", "None"], None)
        ports_df = ports_df.na.replace(["nan", "NaN", "Nan", "NAN", "None"], None)

        last_date = weather_df.agg(max(col("ref_time").cast("long"))).collect()[0][0]
        weather_df = weather_df.filter(col("ref_time") == last_date)
        print(
            f"Data loaded from latest timestamp: {last_date}, records now: {weather_df.count()}"
        )

        # Join with port info
        regional_analysis = (
            weather_df.join(
                ports_df.select(
                    "port_id", "port_name", "country", "berths", "un_locode"
                ),
                on="port_id",
                how="left",
            )
            .groupBy("country")
            .agg(
                spark_round(avg(col("wave_height").cast("float")), 2).alias(
                    "avg_wave_height"
                ),
                spark_round(avg(col("sea_surface_temperature").cast("float")), 2).alias(
                    "avg_temperature"
                ),
                spark_round(avg(col("ocean_current_velocity").cast("float")), 2).alias(
                    "avg_current"
                ),
                count("*").alias("total_measurements"),
            )
            .orderBy(desc("avg_wave_height"))
        )

        regional_analysis = regional_analysis.filter(
            col("country").isNotNull()
            & (
                col("avg_wave_height").isNotNull()
                | col("avg_temperature").isNotNull()
                | col("avg_current").isNotNull()
            )
        )

        print("\n=== Regional Weather Comparison ===")
        regional_analysis.filter(col("avg_wave_height").isNotNull()).show()

        try:
            regional_analysis.write.mode("overwrite").parquet(
                "hdfs://hdfs-namenode:9000/output/analysis/regional_weather_comparison"
            )
            print("✓ Regional weather analysis saved to HDFS")
        except Exception as e:
            print(f"Warning: Could not save results: {e}")

    # DAG workflow
    (
        analyze_weather_conditions()
        >> identify_hazardous_ports()
        >> compare_ports_weather()
    )


spark_weather_analysis_dag()
