"""Daily Weather Forecast Fetch DAG."""

import datetime as dt

import openmeteo_requests
import requests_cache
from airflow.sdk import dag, task
from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from retry_requests import retry
from utils import fetch_hbase_table, get_hbase_table

PORTS_INFO_CATALOG = """{
    "table": {"namespace": "default", "name": "ports_info"},
    "rowkey": "id",
    "columns": {
        "id": {"cf": "rowkey", "col": "id", "type": "string"},
        "port_id": {"cf": "info", "col": "port_id", "type": "string"},
        "port_name": {"cf": "info", "col": "port_name", "type": "string"},
        "latitude": {"cf": "info", "col": "lat", "type": "string"},
        "longitude": {"cf": "info", "col": "long", "type": "string"},
        "timestamp": {"cf": "meta_data", "col": "timestamp", "type": "string"}
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
    "spark.hadoop.hbase.zookeeper.quorum": "zookeeper",
    "spark.hadoop.hbase.zookeeper.property.clientPort": "2181",
    # Resource Allocation
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "1",
    "spark.worker.cleanup.enabled": "true",
    "spark.worker.cleanup.interval": "1800",  # Clean every 30 mins
    "spark.worker.cleanup.appDataTtl": "3600",  # Remove app data older than 1 hour
}

WEATHER_FIELDS = [
    "wave_height",
    "wave_direction",
    "wave_period",
    "wave_peak_period",
    "wind_wave_height",
    "wind_wave_direction",
    "wind_wave_period",
    "wind_wave_peak_period",
    "swell_wave_height",
    "swell_wave_direction",
    "swell_wave_period",
    "swell_wave_peak_period",
    "secondary_swell_wave_height",
    "secondary_swell_wave_period",
    "secondary_swell_wave_direction",
    "tertiary_swell_wave_height",
    "tertiary_swell_wave_period",
    "tertiary_swell_wave_direction",
    "sea_level_height_msl",
    "sea_surface_temperature",
    "ocean_current_velocity",
    "ocean_current_direction",
]


@dag(
    dag_id="daily_weather_forecast_dag",
    # schedule="@hourly",
    schedule=None,
    start_date=dt.datetime(2025, 12, 1),
    catchup=False,
    tags=["Open-Meteo API"],
)
def daily_weather_forecast_dag():
    """
    DAG to fetch daily weather forecast for ports stored in HBase 'ports_info' table. The weather data is fetched from the Open-Meteo Marine API and stored in the
    'port_weather_forecast' HBase table with relevant metadata.
    """

    def fetch_weather_for_port(lat: float, long: float) -> WeatherApiResponse:
        """Fetch hourly weather data for given port coordinates."""

        cache_session = requests_cache.CachedSession(".cache", expire_after=600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo_client = openmeteo_requests.Client(session=retry_session)

        url = "https://marine-api.open-meteo.com/v1/marine"
        params = {
            "latitude": lat,
            "longitude": long,
            "hourly": WEATHER_FIELDS,
            "forecast_days": 3,
            "wind_speed_unit": "ms",
        }

        response = openmeteo_client.weather_api(url, params=params)[0]
        print(f"Fetched weather data for port at {lat}, long {long}")
        return response

    @task.pyspark(
        conn_id="spark",
        config_kwargs=SPARK_CONF,
    )
    def fetch_weather_ports(spark: SparkSession) -> None:
        """
        Fetch weather forecast data for all ports and store in HBase.
        Retrieves port information from HBase, fetches weather data from Open-Meteo API,
        and writes results to port_weather_forecast table.
        """

        ports_df = fetch_hbase_table(spark, PORTS_INFO_CATALOG)

        latest_timestamp = ports_df.agg({"timestamp": "max"}).collect()[0][0]
        unique_ports_df = ports_df.filter(
            col("timestamp") == latest_timestamp
        ).dropDuplicates(["port_id"])

        weather_table = get_hbase_table(
            "port_weather_forecast",
            {
                "weather_data": dict(),
                "meta_data": dict(),
            },
        )

        print(f"Unique Ports: {unique_ports_df.count()}.")

        for row in unique_ports_df.collect():
            port_id = row["port_id"]
            port_name = row["port_name"]
            lat = float(row["latitude"])
            long = float(row["longitude"])

            response = fetch_weather_for_port(lat, long)
            forecast_data = response.Hourly()

            start_ts = forecast_data.Time()
            interval = forecast_data.Interval()
            num_steps = (forecast_data.TimeEnd() - start_ts) // interval

            weather_arrays = {}
            for i, field in enumerate(WEATHER_FIELDS):
                weather_arrays[field] = forecast_data.Variables(i).ValuesAsNumpy()

            with weather_table.batch(batch_size=100) as b:
                for i in range(num_steps):
                    current_ts = start_ts + (i * interval)

                    # RowKey: PortID - CurrentTime - ForecastTime
                    rk = f"{port_id}-{start_ts}-{current_ts}".encode("utf-8")

                    payload = {
                        b"meta_data:port_id": str(port_id).encode(),
                        b"meta_data:port_name": str(port_name).encode(),
                        b"meta_data:lat": str(lat).encode(),
                        b"meta_data:long": str(long).encode(),
                        b"meta_data:forecast_timestamp": str(current_ts).encode(),
                        b"meta_data:ref_time": str(start_ts).encode(),
                    }

                    for field in WEATHER_FIELDS:
                        val = weather_arrays[field][i]
                        payload[f"weather_data:{field}".encode()] = str(val).encode()

                    b.put(rk, payload)

            weather_table.put(rk, payload)
            print(f"Inserted weather data for port {port_id}.")

    fetch_weather_ports()


daily_weather_forecast_dag()
