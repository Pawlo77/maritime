from airflow.sdk import dag, task
from airflow.sensors.time_delta import TimeDeltaSensor
import datetime as dt
import happybase
import json
import os
import requests
import time

HBASE_HOST = "hbase-thrift"
HBASE_PORT = 9040
VESSEL_INFO_TABLE = "vessel_info"
PORTS_INFO_TABLE = "ports_info"
API_KEY_MARINESIA = ""
BBOX = "[[[-25.9973998712, 25.3439083708], [44.6049090453, 71.2982931893]]]"

_hb_connection = None  # type: ignore


@dag(
    dag_id="fetch_vessel_info_dag",
    # schedule="@daily",
    schedule=None,
    start_date=dt.datetime(2025, 12, 1),
    catchup=False,
    tags=["Marinesia API"],
)
def fetch_vessel_info_dag():

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
    
    def parse_bbox(bbox_str):
        """Parse [[[min_long, max_long],[min_lat, max_lat]]]"""
        try:
            data = json.loads(bbox_str)
            # data[0][0] is [min_long, max_long], data[0][1] is [min_lat, max_lat]
            return data[0][0][0], data[0][0][1], data[0][1][0], data[0][1][1]
        except Exception as e:
            raise ValueError(f"Failed to parse BBOX: {e}")

    @task()
    def fetch_vessel_info() -> None:
        min_long, max_long, min_lat, max_lat = parse_bbox(BBOX)
        # Fetch vessel info within the BBOX
        print(f"Fetching vessel info within the BBOX: {BBOX}.")
        url = (
            "https://api.marinesia.com//api/v1/vessel/nearby?"
            + f"lat_min={min_lat}&lat_max={max_lat}&long_min={min_long}&long_max={max_long}"
            + f"&key={API_KEY_MARINESIA}"
        )

        response = requests.get(url).json()
        if response.get("error"):
            raise RuntimeError(f"API Error: {response['error']}")

        vessel_info = response["data"]
        curr_time = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        timestamp_ms = int(dt.datetime.fromisoformat(curr_time).timestamp() * 1000)
        print(f"Fetched rows: {len(vessel_info)}.")

        # Put data into HBase
        print("Loading data into HBase.")
        hbase_table = get_hbase_table(VESSEL_INFO_TABLE, {"info": {}, "meta_data": {}})

        for vessel in vessel_info:
            curr_time = int(time.time())
            rk = f"{vessel['mmsi']}-{timestamp_ms}".encode("utf-8")
            payload = {f"info:{k}".encode("utf-8"): str(v).encode("utf-8") for k, v in vessel.items()}
            payload[b"meta_data:timestamp"] = str(curr_time).encode("utf-8")
            hbase_table.put(rk, payload)

        print("HBase Load Complete.")

    fetch_vessel_info()


fetch_vessel_info_dag()
