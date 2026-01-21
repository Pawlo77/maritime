"""Port Information Fetch DAG."""

import datetime as dt
import json
import time

import requests
from airflow.sdk import dag, task
from utils import get_hbase_table

PORTS_INFO_TABLE = "ports_info"
API_KEY_MARINESIA = "djwXVjNxiroXuGiQcPTIxKAZB"
BBOX = (
    "[[[-25.9973998712, 25.3439083708], [44.6049090453, 71.2982931893]]]"  # Europe BBOX
)


@dag(
    dag_id="fetch_ports_info_dag",
    # schedule="@weekly",
    schedule=None,
    start_date=dt.datetime(2025, 12, 1),
    catchup=False,
    tags=["Marinesia API"],
)
def fetch_ports_info_dag():
    """
    DAG to fetch ports information from Marinesia API and store it in HBase. Ports are fetched
    within a specified bounding box (BBOX). The data is stored in the 'ports_info' HBase table with
    relevant metadata.
    """

    def parse_bbox(bbox_str):
        """Parse [[[min_long, max_long],[min_lat, max_lat]]]"""
        try:
            data = json.loads(bbox_str)
            # data[0][0] is [min_long, max_long], data[0][1] is [min_lat, max_lat]
            return data[0][0][0], data[0][0][1], data[0][1][0], data[0][1][1]
        except Exception as e:
            raise ValueError(f"Failed to parse BBOX: {e}")

    @task()
    def fetch_ports_info() -> None:
        """
        Fetch port information from Marinesia API within a bounding box and load into HBase.
        This function retrieves port data from the Marinesia API for a specified geographic
        bounding box (BBOX), handles the API response, and stores the port information in
        an HBase table with metadata timestamps.
        The function performs the following operations:
        1. Parses the bounding box coordinates (min/max longitude and latitude)
        2. Queries the Marinesia API for ports within the specified BBOX
        3. Validates the API response for errors
        4. Generates a current timestamp in milliseconds
        5. Creates or accesses the HBase ports info table with appropriate column families
        6. Stores each port record in HBase with a composite row key (port_id-timestamp)
        7. Includes metadata timestamps for each record
        The row key format is: {port_id}-{timestamp_ms}
        The HBase columns are organized in two families:
        - info: Contains all port data fields
        - meta_data: Contains system metadata (timestamp)

        RuntimeError: If the Marinesia API returns an error in the response.

        Note:
            Requires BBOX, API_KEY_MARINESIA, and PORTS_INFO_TABLE to be defined globally.
            API_KEY_MARINESIA: Authentication key for Marinesia API
            BBOX: Bounding box coordinates for geographic filtering
            PORTS_INFO_TABLE: HBase table name for storing port information
        """

        min_long, max_long, min_lat, max_lat = parse_bbox(BBOX)

        print(f"Fetching ports info within the BBOX: {BBOX}.")
        url = (
            "https://api.marinesia.com/api/v1/port/nearby?"
            + f"lat_min={min_lat}&lat_max={max_lat}&long_min={min_long}&long_max={max_long}"
            + f"&key={API_KEY_MARINESIA}"
        )
        response = requests.get(url).json()
        if response.get("error"):
            raise RuntimeError(f"API Error: {response['error']}")

        ports_info = response["data"]
        curr_time = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        timestamp_ms = int(dt.datetime.fromisoformat(curr_time).timestamp() * 1000)
        print(f"Fetched rows: {len(ports_info)}.")

        # Put data into HBase
        print("Loading data into HBase.")
        hbase_table = get_hbase_table(PORTS_INFO_TABLE, {"info": {}, "meta_data": {}})

        for port in ports_info:
            curr_time = int(time.time())
            rk = f"{port['port_id']}-{timestamp_ms}".encode("utf-8")
            payload = {
                f"info:{k}".encode("utf-8"): str(v).encode("utf-8")
                for k, v in port.items()
            }
            payload[b"meta_data:timestamp"] = str(curr_time).encode("utf-8")
            hbase_table.put(rk, payload)

        print("HBase Load Complete.")

    fetch_ports_info()


fetch_ports_info_dag()
