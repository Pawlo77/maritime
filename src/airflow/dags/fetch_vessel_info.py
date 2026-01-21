"""Vessel Information Fetch DAG."""

import datetime as dt
import time

import requests
from airflow.sdk import dag, task
from utils import get_hbase_table, parse_bbox

VESSEL_INFO_TABLE = "vessel_info"
API_KEY_MARINESIA = "djwXVjNxiroXuGiQcPTIxKAZB"
BBOX = "[[[-25.9973998712, 25.3439083708], [44.6049090453, 71.2982931893]]]"


@dag(
    dag_id="fetch_vessel_info_dag",
    # schedule="@daily",
    schedule=None,
    start_date=dt.datetime(2025, 12, 1),
    catchup=False,
    tags=["Marinesia API"],
)
def fetch_vessel_info_dag():
    """
    DAG to fetch vessel information from Marinesia API and store it in HBase. Vessels are fetched
    within a specified bounding box (BBOX). The data is stored in the 'vessel_info' HBase table with
    relevant metadata.
    """

    @task()
    def fetch_vessel_info() -> None:
        """
        Fetch vessel information from Marinesia API within a bounding box and load into HBase.
        This function retrieves vessel data from the Marinesia API for a specified geographic
        bounding box (BBOX), handles the API response, and stores the vessel information in
        an HBase table with metadata timestamps.
        The function performs the following operations:
        1. Parses the bounding box coordinates (min/max longitude and latitude)
        2. Queries the Marinesia API for vessels within the specified BBOX
        3. Validates the API response for errors
        4. Generates a current timestamp in milliseconds
        5. Creates or accesses the HBase vessel info table with appropriate column families
        6. Stores each vessel record in HBase with a composite row key (mmsi-timestamp)
        7. Includes metadata timestamps for each record
        The row key format is: {mmsi}-{timestamp_ms}
        The HBase columns are organized in two families:
        - info: Contains all vessel data fields
        - meta_data: Contains system metadata (timestamp)

        RuntimeError: If the Marinesia API returns an error in the response.

        Note:
            Requires BBOX, API_KEY_MARINESIA, and VESSEL_INFO_TABLE to be defined globally.
            API_KEY_MARINESIA: Authentication key for Marinesia API
            BBOX: Bounding box coordinates for geographic filtering
            VESSEL_INFO_TABLE: HBase table name for storing vessel information
        """

        min_long, max_long, min_lat, max_lat = parse_bbox(BBOX)
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

        print("Loading data into HBase.")
        hbase_table = get_hbase_table(VESSEL_INFO_TABLE, {"info": {}, "meta_data": {}})

        for vessel in vessel_info:
            curr_time = int(time.time())
            rk = f"{vessel['mmsi']}-{timestamp_ms}".encode("utf-8")
            payload = {
                f"info:{k}".encode("utf-8"): str(v).encode("utf-8")
                for k, v in vessel.items()
            }
            payload[b"meta_data:timestamp"] = str(curr_time).encode("utf-8")
            hbase_table.put(rk, payload)

        print("HBase Load Complete.")

    fetch_vessel_info()


fetch_vessel_info_dag()
