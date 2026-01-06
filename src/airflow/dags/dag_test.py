import datetime
from airflow.sdk import dag, task
from pyspark.sql import SparkSession


@dag(
    dag_id="spark_test_dag_v2",
    schedule=None,
    start_date=datetime.datetime(2025, 12, 1),
    catchup=False,
    tags=["spark_4_0", "airflow_3"],
)
def spark_test_dag():

    # Updated configuration for Spark 4.0 compatibility
    # Note: 'airflow-worker' must be the service name in your docker-compose
    spark_conf = {
        "spark.master": "spark://spark-master:7077",
        "spark.driver.host": "airflow-scheduler",  # Points to the Airflow container
        "spark.driver.bindAddress": "0.0.0.0",  # Listens on all interfaces
        "spark.driver.port": "10000",  # Fixed port for callback
        "spark.blockManager.port": "10001",  # Fixed port for data transfer
        "spark.executor.memory": "600m",
        "spark.driver.memory": "600m",
        "spark.executor.cores": "1",
        "spark.cores.max": "1",
    }

    # spark_conf = {
    #     "spark.master": "local[*]", # Runs Spark inside the Airflow container
    #     "spark.executor.memory": "600m",
    # }

    @task.pyspark(
        conn_id="spark",  # Ensure this connection is set to spark://spark-master:7077
        config_kwargs=spark_conf,
    )
    def test_spark_init(spark: SparkSession) -> None:
        """Verify session and cluster connectivity."""
        print(f"Spark Version: {spark.version}")
        print(f"Master URL: {spark.sparkContext.master}")

    @task.pyspark(
        conn_id="spark",
        config_kwargs=spark_conf,
    )
    def process_sample_data(spark: SparkSession) -> None:
        """Basic DataFrame operations."""
        data = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
        df = spark.createDataFrame(data, ["id", "name"])
        df.show()
    
    test_spark_init() >> process_sample_data()

spark_test_dag()
