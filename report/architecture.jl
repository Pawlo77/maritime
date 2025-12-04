flowchart TD
    %% Top-down layout

    %% External Data Sources
    subgraph External["External Data Sources"]
        direction TB
        extAIS(["AIS Stream"])
        extMarinesia(["Marinesia API"])
        extOpenMeteo(["Open Meteo API"])
    end

    %% Docker Compose Environment
    subgraph DockerCompose["Docker Compose Environment"]
        direction TB

        %% Data Ingestion
        subgraph Ingestion["Data Ingestion & Streaming"]
            AIS(["AIS Downloader / Kafka Producer"])
            Kafka(["Kafka Broker"])
            DAG1(["DAG 1: Marinesia → HBase"])
            DAG2(["DAG 2: Open Meteo → HBase"])
            DAG4(["DAG 4: Kafka → HBase (Parquet)"])
            DAG3(["DAG 3: Spark Job → HBase"])
        end

        %% Storage
        subgraph Storage["Storage Layer"]
            HDFS(["HDFS"])
            HBase(["HBase"])
        end

        %% Processing
        subgraph Processing["Processing Layer"]
            Spark(["Apache Spark"])
        end

        %% Orchestration
        subgraph Orchestration["Orchestration"]
            Airflow(["Apache Airflow"])
        end
    end

    %% External connections
    extAIS --> AIS
    extMarinesia --> DAG1
    extOpenMeteo --> DAG2

    %% Ingestion to storage
    AIS -->|Insert data| Kafka
    Kafka --> DAG4
    DAG4 --> HBase

    %% Storage interactions
    HDFS <--> HBase
    Spark <--> HBase

    %% Airflow DAG triggers with schedule labels
    Airflow -.->|daily 00:00 UTC| DAG1
    Airflow -.->|hourly| DAG2
    Airflow -.->|near real-time| DAG4
    Airflow -.->|after data available| DAG3

    %% DAGs accessing storage
    DAG1 <--> HBase
    DAG2 <--> HBase
    DAG3 --> Spark
    DAG3 --> HBase

    %% Styling for readability
    classDef external fill:#e0f7fa,stroke:#006064,stroke-width:2px;
    classDef ingestion fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    classDef storage fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px;
    classDef processing fill:#f3e5f5,stroke:#4a148c,stroke-width:2px;
    classDef orchestration fill:#ffebee,stroke:#b71c1c,stroke-width:2px;

    class extAIS,extMarinesia,extOpenMeteo external;
    class AIS,Kafka,DAG1,DAG2,DAG4,DAG3 ingestion;
    class HDFS,HBase storage;
    class Spark processing;
    class Airflow orchestration;