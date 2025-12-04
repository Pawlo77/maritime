# maritime
A Comprehensive Approach to Real-Time Vessel Tracking, Historical Analysis, and Environmental Monitoring.

# Project Structure
- `src/ais-kafka-producer/`: Contains the AIS Kafka producer code that streams real-time vessel data to a Kafka topic. For it to work a `.env` file with necessary configurations is required, for example:
```env
TOPIC_NAME = "ais-stream"
BOOTSTRAP_SERVER = "kafka:29092"

API_KEY=<<API_KEY>>
BBOX="[[[-25.9973998712, 25.3439083708], [44.6049090453, 71.2982931893]]]" # Europe with Suez Canal
# BBOX="[[[-25.9973998712, 47.6517504467], [30.5839988025, 71.2828021422]]]" # North Europe
# BBOX="[[[-90, -180], [90, 180]]]" # Global
```

- `src/airflow/`: Contains the Airflow DAGs and configurations for orchestrating data processing tasks.
- `src/setup/`: Contains setup scripts and configuration files for the project environment.
- `src/docker-compose.yaml`: Docker Compose file to set up the entire environment including Kafka, Airflow, and other necessary services.
- `src/Makefile`: Makefile with commands to build, run, and manage the project components.

# Getting Started

1. Clone the repository to your local machine.
2. Navigate to the `src/` directory.
3. Create a `.env` file in the `ais-kafka-producer` directory with the necessary configurations.
4. Use make commands to build and run the project components. For example:
   ```bash
    make up
   ```
5. Access the Airflow web interface at `http://localhost:8090` to monitor and manage your DAGs.
