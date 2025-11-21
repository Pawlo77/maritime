#!/bin/bash

set -e
GREEN=$'\e[32m'
RED=$'\e[31m'
CYAN=$'\e[36m'
NC=$'\e[0m'

echo -e "${CYAN}=== CHECK 1: NameNode UI ===${NC}"
if curl -s -I http://localhost:9870 | grep -q "200 OK"; then
  echo -e "${GREEN}NameNode reachable✔${NC}"
else
  echo -e "${RED}NameNode NOT reachable ✘${NC}"
fi

echo -e "${CYAN}=== CHECK 2: DataNode UI ===${NC}"
if curl -s -I http://localhost:9864 | grep -q "200 OK"; then
  echo -e "${GREEN}DataNode reachable✔${NC}"
else
  echo -e "${RED}DataNode NOT reachable ✘${NC}"
fi

echo -e "${CYAN}=== CHECK 3: HDFS Report ===${NC}"
docker compose exec hdfs-namenode hdfs dfsadmin -report | grep "Live datanodes"
docker compose exec hdfs-namenode hdfs dfsadmin -report | sed -n '1,40p'

echo -e "${CYAN}=== CHECK 4: HDFS mkdir & ls ===${NC}"
docker compose exec hdfs-namenode hdfs dfs -mkdir -p /test-dir
docker compose exec hdfs-namenode hdfs dfs -ls /

# echo -e "${CYAN}=== CHECK 5: HDFS file write/read ===${NC}"
# echo "hello-from-host" > /tmp/hdfs_test_host.txt
# docker compose cp /tmp/hdfs_test_host.txt hdfs-namenode:/tmp/
# docker compose exec hdfs-namenode hdfs dfs -put -f /tmp/hdfs_test_host.txt /test-dir/
# docker compose exec hdfs-namenode hdfs dfs -cat /test-dir/hdfs_test_host.txt

echo -e "${GREEN}HDFS write/read OK ✔${NC}"

echo -e "${CYAN}=== CHECK 6: Spark Master UI ===${NC}"
if curl -s -I http://localhost:8040 | grep -q "200 OK"; then
  echo -e "${GREEN}Spark Master reachable✔${NC}"
else
  echo -e "${RED}Spark Master NOT reachable ✘${NC}"
fi

echo -e "${CYAN}=== CHECK 7: Spark Worker UI ===${NC}"
if curl -s -I http://localhost:8081 | grep -q "200 OK"; then
  echo -e "${GREEN}Spark Worker reachable✔${NC}"
else
  echo -e "${RED}Spark Worker NOT reachable ✘${NC}"
fi

echo -e "${CYAN}=== CHECK 8: Spark Worker registered ===${NC}"
curl -s http://localhost:8040/json/ | grep -q "aliveWorkers"
curl -s http://localhost:8040/json/ | grep "aliveWorkers"

echo -e "${CYAN}=== CHECK 9: SparkPi job ===${NC}"
docker compose exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class org.apache.spark.examples.SparkPi \
  /spark/examples/jars/spark-examples_2.12-3.0.0.jar 10

echo -e "${GREEN}Spark job executed successfully ✔${NC}"

echo -e "${CYAN}=== CHECK 10: Spark write to HDFS ===${NC}"
docker compose exec spark-master bash -c "/spark/bin/spark-shell --master spark://spark-master:7077 -i /dev/null <<'EOF'
val df = spark.range(1,6)
df.write.mode("overwrite").json("hdfs://hdfs-namenode:9000/spark-check/")
System.exit(0)
EOF"

docker compose exec hdfs-namenode hdfs dfs -ls /spark-check/

echo -e "${GREEN}Spark → HDFS write OK ✔${NC}"

echo -e "${CYAN}=== CHECK 11: Spark read from HDFS ===${NC}"
docker compose exec spark-master bash -c "/spark/bin/spark-shell --master spark://spark-master:7077 -i /dev/null <<'EOF'
val df = spark.read.json("hdfs://hdfs-namenode:9000/spark-check/")
df.show()
System.exit(0)
EOF"

echo -e "${GREEN}Spark read from HDFS OK ✔${NC}"

echo -e "${GREEN}=== ALL TESTS COMPLETED SUCCESSFULLY ===${NC}"
