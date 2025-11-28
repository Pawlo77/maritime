#!/bin/bash

echo "Czekam na start Kafki..."
# cub to narzedzie wbudowane w obrazy Confluent do sprawdzania dostepnosci uslug
cub kafka-ready -b kafka:29092 1 60

echo "Tworze temat 'ais-data'..."
kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1 --topic ais-data

echo "Lista tematow:"
kafka-topics --list --bootstrap-server kafka:29092
