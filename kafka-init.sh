#!/bin/bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic orders \
  --partitions 1 \
  --replication-factor 1