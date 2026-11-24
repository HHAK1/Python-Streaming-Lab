#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
for topic in transactions_csv transactions_json_raw transactions_json_filtered transactions_json_rejected; do
  docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done
