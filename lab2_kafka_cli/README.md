# Lab 2: Hands-on Kafka using the CLI

Harutyun Hakobyan · Datalakes and Streaming

The deliverable is `REPORT.md`, with actual command transcripts in `evidence/`. The original [assignment](https://github.com/nourbenali/Hands-on-Kafka) is retained as `ASSIGNMENT.md`. No application code is used in this lab; `run_lab.sh` automates the Kafka command-line tools.

## Reproduce using Docker

Use a new, disposable lab broker. The `transactions` topic must not already exist; the script refuses to overwrite it.

```bash
docker compose up -d --wait --wait-timeout 180
bash run_lab.sh
```

The script creates one partition, sends and consumes records, checks committed offsets and restart behavior, resets an inactive group's offsets, increases the topic to three partitions, and inspects groups containing two and four consumers. `commands.log` records the executed commands and `result.txt` records assertion success.

Run against the lab broker only. Offset-reset operations affect that named lab group's reading position. These do not delete records. The compose file uses the teacher's Confluent 7.5.0/ZooKeeper setup, with health checks and a localhost-only broker port.

For reference verification without Docker, point `KAFKA_BIN` at an installed Apache Kafka `bin` directory and set `BROKER_MODE` accurately. This alternate mode exercises the same Kafka commands but does not establish that the required Docker setup ran. The included local transcript explicitly records native Kafka 3.5.2. The repository's Docker workflow provides separate container verification and artifacts.

AI assistance was used for the script and report. Results must be read with their recorded execution mode.
