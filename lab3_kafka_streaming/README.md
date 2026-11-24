# Kafka streaming: CSV, JSON and dirty data

Harutyun Hakobyan · Datalakes and Streaming

This completes the CSV and JSON tasks from [nourbenali/Kafka-Streaming](https://github.com/nourbenali/Kafka-Streaming). The teacher's task descriptions and datasets are retained. AI assistance was used to implement, test and document the solution. Measured results are in `REPORT.md` and `evidence/`.

## Start and run

Use Python 3.9+ and Docker Compose v2. From this directory:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d --wait --wait-timeout 180
bash create_topics.sh
```

The Docker broker follows the teacher's Confluent 7.5.0 / ZooKeeper configuration. It exposes the plaintext teaching endpoint only on `127.0.0.1:9092`. One broker and replication factor one are for the lab, not a fault-tolerant production deployment. Do not start both lab Compose stacks at once because they use the same host port.

### Part I: CSV

Start the consumer in one terminal, then the producer in another:

```bash
python csv_lab/consumer_csv.py --group csv-clean --limit 200 --idle-seconds 60
python csv_lab/producer_csv.py --delay 0.05
```

There is no header message. Each complete CSV record becomes one UTF-8 Kafka message; the producer uses the user ID as key. The consumer prints the raw CSV, partition and offset. It distinguishes invalid field values and duplicate IDs without hiding the raw record. CSV remains a byte payload to the broker: Python supplies the parsing and validation.

For dirty data, use a fresh topic and group to keep observations separate:

```bash
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --topic transactions_csv_dirty --partitions 3 --replication-factor 1
python csv_lab/producer_csv.py --file csv_lab/data/transactions_dirty.csv --topic transactions_csv_dirty
python csv_lab/consumer_csv.py --topic transactions_csv_dirty --group csv-dirty --limit 14
```

All 14 dirty source rows are deliberately published in the CSV stage. Seven meet the schema, including one repeated transaction ID; seven fail it. Duplicate detection is local to one consumer process and is diagnostic, not durable deduplication.

### Part II: JSON

```bash
python json_lab/producer_json.py --file csv_lab/data/transactions.csv
python json_lab/consumer_json.py --group json-clean --limit 200
```

The raw topic holds validated structured events. The consumer writes accepted events to `transactions_json_filtered` and invalid payloads to `transactions_json_rejected`. It adds the input topic, partition and offset to accepted output so a downstream sink can identify replayed records.

For the supplied dirty CSV, use separate topics or an isolated lab broker:

```bash
python json_lab/producer_json.py --file csv_lab/data/transactions_dirty.csv
python json_lab/consumer_json.py --group json-dirty --limit 6
```

If reusing the same raw topic after the clean run, a new group also replays the 200 clean records. Use unique topic names with `--topic`, `--filtered-topic` and `--rejected-topic` for an isolated experiment. The integration script does this automatically.

### Schema decisions

- `transaction_id`, `user_id`: positive JSON integers; booleans and fractional IDs rejected.
- `amount`: non-negative decimal encoded as a string with exactly two decimal places, avoiding binary floating-point rounding. Non-finite values and more than two decimal places are rejected.
- `timestamp`: ISO datetime. Source values have no timezone, so no UTC claim is added.
- `schema_version`: integer 1 or 2; version 2 permits `currency` and defaults it to EUR. Other currencies and unsupported versions are rejected explicitly.
- Adding an optional field is compatible and its value is preserved. Removing a required field is rejected and quarantined. Invalid JSON, arrays, invalid UTF-8 and non-standard JSON constants are rejected.
- Input conversion rejects duplicate transaction IDs within one file run; this memory is not preserved across separate runs. The supplied flight-count JSON is retained as an upstream example but is not mixed into the transaction schema.

## Replay, groups and ordering

Offsets belong to `(group, topic, partition)` and identify the next record. A restart with the same group resumes after the last committed record. A new group using `earliest` replays retained data. `earliest` does not override an existing commit.

Manual commits occur only after printing/processing a CSV record, or after Kafka acknowledges a routed JSON output. This provides at-least-once behavior: a crash after output but before commit can produce a duplicate. Manual commits and `acks=all` do not make this exactly once. An atomic Kafka transaction or an idempotent external sink would be needed for stronger effects guarantees.

Three partitions permit up to three active consumers per group. With two consumers, one receives two partitions and one receives one; with four, one is idle. A partition has one owner within a standard consumer group. Different groups read independently.

Order exists within each partition, not across them. A stable key and partition count keep one user's events on one partition. Increasing the partition count changes key mapping for future messages; do not assume global arrival order or permanent key affinity after that change.

## Verification

```bash
python -m unittest discover -s tests -p 'test_*.py' -v
BROKER_MODE='Docker Compose / Confluent 7.5.0' python tests/integration.py
```

The real-broker checks exercise all 200 clean source rows, partial commits and restart, a new group's replay, the 14 dirty CSV rows, JSON quarantine, two/four-member assignments, increasing partitions, schema evolution and duplicate output after replay. Every run creates uniquely named verification topics and retains JSONL output and a result summary under `evidence/`. It does not delete existing user topics.

Read `evidence/integration_results.json` for the actual run's broker mode. Local runs used native Apache Kafka 3.5.2 in KRaft mode because Docker is unavailable in the authoring runtime. The Docker-based GitHub Actions workflow checks the required container setup separately and retains its own evidence artifact. Do not label a native run as Docker evidence.

## References

- [Kafka 3.5 quick start](https://kafka.apache.org/35/getting-started/quickstart/)
- [Kafka consumer groups and offset reset](https://kafka.apache.org/36/operations/basic-kafka-operations/)
- [kafka-python 2.0.6 consumer API](https://kafka-python.readthedocs.io/en/2.0.6/apidoc/KafkaConsumer.html)
- [kafka-python 2.0.6 producer API](https://kafka-python.readthedocs.io/en/2.0.6/apidoc/KafkaProducer.html)

The upstream client pin was 2.0.2. This solution pins 2.0.6 for compatibility with the Python 3.12 verification runtime; the Kafka client API remains explicit in the code.
