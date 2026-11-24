# Kafka streaming report

**Harutyun Hakobyan — Datalakes and Streaming**

## Measured execution

The included run used real native Apache Kafka 3.5.2/KRaft and kafka-python 2.0.6. The authoring runtime has no Docker. The supplied Docker Compose configuration and GitHub Actions workflow repeat the tests using Confluent 7.5.0/ZooKeeper; their execution status and artifact are separate evidence. AI assistance was used for implementation, testing and this analysis.

The source data and all test outputs are included. Each JSONL file contains the individual generated/consumed records and a final summary. `integration_results.json` records the run ID, broker mode and assertions.

| Check | Observed result |
|---|---:|
| Clean CSV published | 200 |
| First same-group read | 17 |
| Restart: remaining records | 183 |
| Second restart: remaining | 0 |
| New group: replay | 200 |
| Dirty CSV delivered | 14 |
| Dirty CSV schema-valid / invalid | 7 / 7 |
| Clean JSON accepted / rejected | 200 / 0 |
| Clean JSON routed | 200 |
| Dirty JSON accepted / rejected | 6 / 8 |
| Schema cases accepted / rejected | 2 / 5 |
| Filtered output after a second-group replay | 400 |

## Interpretation

The clean CSV's 200 source transactions were all delivered. The first consumer stopped after 17 records and committed only the corresponding processed offsets. Restarting that group consumed 183; the next restart consumed zero. A separate group consumed all 200 again. This distinguishes committed progress from retention: Kafka still had the data after the first group finished.

The dirty CSV has 14 data records. Seven have valid field values, including a repeated transaction ID; seven fail the schema. The CSV stage prints every raw record. The JSON conversion accepts six distinct valid transactions and quarantines eight records, including the duplicate. Rejections identify missing fields, a nonnumeric user ID, malformed timestamps, a nonnumeric amount and the truncated final row. The quarantine retains the source row and reason so errors can be investigated rather than silently discarded.

The schema experiment adds an optional field, supplies a version-2 event, removes a required field, and injects malformed JSON, an array, invalid UTF-8 and an unsupported version. The compatible additions are accepted; the five incompatible payloads are quarantined. The required transaction fields are checked by the consumer as well as the producer, so other producers cannot bypass validation.

The saved 200-row replay also preserves increasing offsets within each partition and increasing transaction IDs for each user key, across three populated partitions. This does not establish a global order across partitions.

Two consumers shared the three partitions as 2+1. Four consumers shared them as 1+1+1+0. The test asserted non-overlap and complete coverage of partition assignments. A separate operation increased the CSV verification topic from three to five partitions. This is a routing/configuration change, not a claim that every workload becomes faster.

Replaying the clean JSON source with another group increased filtered output from 200 to 400 records. This is expected in an at-least-once, nontransactional pipeline. Each output's `_source` identity can support downstream deduplication; this exercise does not implement a persistent exactly-once sink.

## Reflection: CSV

Kafka stores byte sequences; it does not need to understand CSV columns. The application must agree on the header, quoting, field types and record boundaries. A physical newline is not always a complete CSV record if quoted fields contain line breaks, so the producer uses Python's CSV parser rather than splitting text blindly. Missing columns and invalid field values are application-level errors.

Offsets make restart and replay possible. Commit the next offset after successful processing, not merely after fetching a batch. Otherwise a consumer can acknowledge prefetched records it has not processed.

## Reflection: JSON

JSON carries field names and types with the event, making optional fields easier to evolve than positional CSV. It does not remove the need for a schema: a missing required amount, incompatible type or unsupported version still needs a defined response. Here that response is an explicit rejection topic with diagnostic context.

Kafka can act as a replayable event log while retention keeps the original records available. It is not an eternal source of truth by default: retention, compaction, durability settings and access policies determine which historical events remain recoverable.

## Limits and task coverage

The code implements the CSV producer/consumer, JSON conversion, validation, schema additions/removals, raw/filtered/rejected topics, manual offset commits and replay. Tests use the full supplied clean/dirty transaction files. Acknowledged sends and one in-flight request preserve per-partition send order under retries, but they do not prevent duplicate delivery. Key-based grouping assumes an unchanged partition count. No global ordering, exactly-once effects or production fault tolerance is claimed. Producer-side duplicate detection lasts only for one run.

Docker validation must be read from the actual workflow status; a native broker run alone does not satisfy that part of the installation requirement. Run instructions and official references are in README.md. The preserved flight-count JSON file belongs to a different example schema and is not substituted for the transaction task.
