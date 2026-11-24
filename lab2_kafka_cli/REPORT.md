# Lab 2 report: Kafka CLI

**Harutyun Hakobyan — Datalakes and Streaming**

## Execution and evidence

I used Kafka's topic, console-producer, console-consumer, consumer-group and broker-version command-line tools. No Python application implements this lab. `run_lab.sh` records commands in `evidence/commands.log`; individual observations and the assertion result are beside it.

The included local execution used **Apache Kafka 3.5.2, Java 21 and KRaft**, not Docker. Docker is unavailable in the authoring runtime. The teacher's Docker requirement is addressed by the supplied Confluent 7.5.0/ZooKeeper Compose setup and the repository's Docker verification workflow. Read the workflow run and artifact separately for container evidence; do not infer Docker execution from the native transcript.

AI assistance was used to implement the automation and prepare the report. The offsets, record counts and assignments below are measured outputs.

## Commands and observations

The commands below are the Docker equivalents of the executed native commands. The full transcript records which launcher was actually used.

| Task | Command after `docker compose exec -T kafka` | Observation from the included run |
|---|---|---|
| Broker reachability | `kafka-broker-api-versions --bootstrap-server localhost:9092` | Broker 1 responded with supported APIs. |
| Topic creation | `kafka-topics --bootstrap-server localhost:9092 --create --topic transactions --partitions 1 --replication-factor 1` | One partition, one replica. |
| Inspect | `kafka-topics --bootstrap-server localhost:9092 --describe --topic transactions` | Partition 0 was led by broker 1. |
| Produce | `kafka-console-producer --bootstrap-server localhost:9092 --topic transactions` | Sent `first`, `second`, `third`. |
| First consumer | `kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions --group GROUP-resume --from-beginning --max-messages 3` | Read offsets 0, 1 and 2. |
| Inspect progress | `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group GROUP-resume` | Committed next offset 3 after the initial three records. |
| Same-group restart | Same consumer group, after sending `fourth` and `fifth` | Read only offsets 3 and 4, despite `--from-beginning`. |
| Independent replay | New group plus `--from-beginning --max-messages 5` | Read all five retained records. |
| Explicit replay | `kafka-consumer-groups --bootstrap-server localhost:9092 --group GROUP-resume --topic transactions --reset-offsets --to-earliest --execute` | With that group inactive, reset its next offset to 0; another read returned all five. |
| Increase parallelism | `kafka-topics --bootstrap-server localhost:9092 --alter --topic transactions --partitions 3` | Topic increased from one to three partitions. Existing records stayed on partition 0. |
| Inspect members | `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group GROUP-two --members --verbose` | Two consumers owned two and one partitions respectively. |
| Excess consumers | Same inspection for `GROUP-four` | Three consumers owned one partition each; the fourth had no assignment. |

`GROUP` above abbreviates the run-specific prefix stored in the transcript. The consumer options also print partitions/offsets and use explicit small `max.poll.records` in the restart case; the exact invocations are in the script.

The restart output is:

```text
Partition:0  Offset:3  fourth
Partition:0  Offset:4  fifth
```

After adding 12 unkeyed events to the three-partition topic, this particular producer batch placed them all on partition 2. That is a useful observation: partitioning enables parallelism but does not promise even distribution for a small batch. The four-member group's final committed offsets were 5, 0 and 12 for partitions 0, 1 and 2, with lag zero. Both consumer groups independently read the same retained topic.

## Reflection answers

**Why are topics logical streams?** A topic names a continuing sequence of events independently of how that sequence is split across broker partitions. Producers and consumers refer to the topic, while Kafka stores its partition logs.

**Why partitions?** They split storage and work. Several consumers in a group can process different partitions concurrently. This comes with a boundary: Kafka orders records within a partition, not across the whole topic.

**What do offsets do?** An offset identifies a record's position in a partition. A consumer group's committed offset records its next reading position. After processing offset 4, committing 5 means a restart should begin at 5. The commit does not remove records from the log.

**Why one consumer per partition within a group?** A single owner gives the group a clear reading position and ordered processing for that partition. Members share partitions rather than competing to advance the same partition cursor. A different group has its own ownership and progress.

**More consumers than partitions?** Some members are idle. Four members could not all receive work when the topic had only three partitions. Increasing the partition count can permit more active members, but it also changes key-based routing for future records.

## References

- [Apache Kafka 3.5 quick start](https://kafka.apache.org/35/getting-started/quickstart/)
- [Consumer-group inspection and resetting offsets](https://kafka.apache.org/36/operations/basic-kafka-operations/)
- [Teacher's task description](https://github.com/nourbenali/Hands-on-Kafka)
