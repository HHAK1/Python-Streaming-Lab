# Kafka labs: successful Docker verification

**Harutyun Hakobyan — 24 November 2026**

The required Docker execution has completed successfully. The earlier native-broker evidence in the lab directories is supplementary; it is not the only verification.

- [Successful Docker run and downloadable evidence](https://github.com/HHAK1/Python-Streaming-Lab/actions/runs/45477635395)
- Tested source commit: `05427e958fa4e1df30373554c5529ba142eb097a`
- Broker: Confluent Kafka 7.5.0 with ZooKeeper, using the submitted Docker Compose configuration.
- Python: 3.12; kafka-python: 2.0.6.
- Seven schema tests passed.
- All Kafka CLI assertions passed, including same-group resume, independent replay, offset reset, partition increase and two/four-consumer membership.
- CSV/JSON integration passed on all 200 clean transactions and all 14 supplied dirty rows.
- JSON dirty-data conversion accepted six distinct valid events and quarantined eight records.
- Schema evolution accepted two compatible cases and quarantined five incompatible cases.
- A second consumer-group replay increased filtered output from 200 to 400, demonstrating the documented at-least-once behavior.

The run's `kafka-lab-evidence` artifact contains freshly generated Docker command transcripts, raw outputs and `integration_results.json`. The workflow removes the checkout's native evidence before running, so its artifact does not reuse native results. Selected actual log lines are retained in [docker_verification/summary.log](docker_verification/summary.log).

This documentation-only follow-up records the successful run without changing the tested application or Docker configuration.
