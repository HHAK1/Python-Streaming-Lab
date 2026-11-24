"""Route validated JSON to a filtered topic, invalid payloads to a dead-letter topic."""
import argparse
import json
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import consumer_args, commit_record, transaction, json_bytes, make_producer, strict_json_loads


def consume(bootstrap='localhost:9092', topic='transactions_json_raw', group='json-lab',
            filtered_topic='transactions_json_filtered', rejected_topic='transactions_json_rejected',
            limit=0, idle_seconds=10):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(topic, **consumer_args(bootstrap, group, 'json-router'))
    producer = make_producer(bootstrap)
    processed = accepted = rejected = 0
    last = time.monotonic()
    try:
        while not limit or processed < limit:
            batches = consumer.poll(timeout_ms=500)
            if not batches:
                if time.monotonic() - last >= idle_seconds: break
                continue
            for records in batches.values():
                for record in records:
                    try:
                        event = transaction(strict_json_loads(record.value.decode('utf-8')))
                    except (ValueError, UnicodeDecodeError) as error:
                        destination = rejected_topic
                        value = dict(source_topic=record.topic, source_partition=record.partition,
                                     source_offset=record.offset, raw=record.value.decode('utf-8',errors='replace'),
                                     reason=str(error))
                        rejected += 1
                    else:
                        destination = filtered_topic
                        # Source identity permits downstream deduplication after a replay.
                        value = {**event, '_source':dict(topic=record.topic, partition=record.partition,
                                                       offset=record.offset)}
                        accepted += 1
                    ack = producer.send(destination, key=record.key, value=json_bytes(value)).get(timeout=30)
                    # Acknowledged output first, then commit exactly this input record.
                    # A crash between these steps can repeat an output (at least once).
                    commit_record(consumer, record)
                    print(json.dumps(dict(partition=record.partition, offset=record.offset,
                                          destination=destination, output_offset=ack.offset)), flush=True)
                    processed += 1
                    last = time.monotonic()
                    if limit and processed >= limit: break
                if limit and processed >= limit: break
    finally:
        consumer.close(autocommit=False)
        producer.close()
    return dict(processed=processed, accepted=accepted, rejected=rejected)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bootstrap', default='localhost:9092')
    p.add_argument('--topic', default='transactions_json_raw')
    p.add_argument('--group', default='json-lab')
    p.add_argument('--filtered-topic', default='transactions_json_filtered')
    p.add_argument('--rejected-topic', default='transactions_json_rejected')
    p.add_argument('--limit', type=int, default=0)
    p.add_argument('--idle-seconds', type=float, default=10)
    a = p.parse_args()
    if a.limit < 0 or a.idle_seconds <= 0: p.error('invalid limit/idle timeout')
    print(json.dumps(consume(a.bootstrap,a.topic,a.group,a.filtered_topic,a.rejected_topic,a.limit,a.idle_seconds)))
