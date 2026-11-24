"""Print CSV records and offsets; commit only after processing/output succeeds."""
import argparse
import csv
import json
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import consumer_args, commit_record, row_event


def consume(bootstrap='localhost:9092', topic='transactions_csv', group='csv-lab',
            limit=0, idle_seconds=10, client='csv-consumer'):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(topic, **consumer_args(bootstrap, group, client))
    processed = valid = invalid = 0
    seen = set()
    last = time.monotonic()
    try:
        while not limit or processed < limit:
            batches = consumer.poll(timeout_ms=500)
            if not batches:
                if time.monotonic() - last >= idle_seconds: break
                continue
            for records in batches.values():
                for record in records:
                    raw = record.value.decode('utf-8')
                    try:
                        parsed = list(csv.reader([raw], strict=True))
                        event = row_event(parsed[0])
                        duplicate = event['transaction_id'] in seen
                        seen.add(event['transaction_id'])
                        status = 'duplicate' if duplicate else 'valid'
                        valid += 1
                    except (ValueError, csv.Error) as error:
                        status = f'invalid: {error}'
                        invalid += 1
                    # Raw CSV is retained even if its schema is invalid.
                    print(json.dumps(dict(partition=record.partition, offset=record.offset,
                                          raw_csv=raw, status=status)), flush=True)
                    commit_record(consumer, record)
                    processed += 1
                    last = time.monotonic()
                    if limit and processed >= limit: break
                if limit and processed >= limit: break
    finally:
        consumer.close(autocommit=False)
    return dict(processed=processed, valid=valid, invalid=invalid)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bootstrap', default='localhost:9092')
    p.add_argument('--topic', default='transactions_csv')
    p.add_argument('--group', default='csv-lab')
    p.add_argument('--client', default='csv-consumer')
    p.add_argument('--limit', type=int, default=0)
    p.add_argument('--idle-seconds', type=float, default=10)
    a = p.parse_args()
    if a.limit < 0 or a.idle_seconds <= 0: p.error('invalid limit/idle timeout')
    print(json.dumps(consume(a.bootstrap, a.topic, a.group, a.limit, a.idle_seconds, a.client)))
