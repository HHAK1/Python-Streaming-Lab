"""Convert CSV rows to typed JSON; quarantine malformed input and duplicate IDs."""
import argparse
import json
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import csv_rows, row_event, json_bytes, make_producer


def produce(path, bootstrap='localhost:9092', topic='transactions_json_raw',
            rejected_topic='transactions_json_rejected', delay=.05, version=1):
    producer = make_producer(bootstrap)
    accepted = rejected = 0
    seen = set()
    try:
        for line, row, raw in csv_rows(path):
            try:
                event = row_event(row, version)
                if event['transaction_id'] in seen: raise ValueError('duplicate transaction_id')
            except ValueError as error:
                reason = str(error)
                producer.send(rejected_topic, value=json_bytes(dict(source_line=line,
                              raw_csv=raw, reason=reason))).get(timeout=30)
                rejected += 1
                print(json.dumps(dict(source_line=line, status='rejected', reason=reason)), flush=True)
            else:
                producer.send(topic, key=str(event['user_id']).encode(),
                              value=json_bytes(event)).get(timeout=30)
                seen.add(event['transaction_id'])
                accepted += 1
                print(json.dumps(dict(source_line=line, status='accepted', event=event)), flush=True)
            time.sleep(delay)
        producer.flush()
    finally:
        producer.close()
    return dict(accepted=accepted, rejected=rejected)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--file', default=str(Path(__file__).resolve().parents[1]/'csv_lab/data/transactions.csv'))
    p.add_argument('--bootstrap', default='localhost:9092')
    p.add_argument('--topic', default='transactions_json_raw')
    p.add_argument('--rejected-topic', default='transactions_json_rejected')
    p.add_argument('--delay', type=float, default=.05)
    p.add_argument('--schema-version', type=int, choices=[1,2], default=1)
    a = p.parse_args()
    if a.delay < 0: p.error('--delay must be non-negative')
    print(json.dumps(produce(a.file,a.bootstrap,a.topic,a.rejected_topic,a.delay,a.schema_version)))
