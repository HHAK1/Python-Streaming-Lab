"""CSV producer: preserve each data row as one Kafka record, including dirty rows."""
import argparse
import json
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import csv_rows, make_producer


def produce(path, bootstrap='localhost:9092', topic='transactions_csv', delay=.05):
    producer = make_producer(bootstrap)
    sent = 0
    try:
        for line, row, raw in csv_rows(path):
            key = row[1].strip().encode('utf-8') if len(row) > 1 else None
            ack = producer.send(topic, key=key, value=raw.encode('utf-8')).get(timeout=30)
            print(json.dumps(dict(source_line=line, partition=ack.partition, offset=ack.offset,
                                  raw_csv=raw)), flush=True)
            sent += 1
            time.sleep(delay)
        producer.flush()
    finally:
        producer.close()
    return sent


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--file', default=str(Path(__file__).with_name('data') / 'transactions.csv'))
    p.add_argument('--bootstrap', default='localhost:9092')
    p.add_argument('--topic', default='transactions_csv')
    p.add_argument('--delay', type=float, default=.05)
    a = p.parse_args()
    if a.delay < 0: p.error('--delay must be non-negative')
    print(json.dumps({'sent': produce(a.file, a.bootstrap, a.topic, a.delay)}))
