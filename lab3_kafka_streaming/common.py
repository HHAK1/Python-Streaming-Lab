"""Shared schema, CSV parsing and Kafka configuration for the two lab stages."""
import csv
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import StringIO
from pathlib import Path

FIELDS = ('transaction_id', 'user_id', 'amount', 'timestamp')


def csv_rows(path):
    """Yield physical CSV records, including malformed field counts for quarantine."""
    with Path(path).open(encoding='utf-8-sig', newline='') as source:
        reader = csv.reader(source, strict=True)
        header = next(reader)
        if tuple(header) != FIELDS:
            raise ValueError(f'Expected header {FIELDS}, found {header}')
        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            # Re-serialize the complete logical record, not a split physical line.
            output = StringIO(newline='')
            csv.writer(output, lineterminator='\n').writerow(row)
            yield reader.line_num, row, output.getvalue().removesuffix('\n')


def positive_integer(value, name):
    if isinstance(value, bool):
        raise ValueError(f'{name} must be a positive integer')
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value.strip().isascii() and value.strip().isdigit():
        result = int(value)
    else:
        raise ValueError(f'{name} must be a positive integer')
    if result <= 0:
        raise ValueError(f'{name} must be positive')
    return result


def transaction(event):
    """Normalize schema versions 1/2; reject bad required fields, allow extras."""
    if not isinstance(event, dict):
        raise ValueError('event must be an object')
    version = event.get('schema_version', 1)
    if type(version) is not int or version not in (1, 2):
        raise ValueError('unsupported schema_version')
    if any(k not in event or event[k] in ('', None) for k in FIELDS):
        raise ValueError('missing required transaction field')
    tid = positive_integer(event['transaction_id'], 'transaction_id')
    uid = positive_integer(event['user_id'], 'user_id')
    value = event['amount']
    if isinstance(value, bool):
        raise ValueError('amount must be a number')
    try:
        amount = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError('amount is not numeric') from error
    if not amount.is_finite() or amount < 0:
        raise ValueError('amount must be finite and non-negative')
    if amount.as_tuple().exponent < -2:
        raise ValueError('amount has more than two decimal places')
    # Source timestamps are timezone-naive. Preserve that fact; do not invent UTC.
    try:
        stamp = datetime.fromisoformat(str(event['timestamp']))
    except ValueError as error:
        raise ValueError('timestamp must be ISO 8601') from error
    normalized = {**event, 'schema_version': version, 'transaction_id': tid,
                  'user_id': uid, 'amount': format(amount, '.2f'),
                  'timestamp': stamp.isoformat(sep=' ')}
    if version == 2 and normalized.get('currency', 'EUR') != 'EUR':
        raise ValueError('this exercise supports EUR only')
    if version == 2:
        normalized.setdefault('currency', 'EUR')
    return normalized


def row_event(row, version=1):
    if len(row) != len(FIELDS):
        raise ValueError(f'expected 4 fields, found {len(row)}')
    return transaction(dict(zip(FIELDS, row), schema_version=version))


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8')


def make_producer(bootstrap):
    from kafka import KafkaProducer
    # Preserve send order on retries. This client/config is at-least-once,
    # not an idempotent/exactly-once transaction pipeline.
    return KafkaProducer(bootstrap_servers=bootstrap, acks='all', retries=3,
                         max_in_flight_requests_per_connection=1,
                         request_timeout_ms=20000, max_block_ms=20000)


def consumer_args(bootstrap, group, client):
    return dict(bootstrap_servers=bootstrap, group_id=group, client_id=client,
                auto_offset_reset='earliest', enable_auto_commit=False,
                max_poll_records=100, session_timeout_ms=10000,
                heartbeat_interval_ms=1000)


def commit_record(consumer, record):
    from kafka import TopicPartition
    from kafka.structs import OffsetAndMetadata
    consumer.commit({TopicPartition(record.topic, record.partition):
                     OffsetAndMetadata(record.offset + 1, '')})


def strict_json_loads(raw):
    def reject(value):
        raise ValueError(f'non-standard JSON constant: {value}')
    return json.loads(raw, parse_constant=reject)
