"""Read transaction rows from a CSV file and publish them to a queue."""

import csv
import time
from pathlib import Path
from queue import Queue


DEFAULT_CSV = Path(__file__).with_name("transactions.csv")


class CSVProducer:
    def __init__(self, csv_path, q, delay=1.0):
        self.csv_path = csv_path
        self.q = q
        self.delay = delay

    def start(self):
        with open(self.csv_path, encoding="utf-8-sig", newline="") as source:
            for event in csv.DictReader(source):
                print(f"Produced: {event}", flush=True)
                self.q.put(event)
                time.sleep(self.delay)


if __name__ == "__main__":
    CSVProducer(DEFAULT_CSV, Queue()).start()
