"""Run the producer and consumer concurrently on a shared FIFO queue."""

from concurrent.futures import ThreadPoolExecutor
from queue import Queue

from consumer import Consumer
from producer import CSVProducer, DEFAULT_CSV


def main(csv_path=DEFAULT_CSV, delay=0.02):
    events = Queue()
    producer = CSVProducer(csv_path, events, delay)
    consumer = Consumer(events)

    with ThreadPoolExecutor(max_workers=2) as workers:
        receiving = workers.submit(consumer.start)
        sending = workers.submit(producer.start)
        try:
            sending.result()
        finally:
            # The marker follows the last transaction, even if reading failed.
            events.put(None)
        receiving.result()

    print(f"Processed {consumer.count} transactions from {len(consumer.totals_by_user)} "
          f"users. Total amount: {consumer.total_amount:.2f}")
    return consumer


if __name__ == "__main__":
    print("Starting the Python Streaming Pipeline...")
    main()
