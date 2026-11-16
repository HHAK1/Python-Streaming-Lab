"""Receive transactions and update the running totals as each row arrives."""

import time
from decimal import Decimal
from queue import Queue


class Consumer:
    def __init__(self, q):
        self.q = q
        self.count = 0
        self.total_amount = Decimal("0.00")
        self.totals_by_user = {}

    def start(self):
        while True:
            event = self.q.get()
            try:
                if event is None:
                    return
                print(f"Received: {event}", flush=True)
                self.process(event)
            finally:
                self.q.task_done()

    def process(self, event):
        time.sleep(0.01)
        user_id = int(event["user_id"])
        amount = Decimal(event["amount"])
        if not amount.is_finite():
            raise ValueError(f"Invalid amount for transaction {event['transaction_id']}")
        self.totals_by_user[user_id] = self.totals_by_user.get(user_id, 0) + amount
        self.total_amount += amount
        self.count += 1


if __name__ == "__main__":
    # With no producer, the standalone consumer waits for its first message.
    Consumer(Queue()).start()
