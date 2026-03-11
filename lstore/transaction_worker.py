from lstore.table import Table, Record
from lstore.index import Index
from lstore.transaction import Transaction, Aborts, SHARED_LOCK_MANAGER
import threading
import time
import random

class TransactionWorker:

    # idea to is to make all the workers share one lock table 
    shared_lock_manager = SHARED_LOCK_MANAGER

    MAX_RETRy = 200
    MAX_RETRY_SEC = 10.0
    NOPE_BASE_SEC = 0.001
    NOPE_CAP_SEC = 0.05

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None, lock_manager = None):
        self.lock_manager = lock_manager or TransactionWorker.shared_lock_manager   # added this implementation instead
        self.stats = []
        self.transactions = list(transactions) if transactions is not None else []
        for t in self.transactions:    # I think adding this shared worker should help manage any conflicts? 
            t.lock_manager = self.lock_manager 
        self.result = 0
        self._thread = None
        pass

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        t.lock_manager = self.lock_manager
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        pass
        # here you need to create a thread and call __run
        self._thread = threading.Thread(target=self.__run, daemon=True)
        self._thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if self._thread is not None:
            self._thread.join()


# Saee - I'm adding a different version of this to try and fix the milestone 3 bug...
# right now I think the while True loop is running forever because any aborted transactions are being retried forever 
# changed this even more to handle retries better? because we do want retry but not forever 
# basically me trying to put a bunch of time limits on things so it still runs properly and does retries properly
    def __run(self):
        for transaction in self.transactions:
            attempts = 0
            start = time.monotonic()
            
            while True:
                transaction.reset()
                result = transaction.run()

                if result:
                    self.stats.append(True)
                    break

                attempts += 1
                if transaction.abort_reason != Aborts.LOCK_CONFLICT:
                    self.stats.append(False)
                    break

                time_passed = time.monotonic() - start
                if attempts >= self.MAX_RETRy or time_passed >= self.MAX_RETRY_SEC:
                    transaction.mark_abort(
                        Aborts.NON_RETRY,
                        f"retry limit exceeded: attempts={attempts}, time={time_passed:.3f}s",
                    )
                    self.stats.append(False)
                    break

                nope = min(
                    self.NOPE_BASE_SEC * (2 ** min(attempts, 10)),
                    self.NOPE_CAP_SEC,
                )
                time.sleep(nope * (0.5 + random.random()))

        self.result = sum(1 for s in self.stats if s)

"""
    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            while True:
                # Reset transaction state for each attempt
                transaction.undo_log = []
                transaction.held_locks = []
                transaction.status = "PENDING"

                result = transaction.run()

                if result:  # committed
                    self.stats.append(True)
                    break
                else:       # if aborted aborted retry
                    self.stats.append(False)
                    time.sleep(0)  # yield to other threads before retrying

        self.result = len([s for s in self.stats if s])
"""
