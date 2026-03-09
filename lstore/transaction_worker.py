from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manager import LockManager
from lstore.transaction import Transaction
import threading
import time

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.lock_manager = LockManager()
        self.stats = []
        self.transactions = list(transactions) if transactions is not None else []
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

