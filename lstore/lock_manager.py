from lstore.table import Table, Record

import threading
from collections import deque
import time



class LockManager():
    
    def __init__(self):
        self.lock_table = {}
        self.latch = threading.Lock()
    

    #Have Check if the RID actually exists before this not in this file
    def lock(self, transaction, rid, type):

        event = threading.Event()

        with self.latch:
        
            record = self.lock_table.get(rid)

            if record is None:
                self.lock_table[rid] = {"mode": type, "transactions_held": {transaction}, "queue": deque()}
                transaction.held_locks.append((self, rid))
                return True


            if record["mode"] == "S" and type == "S" and not any(t == "X" for _, t, _ in record["queue"]):
                record["transactions_held"].add(transaction)
                transaction.held_locks.append((self,rid))
                return True
            

            elif not record["transactions_held"] and type == "X":
                record["mode"] = "X"
                record["transactions_held"].add(transaction)
                transaction.held_locks.append((self, rid))
                return True

            record["queue"].append((transaction, type, event))
        event.wait()

        with self.latch:
            record = self.lock_table.get(rid)
            if record and transaction in record["transactions_held"]:
                return True
            else:
                return False

    def unlock(self, rid, transaction):
        with self.latch:
            record = self.lock_table.get(rid)

            if record is None:
                return False
            
            record["transactions_held"].remove(transaction)
            if not record["transactions_held"]:
                record["mode"] = None

            temp = deque()

            while record["queue"]:          
                next_transaction, next_type, next_event = record["queue"].popleft()

                if next_type == "S":
                    if record["mode"] in ("S", None):
                        record["mode"] = "S"
                        record["transactions_held"].add(next_transaction)
                        next_transaction.held_locks.append((self, rid))
                        next_event.set()
                    else:
                        temp.append((next_transaction, next_type, next_event))

                elif next_type == "X":
                    if not record["transactions_held"]:
                        record["mode"] = "X"
                        record["transactions_held"].add(next_transaction)
                        next_transaction.held_locks.append((self, rid))
                        next_event.set()
                    else:
                        temp.append((next_transaction, next_type, next_event))
            record["queue"] = temp
        return True


    

    # def unlock_all(self, transaction):
    #     with self.latch:
    #         for lock_manager, rid in list(transaction.held_locks):
    #             lock_manager.unlock(rid, transaction)
    #         transaction.held_locks = []


