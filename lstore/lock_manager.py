import threading
from collections import deque
import time



class LockManager():
    
    def __init__(self):
        self.lock_table = {}
        self.latch = threading.Lock()
    

    #Have Check if the RID actually exists before this not in this file
    def lock(self, transaction, rid, type):
        with self.latch:
            record = self.lock_table.get(rid)

            if record is None:
                self.lock_table[rid] = {"mode": type, "transactions_held" : {transaction}}
                transaction.held_locks.append((self, rid))
                return True

            can_grant = False

            if type == "S" and record["mode"] == "S":
                can_grant = True
            elif type == "X" and record["transactions_held"] == {transaction}:
                record["mode"] = "X"
                transaction.held_locks.append((self, rid))  
                return True
            elif not record["transactions_held"]:
                can_grant = True
            
            if can_grant:
                record["transactions_held"].add(transaction)
                transaction.held_locks.append((self,rid))
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
                del self.lock_table[rid]
            
            return True

           


    

    # def unlock_all(self, transaction):
    #     with self.latch:
    #         for lock_manager, rid in list(transaction.held_locks):
    #             lock_manager.unlock(rid, transaction)
    #         transaction.held_locks = []


