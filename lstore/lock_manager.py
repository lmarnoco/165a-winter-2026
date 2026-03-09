import threading
from collections import deque
import time



class LockManager():
    
    def __init__(self):
        self.lock_table = {}
        self.latch = threading.Lock()
    

    def remember_lock(self, transaction, rid):
        lock_ref = (self, rid)
        if lock_ref not in transaction.held_locks:
            transaction.held_locks.append(lock_ref)

    #Have Check if the RID actually exists before this not in this file
    def lock(self, transaction, rid, type):
        if type not in ("S", "X"):
            return False # added quick sanity check for faliure cases 

        with self.latch:
            record = self.lock_table.get(rid)

            if record is None:
                self.lock_table[rid] = {"mode": type, "transactions_held" : {transaction}}
                if (self, rid) not in transaction.held_locks:
                    transaction.held_locks.append((self, rid))
                return True

            if transaction in record["transactions_held"]:
                if record["mode"] == "X":
                    # already has the strongest lock?
                    return True
                if type == "S":
                    # already has S
                    return True
                if type == "X" and record["transactions_held"] == {transaction}:
                    # upgrade S -> X 
                    record["mode"] = "X"
                    return True

            can_grant = False

            if type == "S" and record["mode"] == "S":
                can_grant = True
            elif type == "X" and record["transactions_held"] == {transaction}:
                # record["mode"] = "X"  --> I think I handled this already at the top
                # transaction.held_locks.append((self, rid))  
                return True
            elif not record["transactions_held"]:   
                can_grant = True
            
            if can_grant:
                if type == "X":
                    record["mode"] = "X"
                elif record["mode"] is None:
                    record["mode"] = "S"

                record["transactions_held"].add(transaction)
                if (self, rid) not in transaction.held_locks:
                    transaction.held_locks.append((self, rid))
                return True
            
            # took out the else, don't think we need it cuz it should just fail on any conflict?
            return False

            

    def unlock(self, rid, transaction):
        with self.latch:
            record = self.lock_table.get(rid)

            if record is None:
                return False
            
            holders = record["transactions_held"]
            if transaction not in holders:
                return False

            holders.remove(transaction)
            if not holders:
                record["mode"] = None
                del self.lock_table[rid]
            
            return True

           


    

    # def unlock_all(self, transaction):
    #     with self.latch:
    #         for lock_manager, rid in list(transaction.held_locks):
    #             lock_manager.unlock(rid, transaction)
    #         transaction.held_locks = []


