from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manager import LockManager

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        #M3 Changes

        #list of (RIDS, transaction)
        self.held_locks = []
        #list of (undo_function, arguments)
        self.undo_log = []
        self.status = "PENDING"
        self.active_tables = [] 
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    #made it grades_table instead of table -> check ltr if this is fine
    def add_query(self, query, grades_table, *args):
        self.queries.append((query,grades_table, args))
        # use grades_table for aborting

    def add_undo(self, undo_func, *args):
        self.undo_log.append((undo_func, args))

    # added these two functions to make sure the transaction lifecycle will work with merges
    # probably going to change a lot of the merge functionalty :(
    def begin_tables(self):
        seen = set()
        self.active_tables = []
        for _, table, _ in self.queries:
            if table is None:
                continue
            tid = id(table)
            if tid in seen:
                continue
            seen.add(tid)
            self.active_tables.append(table)
            if hasattr(table, "begin_transaction"):
                table.begin_transaction()

    def end_tables(self, committed):
        for table in self.active_tables:
            if hasattr(table, "end_transaction"):
                table.end_transaction(committed)
        self.active_tables = []
        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        self.begin_tables()
        self.status = "RUNNING"
        
        # I wrapped all this in a try, except block for error handling stuff
        try: 
            for query, table, args in self.queries:
                result = query(*args, transaction=self)
                # If the query has failed the transaction should abort
                if result is False:
                    return self.abort()
            return self.commit()
        except Exception: 
            return self.abort()

    
    def abort(self):
        #reverse the undo log
        for undo_func, args in reversed(self.undo_log):
            try: 
                undo_func(*args)
            except Exception:
                pass
        #release all locks (Strict 2PL)
        self.undo_log.clear()
        self._release_all_locks()
        self.end_tables(committed = False)
        self.status = "ABORTED"
        return False

    
    def commit(self):
        #persistence handled by the Table/Bufferpool; 
        #commit in 2PL primarily involves releasing locks.
        self.undo_log.clear()
        self._release_all_locks()
        self.end_tables(committed = True)
        self.status = "COMMITTED"
        return True

    def _release_all_locks(self):
        for lock_info, rid in self.held_locks:
            try: 
                lock_info.unlock(rid, self)
            except Exception:
                pass
        self.held_locks = []



