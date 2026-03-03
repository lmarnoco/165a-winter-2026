from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        #M3 Changes

        #list of (RID, lock_type) or (table, RID, type)
        self.held_locks = []
        #list of (undo_function, arguments)
        self.undo_log = []
        self.status = "PENDING"
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

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, table, args in self.queries:
            result = query(*args, transaction=self)
            # If the query has failed the transaction should abort
            if result is False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        #reverse the undo log
        for undo_func, args in reversed(self.undo_log):
            undo_func(*args)
        #release all locks (Strict 2PL)
        self._release_all_locks()
        self.status = "ABORTED"
        return False

    
    def commit(self):
        #persistence handled by the Table/Bufferpool; 
        #commit in 2PL primarily involves releasing locks.
        self._release_all_locks()
        self.status = "COMMITTED"
        return True

    def _release_all_locks(self):
        for lock_info in self.held_locks:
            pass
        self.held_locks = []



