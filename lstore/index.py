"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns

        #init table
        self.table = table
        #create index for primary key
        #if table.key is not None:
        #    self.create_index(table.key)

        # fix where we need to create index for all columns? instead of just the primary key 
        for i in range(table.num_columns):
            self.create_index(i)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        #Check if an index actually exists for this column.
        # If not -> return an empty list to prevent crashing.
        if self.indices[column] is None:
            return []

        rids = self.indices[column].get(value, set())

        return list(rids)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            return []

        rids = []
        #hashmap because its in-place
        index_map = self.indices[column]
        
        #iterate over all keys in index & check range
        for key, val_rids in index_map.items():
            if begin <= key <= end:
                rids.extend(val_rids)
        
        rids = [rid for rid in rids if rid not in self.table.deleted]
                
        return rids
        

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = {}

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None

#helper functions: (we need them because otherwise data doesn't enter into the index)
    def add_entry(self, column_number, rid, value):
        if self.indices[column_number] is None: return
        if value not in self.indices[column_number]:
            self.indices[column_number][value] = set()
        self.indices[column_number][value].add(rid)

    def remove_entry(self, column_number, rid, value):
        if self.indices[column_number] is None: 
            return
            #Check if the value exists in the index
        if value in self.indices[column_number]:
            #Check if the RID is missing
            self.indices[column_number][value].discard(rid)

    def update_entry(self, column_number, rid, old_value, new_value):
       #If value didn't actually change -> nothing
        if old_value == new_value: 
            return
            #Remove RID from old value
        self.remove_entry(column_number, rid, old_value)
        #Add RID to new value
        self.add_entry(column_number, rid, new_value)
