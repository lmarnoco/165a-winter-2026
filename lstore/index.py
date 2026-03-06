"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

import os
import pickle

#Do we need these imports? -> 
import threading
from lstore.disk import Disk
from lstore.bufferpool import BUFFERPOOL
from lstore.record_info import RID
import lstore.config as Config


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns

        #init table
        self.table = table

        #I think I want to implement a locking mechanism with Rlock
        #create index for primary key
        if table.key is not None:
           self.create_index(table.key)

        self.lock = threading.RLock()

        # fix where we need to create index for all columns? instead of just the primary key -> EDIT FOR M2: secondary indexes created by the create_index method so I think we won't need this
        #for i in range(table.num_columns):
        #    self.create_index(i)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        with self.lock: # Protect read access
            if self.indices[column] is None:
                return []
            rids = self.indices[column].get(value, set())
            valid_rids = [rid for rid in rids if rid not in self.table.deleted]
            return list(valid_rids)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        with self.lock:
            if self.indices[column] is None:
                return []

        rids = []
        #hashmap because its in-place
        #Milestone 2 -> Hashmap stil valid, but needs to be altered in order to account for keys other than the primary one.
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
        # Initialize the index dictionary if it doesn't exist
        with self.lock:
            if self.indices[column_number] is None:
                self.indices[column_number] = {}
            
        # Iterate through all records currently in the table to populate the index
        # We only index Base RIDs as they represent the logical record
            for rid, meta in self.table.page_directory.items():
                is_tail = meta[1]
                if not is_tail and rid not in self.table.deleted:
                    latest_values = self.table.latest_cols(rid)
                    self.add_entry(column_number, rid, latest_values[column_number])
                
            # We only care about base records (tail records are part of base record history)
            # In page_directory, index 1 of tuple is 'is_tail'
            is_tail = self.table.page_directory[rid][1]
            if not is_tail:
                # Retrieve the latest values to ensure index accuracy
                latest_values = self.table.latest_cols(rid)
                column_value = latest_values[column_number]
                
                # Add the entry to the newly created index
                self.add_entry(column_number, rid, column_value)
    #what function to call to iterate over existing records? -> need this from table.py

    

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None

#helper functions: (we need them because otherwise data doesn't enter into the index)
    # should we move these into a new file?
    def add_entry(self, column_number, rid, value):
        with self.lock:
            if self.indices[column_number] is None: return
            if value not in self.indices[column_number]:
                self.indices[column_number][value] = set()
            self.indices[column_number][value].add(rid)

    def remove_entry(self, column_number, rid, value):
        with self.lock:
            if self.indices[column_number] is None: 
                return
            #Check if the value exists in the index
            if value in self.indices[column_number]:
                self.indices[column_number][value].discard(rid)
                if not self.indices[column_number][value]:
                    del self.indices[column_number][value]

    def update_entry(self, column_number, rid, old_value, new_value):
       with self.lock:
            if old_value == new_value: return
            self.remove_entry(column_number, rid, old_value)
            self.add_entry(column_number, rid, new_value)

#Milestone 2 Additions
#----------------------
    def save_to_disk(self, directory_path):
        # saves the index data to disk when Database.close() is called
        file_path = os.path.join(directory_path, f"{self.table.name}_index.pkl")
        with open(file_path, 'wb') as file:
            pickle.dump(self.indices, file)

    def load_from_disk(self, directory_path):
        # loads the index data from disk when Database.open() is called
        file_path = os.path.join(directory_path, f"{self.table.name}_index.pkl")
        if os.path.exists(file_path):
            with open(file_path, 'rb') as file:
                self.indices = pickle.load(file)
            return True
        return False


#Milestone 3 
