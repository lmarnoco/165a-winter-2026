from lstore.table import Table
import os # use path, mkdir, isdir
from lstore.bufferpool import BufferPool

class Database():
    """
    Handles high-level operations such as starting and shutting down the database instance and 
    loading thedatabase from stored disk files
    
    Initialize a dictionary of tables (self.tables) in the database object.

    When (create_table) is called, assisgn new table with passed params to self.tables dict 
    - :param name:  key
    - table object:  value

    When (drop_table) is called, the table with the passed name is removed from self.tables dict
    
    When (get_table) is called, self.tables is searched for the table with the passed name and returned if found.

    """
    def __init__(self):

        self.tables: dict[str, Table] = {}
        self.path: str = None
        self.bufferpool = None  # reserved for future use

    
    def open(self, path):
        """
        Open the database from the path specified. 
        
        :param path: string representing path for directory of database
        """
        self.path = path
        if not os.path.isdir(self.path):
            os.makedirs(self.path)

        self.bufferpool = BufferPool(capacity=32, path=path)

        # Reload table schemas if they exist
        meta_path = os.path.join(self.path, "tables.meta")
        if os.path.exists(meta_path):
            self._load_tables_meta(meta_path)

        
        

    def close(self):
        """
        Close the database
        Later need to implement save function for tables and write to disk.
        """
        for table in self.tables.values():
            table.stop_merge_thread()

        if self.path:
        # Create all table directories first
            for table in self.tables.values():
                table_dir = os.path.join(self.path, table.name)
                os.makedirs(table_dir, exist_ok=True)
        
            # Now flush bufferpool (all dirs exist)
            if self.bufferpool is not None:
                self.bufferpool.flush_all()
        
            # Save metadata
            for table in self.tables.values():
                table._save_metadata(os.path.join(self.path, table.name))
        
            self._save_tables_meta()
        
    
    def create_table(self, name, num_columns, key_index):

        """
        # Creates a new table
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key_index: int       #Index of table key in columns

        """
        # If the table already exists, don't create a new one, just return the existing one 
        if name in self.tables:
            return self.tables[name]
        
        # Create new table object with passed parameters
        table = Table(name, num_columns, key_index)

        table.bufferpool = self.bufferpool
        
        # Assign the table object to the table name in self.tables dictionary
        self.tables[name] = table

        # Return table object
        return table

    
    
    def drop_table(self, name):
        """
        # Deletes the specified table
        # First check to make sure that the table exists in self.tables and raise and error if it doesn't.

        """
        if name not in self.tables:
            raise TypeError("Table does not exist") 
        
        # Stop the background merge thread cleanly before dropping
        self.tables[name].stop_merge_thread()

        #remove the table from self.tables dictionary
        del self.tables[name]
        print(f"Table {name} has been dropped")

        # SAEE - NEED TO ALSO STOP BACKGROUNG MERGE THREAD HERE IF ITS RUNNING 

    
   
    def get_table(self, name):
        """
        # Returns table with the passed name
        # First check to make sure that the table exists in self.tables and raise and error if it doesn't.
        # Return the table otherwise by accessing it from self.tables with the passed name as the key.

        """
        if name not in self.tables:
            raise TypeError("Table does not exist")
        
        return self.tables[name]

    def _save_tables_meta(self):
        meta_path = os.path.join(self.path, "tables.meta")
        with open(meta_path, 'w') as f:
            for name, table in self.tables.items():
                f.write(f"{name},{table.num_columns},{table.key}\n")

    def _load_tables_meta(self, meta_path: str):
        with open(meta_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                name, num_columns, key = line.split(',')
                table = Table(name, int(num_columns), int(key))
                table.bufferpool = self.bufferpool
                table.load_from_disk(self.path)      # restore pages + metadata
                self._rebuild_indexes(table)          # indexes are not persisted; rebuild from data
                self.tables[name] = table

    @staticmethod
    def _rebuild_indexes(table: Table):
        """Re-populate in-memory indexes from stored records after a restart."""
        for base_rid, (range_id, is_tail, pageset_id, slot) in table.page_directory.items():
            if is_tail:
                continue
            if base_rid in table.deleted:
                continue
            latest_cols = table.latest_cols(base_rid)
            for c in range(table.num_columns):
                table.index.add_entry(c, base_rid, latest_cols[c])