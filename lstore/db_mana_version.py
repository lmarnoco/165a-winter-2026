from lstore.table import Table
import os # use path, mkdir, isdir
import json

class Bufferpool_page():
    def __init(self):
        self.dirty = False # if True, do not evict
        self.pin = False # if True, do not evict
        pass

class Bufferpool():
    """
    Need to see if the page is in bufferpool already
    If it is not, need to load it into bufferpool and evict LRU page
     """
    def __init__(self):
        self.max_pages_bufferpool = 15
        self.LRU_key = None
        self.pages_in_pool = []
        self.keys{}


    def get_Bufferpool_page(self, path):
        """
        Needs to return Bufferpool_page
        """
        pass

    def evict_Bufferpool_page(self, path):
        """
        deploy replacemet policy:
        Evict single page (standard practice) --> LRU
        --> ONLY EVICT IF PAGE IS NOT PINNED
        --> Check if page is dirty and save before eviction
        --> later can test evicting all columns of base/tail page together
        --> later test eviction at granularity of page range
        """
        # Get the name for the page to evict from the pool list 
        eviction_page = None
        for page in self.pool: #LRU will be at the front of the list (ie self.pool[0]) if new pages are appended at end
            
            # could add print statements here for test cases

            if page.pin == False: # we are allowed to evict it
                eviction_page = page
                break  
            
            if eviction_page is None:
                raise Exception("No unpinned pages are available for eviction in the bufferpool")

        # If the page for eviction is dirty, we need to write the page to disk before eviction
        if eviction_page.dirty == True:
            ### TO-DO ###
            # open file
            # write contents back to disk 
                # Needs to be binary --> "wb" write binary (over write)
            # close file
            eviction_page.dirty = False 
            pass

        # Now remove the eviction_page from the pool
        self.pool.remove(eviction_page)
        # Also remove the key for the eviction_page in the lookup dictionary
        self.keys.pop() # I think this needs fixing but I gotta stop working on this rn
        return eviction_page
    

    def read():
        pass


    def write():
        pass








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

        # Initialize a dictionary of tables (self.tables) in the database object.
        self.tables = {} 
        
        # Set path for directory of database.
        self.path = None

    
    def open(self, directory):
        """
        Open the database from the path specified. 
        
        :param directory: string representing name for directory of database
        
        Load table metadata and reconstruct logical structure w/o reading all data

        Fixed Constant number of pages in bufferfpool which is defined when we create 
        and initialize the database
        """
        self.directory = os.path.expanduser(directory)
        self.max_pages_bufferpool = 15 # Set in open or in bufferpool class??

        #os.path.isdir(self.directory) Returns True if entry is a directory
        if not os.path.isdir(self.directory):
            os.mkdir(self.directory) # FileNotFoundError if parent directory not found
        
        # I AM VERY UNSURE ABOUT THIS PLS HELP:

        # Go through table folders
        for table_dir_name in os.listdir(self.directory): #os.listdir returns a list containing the names of the entries in the directory
            table_full_path = os.path.join(self.directory, table_dir_name)

            if os.path.isdir(table_full_path) == True: #then read the file and construct the table
                # Professor says we can't use Pickle --> use Json I guess
                # Table Class currently includes integer values for metadata?
                metadata_path = os.path.join(table_full_path, "metadata.json")
                # Edgecase : file doesn't have metadata for some reason? Just skip over it
                if not os.path.exists(metadata_path):
                    continue
                #  
                with open(metadata_path, "r") as f:
                    metadata = json.load(f)
                
                # Construct table from json metadata 
                table = Table(table_dir_name, metadata["num_columns"], metadata["key"])
                # Restore metadata
                table.next_rid = metadata.get("next_rid", 1)
                
                # JSON converts the dict keys to strings so need to convert back to int
                table.base_indirection = {
                    int(k): v
                    for k, v in metadata.get("base_indirection", {}).items()
                }
                # Update
                table.deleted = set(metadata.get("deleted", []))

                #populate the table dictionary with the table constructed from metadata
                self.tables[table.name] = table

        print("Database: Open")

    def close(self):
        """
        Writes dirty pages to disk.
        Close the database
        """
        # write all data to files at restart
        for name, table_object in self.tables.items():
            table_object.save_table(self.directory) #Save_table needs to get written in Table class!
        
        print("Database: Closed")
    
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
        
        #remove the table from self.tables dictionary
        del self.tables[name]
        print(f"Table {name} has been dropped")

    
   
    def get_table(self, name):
        """
        # Returns table with the passed name
        # First check to make sure that the table exists in self.tables and raise and error if it doesn't.
        # Return the table otherwise by accessing it from self.tables with the passed name as the key.

        """
        if name not in self.tables:
            raise TypeError("Table does not exist")
        
        return self.tables[name]
