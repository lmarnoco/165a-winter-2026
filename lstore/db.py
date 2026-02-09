from lstore.table import Table
import os # use path, mkdir, isdir



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

    
    def open(self, path):
        """
        Open the database from the path specified. 
        
        :param path: string representing path for directory of database
        """
        #self.path = path

        # Check if path is a directory
        # If not, make it a directory
        # dir = os.path.isdir(self.path)
        # if not dir:
        #    os.mkdir(self.path)
        pass

        

    def close(self):
        """
        Close the database
        Later need to implement save function for tables and write to disk.
        """
        

    
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
