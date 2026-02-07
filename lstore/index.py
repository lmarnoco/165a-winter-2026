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
        if table.key is not None:
            self.create_index(table.key)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        #Can you return an array so that I can implement it into my Query
        if self.indices[column] is None:
            return []

        return list(self.indices[column].get(value, set()))

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
