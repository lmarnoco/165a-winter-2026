from lstore.table import Table, Record, INDIRECTION_COLUMN, INVALID_RID
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key, transaction=None):
        self.table.publish_merge()
        
        locations = self.table.index.locate(self.table.key, primary_key)
        success = False

        #Add Lock Check  later
        if len(locations) <= 0:
            return False 
        
        
        for RID in locations:
            if self.table.delete(RID):
                success = True
        
        return success


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns, transaction=None):
        self.table.publish_merge()

        key_val = columns[self.table.key]
        #If insert_base_record fails 
        if len(self.table.index.locate(self.table.key, key_val)) > 0:
            return False
        
        try: 
            self.table.insert_base_record(columns)
            return True
        except Exception:
            return False
            
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index, transaction=None):
        self.table.publish_merge()
        
        if self.table.index.indices[search_key_index] is not None:
            rids_list = self.table.index.locate(search_key_index, search_key)
        else:
            rids_list = []
            for rid, meta in self.table.page_directory.items():
                is_tail = meta[1]
                if is_tail or rid in self.table.deleted:
                    continue

                latest_val = self.table.latest_cols(rid)[search_key_index]
                if latest_val == search_key:
                    rids_list.append(rid)
        
                    
        if not rids_list:
            return []

        #Check if None Maybe? (Will add if need be) --> added this to see if it helps

        result = []
        seen_list = set()
        for rid in rids_list:
            if rid in seen_list:
                continue
            seen_list.add(rid)

            record = self.table.read_record(rid, projected_columns_index)
            if record is not None:
                result.append(record)
        
        return result
    
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction=None):
        self.table.publish_merge()

        if relative_version <= 0: 
            steps_back = -relative_version 
        else:
            steps_back = relative_version

        if search_key_index == self.table.key and self.table.index.indices[search_key_index] is not None:
            rids_list = self.table.index.locate(search_key_index, search_key)
        else:
            rids_list = []
            for rid, meta in self.table.page_directory.items():
                is_tail = meta[1]
                if is_tail or rid in self.table.deleted:
                    continue

                rids_list.append(rid)

        result = []
        seen = set()

        #Iterates through all locations appending to list
        # Saee - I think this loop logic is right? but like Tharun the storage part is missing 
        # saee again - chnaged this entire logic up again a bit... tests were failing again.. 
        for base_rid in rids_list:
            if base_rid in seen:
                continue

            seen.add(base_rid)

            if base_rid in self.table.deleted:
                continue

            past_rid = self.table.get_previous_rid(base_rid, steps_back)

            if search_key_index == self.table.key:
                version_val = self.table.read_val(base_rid, 4 + search_key_index)
            else:
                version_val = self.table.read_val(past_rid, 4 + search_key_index)

            if version_val != search_key:
                continue

            cols = [None] * self.table.num_columns
            for c in range(self.table.num_columns):
                if projected_columns_index[c] != 1:
                    continue

                if c == self.table.key:
                    cols[c] = self.table.read_val(base_rid, 4 + c)
                else:
                    cols[c] = self.table.read_val(past_rid, 4 + c)

            key_val = self.table.read_val(base_rid, 4 + self.table.key) 
            result.append(Record(base_rid, key_val, cols))  # added this from the below logic to the OG loop 

        return result

        

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns, transaction=None):
        self.table.publish_merge()

        if columns[self.table.key] is not None and columns[self.table.key] != primary_key:
            # Trying to change the primary key - not allowed
            return False
        

        rids_list = self.table.index.locate(self.table.key, primary_key)

        #Or Locked during something
        if len(rids_list) <= 0:
            return False

        update_columns =  {i: val for i, val in enumerate(columns) if val is not None and i != self.table.key}

        if not update_columns:
            return True
        
        #Expect 1
        rid = rids_list[0]

        try:  # put the action in a try except so that if table returns error it doesn't crash? 
            self.table.update_tail_record(rid, update_columns)
            return True
        except Exception:
            return False

 

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index, transaction=None):
        self.table.publish_merge()
        
        og_rids = self.table.index.locate_range(start_range, end_range, self.table.key)

        rids_list = []

        for rid_or_list in og_rids:
            if isinstance(rid_or_list, list):
                rids_list.extend(rid_or_list)
            else:
                rids_list.append(rid_or_list)

        rids_list = [rid for rid in rids_list if rid not in self.table.deleted]

        if len(rids_list) == 0:
            return False

        #Could change later
        result = 0
        for rids in rids_list:
            latest_rid = self.table.latest_cols(rids)
            result += latest_rid[aggregate_column_index]

        return result

                

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction=None):
        self.table.publish_merge()


        og_rids = self.table.index.locate_range(start_range, end_range, self.table.key)

        rids_list = []

        for rid_or_list in og_rids:
            if isinstance(rid_or_list, list):
                rids_list.extend(rid_or_list)
            else:
                rids_list.append(rid_or_list)

        rids_list = [rid for rid in rids_list if rid not in self.table.deleted]

        result = 0
        
        if len(rids_list) == 0:
            return False
        
        for rids in rids_list:

            if relative_version <= 0:
                steps_back = -relative_version
            else:
                steps_back = relative_version

            past_rids = self.table.get_previous_rid(rids, steps_back)
            val = self.table.read_val(past_rids, 4 + aggregate_column_index)
            result += val
        
        return result
        

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column, transaction=None):
        self.table.publish_merge()

        result = self.select(key, self.table.key, [1] * self.table.num_columns)
        if result is False or not result:
            return False

        r = result[0]
        updated_columns = [None] * self.table.num_columns
        updated_columns[column] = r.columns[column] + 1

        result = self.update(key, *updated_columns)
        return result
