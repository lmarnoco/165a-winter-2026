from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

INVALID_RID = 0

MAX_BASE_PAGES_PER_RANGE = 16

class PageRange:
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.total_columns = 4 + num_columns
        self.base_pages: list[list[Page]] = []   # list of pagesets
        self.tail_pages: list[list[Page]] = []   # list of pagesets
        self.tps = 0  # Tail Page Sequence Number

        self._add_base_pageset()
        self._add_tail_pageset()

    def _add_base_pageset(self):
        self.base_pages.append([Page() for _ in range(self.total_columns)])

    def _add_tail_pageset(self):
        self.tail_pages.append([Page() for _ in range(self.total_columns)])

    def has_base_capacity(self):
        if len(self.base_pages) < MAX_BASE_PAGES_PER_RANGE:
            return True  
        return self.base_pages[-1][0].has_capacity()  # last pageset has room

    def base_full(self):
        return (len(self.base_pages) >= MAX_BASE_PAGES_PER_RANGE and 
                not self.base_pages[-1][0].has_capacity())

    def tail_capacity(self):
        return self.tail_pages[-1][0].has_capacity()

    def write_base(self, values: list[int]):
        if not self.base_pages[-1][0].has_capacity():
            if len(self.base_pages) >= MAX_BASE_PAGES_PER_RANGE:
                raise RuntimeError("Page range full")
            self._add_base_pageset()
        
        pageset_id = len(self.base_pages) - 1
        pageset = self.base_pages[pageset_id]
        slot = pageset[0].num_records
        for col_id, val in enumerate(values):
            pageset[col_id].write(int(val))
        return pageset_id, slot

    def write_tail(self, values: list[int]):
        if not self.tail_capacity():
            self._add_tail_pageset()
        
        pageset_id = len(self.tail_pages) - 1
        pageset = self.tail_pages[pageset_id]
        slot = pageset[0].num_records
        for col_id, val in enumerate(values):
            pageset[col_id].write(int(val))
        return pageset_id, slot

    def read(self, is_tail: bool, pageset_id: int, slot: int, col_id: int):
        pages = self.tail_pages if is_tail else self.base_pages
        return pages[pageset_id][col_id].read(slot)

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory: dict[int, tuple[int, bool, int, int]] = {}
        self.index = Index(self)
        self.page_ranges: list[PageRange] = []
        self.total_columns = 4 + num_columns
        self.base_indirection: dict[int, int] = {}
        self.base_schema: dict[int, int] = {}
        self.deleted: set[int] = set()
        self.next_rid = 1                          
        self.page_ranges.append(PageRange(num_columns))

    # New pages and capacity

    def new_pages(self, is_tail: bool):
        pageset = [Page() for _ in range(self.total_columns)]
        if is_tail:
            self.tail_pages.append(pageset)
        else:
            self.base_pages.append(pageset)

    def base_capacity(self):
        if not self.base_pages:
            return False
        else:
            return self.base_pages[-1][0].has_capacity()

    def tail_capacity(self):
        if not self.tail_pages:
            return False
        else:
            return self.tail_pages[-1][0].has_capacity() 


    # RID helpers 
    def _current_range(self) -> PageRange:
        if self.page_ranges[-1].base_full():
            self.page_ranges.append(PageRange(self.num_columns))
        return self.page_ranges[-1]

    
    def get_RID(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid;


    # records functions

    def write_record(self, is_tail: bool, values: list[int]):

        range_id = len(self.page_ranges) - 1
        pr = self.page_ranges[range_id]

        if is_tail:
            pageset_id, slot = pr.write_tail(values)
        else:
            # check if current range is full, open new one
            if pr.base_full():
                self.page_ranges.append(PageRange(self.num_columns))
                range_id = len(self.page_ranges) - 1
                pr = self.page_ranges[range_id]
            pageset_id, slot = pr.write_base(values)

        rid = values[RID_COLUMN]
        self.page_directory[rid] = (range_id, is_tail, pageset_id, slot)
        return pageset_id, slot


    def read_val(self, rid: int, col_id: int):
        range_id, is_tail, pageset_id, slot = self.page_directory[rid]
        return self.page_ranges[range_id].read(is_tail, pageset_id, slot, col_id)

    # metada and stuff 

    def latest_rid(self, base_rid: int):
        tail_rid = self.base_indirection.get(base_rid, INVALID_RID)
        latest_rid = tail_rid if tail_rid != INVALID_RID else base_rid
        return latest_rid


    def latest_cols(self, base_rid: int):
        rid = self.latest_rid(base_rid)
        cols = [self.read_val(rid, 4 + c) for c in range(self.num_columns)]
        return cols

    def get_schemaenc(self, rid: int):
        return self.read_val(rid, SCHEMA_ENCODING_COLUMN)

    # more records functions 

    def insert_base_record(self, cols: list[int]):
        
        if len(cols) != self.num_columns:
            raise ValueError("wrong no. of columns")

        key_val = cols[self.key]
        if len(self.index.locate(self.key, key_val)) > 0:
            raise ValueError("there's a duplicate primary key?")

        base_rid = self.get_RID()

        curr_time = int(time())

        values = [0] * self.total_columns
        values[INDIRECTION_COLUMN] = INVALID_RID
        values[RID_COLUMN] = base_rid
        values[TIMESTAMP_COLUMN] = curr_time
        values[SCHEMA_ENCODING_COLUMN] = 0
        for i, x in enumerate(cols):
            values[4 + i] = int(x)
        
        pages_id, index = self.write_record(is_tail = False, values = values)

        self.base_indirection[base_rid] = INVALID_RID
        self.base_schema[base_rid] = 0  #not sure how to handle this 

        self.index.add_entry(self.key, base_rid, key_val)
        
        for c in range(self.num_columns):
            if c == self.key: # skip since self.key has already been inserted above
                continue
            self.index.add_entry(c, base_rid, values[4 + c])

        return base_rid



    def update_tail_record(self, base_rid: int, update_cols: dict[int, int]):
        if base_rid not in self.page_directory:
            raise KeyError("record not found")
        if base_rid in self.deleted:
            raise KeyError("record has been deleted")

        if self.key in update_cols: #double checking
            update_cols = {k: v for k, v in update_cols.items() if k != self.key}
    
        # If nothing left to update after removing primary key
        if not update_cols:
            return base_rid  # Return without doing anything
            
        tail_rid = self.get_RID()
        last_tail_rid = self.base_indirection.get(base_rid, INVALID_RID)
        if last_tail_rid == INVALID_RID:
            prev_rid = base_rid
        else:
            prev_rid = last_tail_rid

        curr_vals = self.latest_cols(base_rid)
        new_vals = curr_vals.copy()     
        for col_ids, val in update_cols.items():
            #if val is not None: #Delete
                new_vals[col_ids] = val

        curr_schema = self.base_schema.get(base_rid, 0)
        new_schema = curr_schema
        for col_ids, val in update_cols.items(): #Change
            #if val is not None:
                new_schema |= (1 << col_ids)

        curr_time = int(time())

        values = [0] * self.total_columns
        values[INDIRECTION_COLUMN] = prev_rid  
        values[RID_COLUMN] = tail_rid
        values[TIMESTAMP_COLUMN] = curr_time
        values[SCHEMA_ENCODING_COLUMN] = new_schema
        values[4:] = new_vals

        # while not self.tail_capacity():
        #     self.new_pages(is_tail = True) #Delete
        pages_id, index = self.write_record(is_tail = True, values = values)

        self.base_indirection[base_rid] = tail_rid
        self.base_schema[base_rid] = new_schema

        for col_ids, val in update_cols.items():
            if col_ids == self.key:
                continue
            old_val = curr_vals[col_ids]
            self.index.update_entry(col_ids, base_rid, old_val, val)
            curr_vals[col_ids] = val

        return tail_rid
        

    def read_record(self, base_rid: int, projected_cols: list[int]):
        
        if base_rid not in self.page_directory:
            return  None
        if base_rid in self.deleted:
            return None

        rid = self.latest_rid(base_rid)

        cols = [None] * self.num_columns
        for c in range(self.num_columns):
            if projected_cols[c] == 1: 
                if c == self.key: #added
                    cols[c] = self.read_val(base_rid, 4 + c) #added
                else: #added
                    cols[c] = self.read_val(rid, 4 + c)

        key_val = self.read_val(rid, 4 + self.key)

        return Record(base_rid, key_val, cols) # need base_rid returned for update? 
        

    def delete(self, base_rid: int):
        
        if base_rid not in self.page_directory:
            return False
        if base_rid in self.deleted:
            return False

        latest = self.latest_cols(base_rid)
        for c in range(self.num_columns):
            self.index.remove_entry(c, base_rid, latest[c])
        
        self.deleted.add(base_rid)

        return True;    


    # merge is milestone 2? 

    def __merge(self):
        print("merge is happening")
        pass
 

    #Iterates through Previous RID until desired
    def get_previous_rid(self, base_rid: int, version: int):
        rid = self.latest_rid(base_rid)

        for _ in range(version):
            prev_rid = self.read_val(rid, INDIRECTION_COLUMN)
            if prev_rid == INVALID_RID:
                return base_rid
            rid = prev_rid
        return rid
