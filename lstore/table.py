from lstore.index import Index
from lstore.page import Page
from time import time

import threading
import queue

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
        self.base_tps: list[int] = []  # Tail Page Sequence Number --> list for TPS per base page set
        self.merge_queued = False;
        self.merge_inprogess = False;
        self.updates_postmerge = 0;

        self._add_base_pageset()
        self._add_tail_pageset()

    def _add_base_pageset(self):
        self.base_pages.append([Page() for _ in range(self.total_columns)])
        self.base_tps.append(INVALID_RID)

    def _add_tail_pageset(self):
        self.tail_pages.append([Page() for _ in range(self.total_columns)])

    # are we using this? take it out? 
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

        # merging stuff
        self.tailtobase_merge: dict[int, int] = {}    # this is for tail RID to base RID linking 
        self.table_lock = threading.RLock()
        self.merge_queue = queue.Queue()
        self.stop_merge = threading.Event()
        self.merge_thread = None
        self.merge_constant = 32 # ?? just how often it merges... wasn't sure what to put here 
        #self.start_merge_thread()


    # RID helpers 
    def _current_range(self) -> PageRange:
        if self.page_ranges[-1].base_full():
            self.page_ranges.append(PageRange(self.num_columns))
        return self.page_ranges[-1]

    
    def get_RID(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid;

    def get_base_range(self, base_rid):
        if base_rid not in self.page_directory:
            raise KeyError(f"{base_rid} not found in the page dir")
        
        range_id, is_tail, pageset_id, slot = self.page_directory[base_rid]
        if is_tail:
            raise ValueError(f"{base_rid} is wrong. Tail RID found.")

        return range_id


    # Merge helpers 

    def start_merge_thread(self):
        if self.merge_thread is not None and self.merge_thread.is_alive() == True:
            return
        
        self.merge_thread = threading.Thread(target = self.merge_loop, daemon = True)
        self.merge_thread.start()

    def stop_merge_thread(self):
        self.stop_merge.set()
        self.merge_queue.put(None)

        if self.merge_thread is not None:
            self.merge_thread.join(timeout = 1.0)
    
    def merge_loop(self):
        while not self.stop_merge.is_set():
            try:
                temp = self.merge_queue.get(timeout = 1.0)
            except queue.Empty: 
                continue
        
            if temp is None:
                break
            # if temp is None:
            #     try: 
            #         self.merge_queue.task_done()
            #     except Exception:
            #         pass
            #     continue

            range_id = temp

            try:
                self.__merge(range_id)
            except Exception:
                with self.table_lock:
                    if 0 <= range_id < len(self.page_ranges):
                        pr = self.page_ranges[range_id]
                        pr.merge_inprogess = False
                        pr.merge_queued = False

            try: 
                self.merge_queue.task_done()
            except Exception: 
                pass 

    def schedule_merge(self, range_id : int):
        with self.table_lock:
            pr = self.page_ranges[range_id]
            pr.updates_postmerge += 1

            if pr.merge_inprogess == True or pr.merge_queued == True:
                return 

            if pr.updates_postmerge >= self.merge_constant:
                pr.merge_queued = True;
                self.merge_queue.put(range_id)

    def latest_read_rid(self, base_rid : int):
        latest_tail_rid = self.base_indirection.get(base_rid, INVALID_RID)
        if latest_tail_rid == INVALID_RID:
            return base_rid

        range_id, is_tail, base_pageset, slot = self.page_directory[base_rid]
        
        tps = self.page_ranges[range_id].base_tps[base_pageset]

        if latest_tail_rid <= tps:
            return base_rid
        
        return latest_tail_rid
        

    # records functions

    def write_record(self, is_tail: bool, values: list[int], target_range_id: int = None):

        if is_tail:
            if target_range_id is None: 
                raise ValueError("no target range specified")
            if not ( 0 <= target_range_id < len(self.page_ranges)):
                raise IndexError("invalid target range")

            # add record to target tail range corrleated with specific base range
            range_id = target_range_id
            pr = self.page_ranges[range_id]
            pageset_id, slot = pr.write_tail(values)

        else:
            # check if current range is full, open new one
            #if pr.base_full():
            #    self.page_ranges.append(PageRange(self.num_columns)) --> already being checked in current_range

            pr = self._current_range()
            range_id = len(self.page_ranges) - 1
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
        rid = self.latest_read_rid(base_rid)
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

        base_range_id = self.get_base_range(base_rid)
            
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
        pages_id, index = self.write_record(is_tail = True, values = values, target_range_id = base_range_id)

        with self.table_lock:
            self.tailtobase_merge[tail_rid] = base_rid

            self.base_indirection[base_rid] = tail_rid
            self.base_schema[base_rid] = new_schema

        self.schedule_merge(base_range_id)

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

        rid = self.latest_read_rid(base_rid)

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


    # now adding the actual merge functionality 
    # - I chose a granularity where the merge is happening at the page range level 
    # - also deleted records should just be skipped during merge and not physically delted 

    # merge the lastest values of the record from the tail page so we implemnet in reverse
    def tail_merge_reverse(self, tail_pagesets):
        result = []

        for page in range(len(tail_pagesets) - 1, -1, -1):
            count = tail_pagesets[page][0].num_records

            for slot in range(count - 1, -1, -1):
                tail_rid = tail_pagesets[page][RID_COLUMN].read(slot)
                result.append((tail_rid, page, slot))

        return result


    def __merge(self, range_id : int):
        print("merge is happening")


        # part 1 where everythings being copied under lock 
        with self.table_lock:

            pr = self.page_ranges[range_id]
            if pr.merge_inprogess:
                return

            pr.merge_inprogess = True
            pr.merge_queued = False

            basepages_clone = []
            for pageset in pr.base_pages:
                temp = []
                for page in pageset:
                    temp.append(page.merge_clone())
                basepages_clone.append(temp)

            tailpages_clone = []
            for pageset in pr.tail_pages:
                temp = []
                for page in pageset:
                    temp.append(page.merge_clone())
                tailpages_clone.append(temp)

            tps_copy = pr.base_tps.copy()

            page_dir_copy = {}
            for rid, loc in self.page_directory.items():
                if loc[0] == range_id:
                    page_dir_copy[rid] = loc

            tailtobase_merge_copy = {}
            for rid, loc in page_dir_copy.items():
                loc_range, is_tail, pageset_id, slot = loc
                if is_tail and rid in self.tailtobase_merge:
                    tailtobase_merge_copy[rid] = self.tailtobase_merge[rid]

            deleted = set(self.deleted)


        # part 2 where the actual merge is happening 

        merged_pages = basepages_clone
        new_base_tps = tps_copy.copy()

        seen_base_rids = set()

        max_tail = {}

        for tail_rid, tail_pageset, tail_slot in self.tail_merge_reverse(tailpages_clone):
            
            if tail_rid not in tailtobase_merge_copy:
                continue

            base_rid = tailtobase_merge_copy[tail_rid]

            if base_rid in deleted: 
                continue 

            if base_rid not in page_dir_copy:
                continue

            base_range, is_tailbase, base_pageset, base_slot = page_dir_copy[base_rid]
            
            old_tps = tps_copy[base_pageset]

            prev_max = max_tail.get(base_pageset, old_tps)
            if tail_rid > prev_max:
                max_tail[base_pageset] = tail_rid

            if base_rid in seen_base_rids:
                continue
            seen_base_rids.add(base_rid)

            for c in range(self.num_columns):
                val = tailpages_clone[tail_pageset][4 + c].read(tail_slot)
                merged_pages[base_pageset][4 + c].merge_write(base_slot, val)

            
        for base_pageset, new_tps in max_tail.items():
            if new_tps > new_base_tps[base_pageset]:
                new_base_tps[base_pageset] = new_tps

        
        # part 3 where we change page dir pointer to new merged pages with lock 

        with self.table_lock:
            if not (0 <= range_id < len(self.page_ranges)):
                return

            pr = self.page_ranges[range_id]

            pr.base_pages = merged_pages

            for i in range(len(pr.base_tps)):
                if i < len(new_base_tps) and new_base_tps[i] > pr.base_tps[i]:
                    pr.base_tps[i] = new_base_tps[i]

            pr.merge_inprogess = False
            pr.merge_queued = False
            pr.updates_postmerge = 0


    #Iterates through Previous RID until desired
    def get_previous_rid(self, base_rid: int, version: int):
        rid = self.latest_rid(base_rid)

        for _ in range(version):
            prev_rid = self.read_val(rid, INDIRECTION_COLUMN)
            if prev_rid == INVALID_RID:
                return base_rid
            rid = prev_rid
        return rid
