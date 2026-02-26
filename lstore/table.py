from lstore.index import Index
from lstore.page import Page
from time import time

import threading
import queue
import os
import struct

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
        self.merge_queued = False
        self.merge_inprogress = False
        self.updates_postmerge = 0

        # Structure tracking only — no Page objects
        self.num_base_pagesets = 0
        self.base_pageset_counts: list[int] = []   # num_records per base pageset
        self.num_tail_pagesets = 0
        self.tail_pageset_counts: list[int] = []   # num_records per tail pageset
        self.base_tps: list[int] = []

        self._add_base_pageset()
        self._add_tail_pageset()

    def _add_base_pageset(self):
        self.num_base_pagesets += 1
        self.base_pageset_counts.append(0)
        self.base_tps.append(INVALID_RID)

    def _add_tail_pageset(self):
        self.num_tail_pagesets += 1
        self.tail_pageset_counts.append(0)

    def base_full(self):
        if self.num_base_pagesets < MAX_BASE_PAGES_PER_RANGE:
            return False
        return self.base_pageset_counts[-1] >= 512  # 4096/8

    def has_base_capacity(self):
        return not self.base_full()

    def tail_capacity(self):
        return self.tail_pageset_counts[-1] < 512

    def alloc_base_slot(self):
        """Allocate a slot in the current base pageset. Returns (pageset_id, slot)."""
        if self.base_pageset_counts[-1] >= 512:
            if self.num_base_pagesets >= MAX_BASE_PAGES_PER_RANGE:
                raise RuntimeError("Page range full")
            self._add_base_pageset()
        pageset_id = self.num_base_pagesets - 1
        slot = self.base_pageset_counts[pageset_id]
        self.base_pageset_counts[pageset_id] += 1
        return pageset_id, slot

    def alloc_tail_slot(self):
        """Allocate a slot in the current tail pageset. Returns (pageset_id, slot)."""
        if self.tail_pageset_counts[-1] >= 512:
            self._add_tail_pageset()
        pageset_id = self.num_tail_pagesets - 1
        slot = self.tail_pageset_counts[pageset_id]
        self.tail_pageset_counts[pageset_id] += 1
        return pageset_id, slot

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
        self.merge_constant = 10 # ?? just how often it merges... wasn't sure what to put here 
        self.bufferpool = None  # injected by Database
        self.start_merge_thread()


    def save_to_disk(self, db_path: str):
    
        table_dir = os.path.join(db_path, self.name)
        os.makedirs(table_dir, exist_ok=True)

        self._save_pages(table_dir)
        self._save_metadata(table_dir)

    def load_from_disk(self, db_path: str):
       
        table_dir = os.path.join(db_path, self.name)
        if not os.path.isdir(table_dir):
            return  # brand-new table, nothing to load

        self._load_pages(table_dir)
        self._load_metadata(table_dir)
    
    def _pid(self, range_id: int, is_tail: bool, pageset_id: int, col_id: int) -> str:
        side = 'tail' if is_tail else 'base'
        return f"{self.name}_{range_id}_{side}_{pageset_id}_{col_id}"

    # ---- page I/O --------------------------------------------------------

    def _save_pages(self, table_dir: str):
        # Bufferpool writes dirty pages to disk at the bufferpool's path.
        # Since bufferpool.path == db_path and page files are named
        # tablename_range_side_pageset_col.pg, they land in the right place.
        #os.makedirs(table_dir, exist_ok=True)
        #if self.bufferpool is not None:
            #self.bufferpool.flush_all()
        pass

    def _load_pages(self, table_dir: str):
        if not os.path.isdir(table_dir):
            return

        base_shape: dict[int, int] = {}
        tail_shape: dict[int, int] = {}
        base_counts: dict[tuple, int] = {}   # (range_id, pageset_id) -> num_records
        tail_counts: dict[tuple, int] = {}
        max_range = -1

        for fname in os.listdir(table_dir):
            if not fname.endswith('.pg'):
                continue
            parts = fname[:-3].split('_')
            if len(parts) != 4:
                continue
            try:
                r, side, ps, c = int(parts[0]), parts[1], int(parts[2]), int(parts[3])
            except ValueError:
                continue
            max_range = max(max_range, r)
            if c == 0:  # only need one column to get num_records
                path = os.path.join(table_dir, fname)
                with open(path, 'rb') as f:
                    num_records = struct.unpack('<q', f.read(8))[0]
                if side == 'base':
                    base_shape[r] = max(base_shape.get(r, -1), ps)
                    base_counts[(r, ps)] = num_records
                else:
                    tail_shape[r] = max(tail_shape.get(r, -1), ps)
                    tail_counts[(r, ps)] = num_records

        if max_range < 0:
            return

        # Rebuild PageRange structure skeletons (no Page objects)
        self.page_ranges = []
        for r in range(max_range + 1):
            pr = PageRange.__new__(PageRange)
            pr.num_columns = self.num_columns
            pr.total_columns = self.total_columns
            pr.merge_queued = False
            pr.merge_inprogress = False
            pr.updates_postmerge = 0
            pr.base_tps = []

            num_base = base_shape.get(r, -1) + 1
            pr.num_base_pagesets = num_base
            pr.base_pageset_counts = [base_counts.get((r, ps), 0) for ps in range(num_base)]
            pr.base_tps = [INVALID_RID] * num_base

            num_tail = tail_shape.get(r, -1) + 1
            pr.num_tail_pagesets = num_tail
            pr.tail_pageset_counts = [tail_counts.get((r, ps), 0) for ps in range(num_tail)]

            self.page_ranges.append(pr)

    @staticmethod
    def _page_path(table_dir: str, range_id: int, is_tail: bool,
                   pageset_id: int, col_id: int) -> str:
        side = 'tail' if is_tail else 'base'
        return os.path.join(table_dir, f"{range_id}_{side}_{pageset_id}_{col_id}.pg")

    @staticmethod
    def _write_page(path: str, page: Page):
        with open(path, 'wb') as f:
            f.write(struct.pack('<q', page.num_records))
            f.write(bytes(page.data))

    @staticmethod
    def _read_page(path: str) -> Page:
        page = Page()
        with open(path, 'rb') as f:
            page.num_records = struct.unpack('<q', f.read(8))[0]
            page.data = bytearray(f.read(4096))
        return page



    # ---- metadata I/O ----------------------------------------------------

    def _save_metadata(self, table_dir: str):
        path = os.path.join(table_dir, 'table.meta')
        with open(path, 'wb') as f:
            # next_rid
            f.write(struct.pack('<q', self.next_rid))

            # page_directory  rid -> (range_id, is_tail, pageset_id, slot)
            f.write(struct.pack('<q', len(self.page_directory)))
            for rid, (range_id, is_tail, pageset_id, slot) in self.page_directory.items():
                f.write(struct.pack('<qqqqq', rid, range_id,
                                    1 if is_tail else 0, pageset_id, slot))

            # base_indirection  rid -> tail_rid
            f.write(struct.pack('<q', len(self.base_indirection)))
            for rid, tail_rid in self.base_indirection.items():
                f.write(struct.pack('<qq', rid, tail_rid))

            # base_schema  rid -> schema_int
            f.write(struct.pack('<q', len(self.base_schema)))
            for rid, schema in self.base_schema.items():
                f.write(struct.pack('<qq', rid, schema))

            # deleted set
            f.write(struct.pack('<q', len(self.deleted)))
            for rid in self.deleted:
                f.write(struct.pack('<q', rid))

            # tailtobase_merge  tail_rid -> base_rid
            f.write(struct.pack('<q', len(self.tailtobase_merge)))
            for tail_rid, base_rid in self.tailtobase_merge.items():
                f.write(struct.pack('<qq', tail_rid, base_rid))

            # base_tps per page range
            f.write(struct.pack('<q', len(self.page_ranges)))
            for pr in self.page_ranges:
                f.write(struct.pack('<q', len(pr.base_tps)))
                for tps in pr.base_tps:
                    f.write(struct.pack('<q', tps))

    def _load_metadata(self, table_dir: str):
        path = os.path.join(table_dir, 'table.meta')
        if not os.path.exists(path):
            return

        with open(path, 'rb') as f:
            def rd(fmt):
                return struct.unpack(fmt, f.read(struct.calcsize(fmt)))

            self.next_rid = rd('<q')[0]

            n = rd('<q')[0]
            for _ in range(n):
                rid, range_id, is_tail_int, pageset_id, slot = rd('<qqqqq')
                self.page_directory[rid] = (range_id, bool(is_tail_int), pageset_id, slot)

            n = rd('<q')[0]
            for _ in range(n):
                rid, tail_rid = rd('<qq')
                self.base_indirection[rid] = tail_rid

            n = rd('<q')[0]
            for _ in range(n):
                rid, schema = rd('<qq')
                self.base_schema[rid] = schema

            n = rd('<q')[0]
            for _ in range(n):
                self.deleted.add(rd('<q')[0])

            n = rd('<q')[0]
            for _ in range(n):
                tail_rid, base_rid = rd('<qq')
                self.tailtobase_merge[tail_rid] = base_rid

            # restore base_tps into the already-loaded page_ranges
            num_ranges = rd('<q')[0]
            for r in range(num_ranges):
                num_tps = rd('<q')[0]
                tps_list = [rd('<q')[0] for _ in range(num_tps)]
                if r < len(self.page_ranges):
                    self.page_ranges[r].base_tps = tps_list


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
                try: 
                    self.merge_queue.task_done()
                except Exception:
                    pass
                continue

            range_id = temp

            try:
                self.__merge(range_id)
            except Exception:
                with self.table_lock:
                    if 0 <= range_id < len(self.page_ranges):
                        pr = self.page_ranges[range_id]
                        pr.merge_inprogress = False
                        pr.merge_queued = False

            try: 
                self.merge_queue.task_done()
            except Exception: 
                pass 

    def schedule_merge(self, range_id : int):
        with self.table_lock:
            pr = self.page_ranges[range_id]
            pr.updates_postmerge += 1

            if pr.merge_inprogress or pr.merge_queued:
                return

            if pr.updates_postmerge >= self.merge_constant:
                # only schedule if bufferpool has enough headroom
                if self.bufferpool is not None and len(self.bufferpool.frames) < self.bufferpool.capacity // 2:
                    pr.merge_queued = True
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
            range_id = target_range_id
            pr = self.page_ranges[range_id]
            pageset_id, slot = pr.alloc_tail_slot()
        else:
            pr = self._current_range()
            range_id = len(self.page_ranges) - 1
            pageset_id, slot = pr.alloc_base_slot()

        rid = values[RID_COLUMN]
        self.page_directory[rid] = (range_id, is_tail, pageset_id, slot)

        # Write each column through bufferpool
        for col_id, val in enumerate(values):
            pid = self._pid(range_id, is_tail, pageset_id, col_id)
            page = self.bufferpool.get_page(pid)
            # write into the correct slot
            offset = slot * 8
            page.data[offset:offset+8] = int(val).to_bytes(8, byteorder='little', signed=True)
            # keep num_records in sync
            if slot >= page.num_records:
                page.num_records = slot + 1
            self.bufferpool.mark_dirty(pid)
            self.bufferpool.unpin(pid)

        return pageset_id, slot

    def read_val(self, rid: int, col_id: int):
        range_id, is_tail, pageset_id, slot = self.page_directory[rid]
        pid = self._pid(range_id, is_tail, pageset_id, col_id)
        page = self.bufferpool.get_page(pid)
        #print(f"read_val: rid={rid} is_tail={is_tail} pid={pid} slot={slot} num_records={page.num_records}")
        val = page.read(slot)
        self.bufferpool.unpin(pid)
        return val

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

        with self.table_lock:
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

        key_val = self.read_val(base_rid, 4 + self.key)

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
        # part 1 where everythings being copied under lock 
        with self.table_lock:
            pr = self.page_ranges[range_id]
            if pr.merge_inprogress:
                return
            pr.merge_inprogress = True
            pr.merge_queued = False

            # Clone base pages by reading from bufferpool
            basepages_clone = []
            for ps_id in range(pr.num_base_pagesets):
                pageset = []
                for col_id in range(self.total_columns):
                    pid = self._pid(range_id, False, ps_id, col_id)
                    orig = self.bufferpool.get_page(pid)
                    pageset.append(orig.merge_clone())
                    self.bufferpool.unpin(pid)
                basepages_clone.append(pageset)

            # Clone tail pages
            tailpages_clone = []
            for ps_id in range(pr.num_tail_pagesets):
                pageset = []
                for col_id in range(self.total_columns):
                    pid = self._pid(range_id, True, ps_id, col_id)
                    orig = self.bufferpool.get_page(pid)
                    pageset.append(orig.merge_clone())
                    self.bufferpool.unpin(pid)
                tailpages_clone.append(pageset)

            tps_copy = pr.base_tps.copy()
            page_dir_copy = {}
            for rid, loc in self.page_directory.items():
                if loc[0] == range_id:
                    page_dir_copy[rid] = loc

            tailtobase_merge_copy = {}
            for rid, loc in page_dir_copy.items():
                _, is_tail, _, _ = loc
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

            _, _, base_pageset, base_slot = page_dir_copy[base_rid]
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


        with self.table_lock:
            if not (0 <= range_id < len(self.page_ranges)):
                return
            pr = self.page_ranges[range_id]

            for ps_id, pageset in enumerate(merged_pages):
                for col_id, page in enumerate(pageset):
                    pid = self._pid(range_id, False, ps_id, col_id)
                    bp_page = self.bufferpool.get_page(pid)
                    bp_page.data[:] = page.data
                    bp_page.num_records = page.num_records
                    self.bufferpool.mark_dirty(pid)
                    self.bufferpool.unpin(pid)
                pr.base_pageset_counts[ps_id] = pageset[0].num_records

            for i in range(len(pr.base_tps)):
                if i < len(new_base_tps) and new_base_tps[i] > pr.base_tps[i]:
                    pr.base_tps[i] = new_base_tps[i]

            pr.merge_inprogress = False
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
    
    