from lstore.index import Index
from lstore.page import Page
from time import time, sleep

import threading
import queue
import os
import struct

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

INVALID_RID = 0
DELETED_INDIRECTION = -1

MAX_BASE_PAGES_PER_RANGE = 16

class PageRange:
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.total_columns = 4 + num_columns
        self.merge_queued = False
        self.merge_inprogress = False
        self.merge_ready = False
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
        # added guard here --> have at least one tail pageset if none of them load in
        if not self.tail_pageset_counts:
            self._add_tail_pageset()
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
    def __init__(self, name, num_columns, key, merge_threshold: int = 10):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory: dict[int, tuple[int, bool, int, int]] = {}
        self.page_ranges: list[PageRange] = []
        self.total_columns = 4 + num_columns
        self.base_indirection: dict[int, int] = {}
        self.base_schema: dict[int, int] = {}
        self.deleted: set[int] = set()
        self.next_rid = 1                          
        self.page_ranges.append(PageRange(num_columns))

        self.table_lock = threading.RLock()

        self.index = Index(self)

        # merging stuff
        self.tailtobase_merge: dict[int, int] = {}    # this is for tail RID to base RID linking 
        self.merge_queue = queue.Queue()
        self.stop_merge = threading.Event()
        self.merge_thread = None
        self.merge_constant = merge_threshold
        self.pending_merges = {}  # range_id -> (merged_pages, new_base_tps, covered_updates)
        self.bufferpool = None  # injected by Database
        self.start_merge_thread()

        # milestone 3 transactions stuff 
        self.active_transactions = 0


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
        self.publish_merge(wait = False)

        if self.bufferpool is not None:
            self.bufferpool.flush_all()

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
            pr.merge_ready = False
            pr.updates_postmerge = 0
            pr.base_tps = []

            num_base = max(1, base_shape.get(r, -1) + 1)
            pr.num_base_pagesets = num_base
            pr.base_pageset_counts = [base_counts.get((r, ps), 0) for ps in range(num_base)]
            pr.base_tps = [INVALID_RID] * num_base

            num_tail = max(1, tail_shape.get(r, -1) + 1)
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
        with self.table_lock:
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
                        pr.merge_ready = False

            try: 
                self.merge_queue.task_done()
            except Exception: 
                pass 

    def merge_lock_check(self, range_id: int):
        pr = self.page_ranges[range_id]

        if pr.merge_inprogress or pr.merge_queued or pr.merge_ready:
            return

        if pr.updates_postmerge < self.merge_constant:
            return

        pr.merge_queued = True
        self.merge_queue.put(range_id)

    def schedule_merge(self, range_id : int):
        with self.table_lock:
            pr = self.page_ranges[range_id]
            pr.updates_postmerge += 1

            if self.active_transactions > 0:
                return

            self.merge_lock_check(range_id)

    # added the actual contention free part? 
    # the merge does the atomic merge and swap stuff so the merge can actually point to the new base page and be published
    def publish_merge(self, wait: bool = False):
        published = False

        while True:
            publish_recent = False

            with self.table_lock:
                active = self.active_transactions > 0
                ready_ranges = sorted(self.pending_merges.keys())
                pending_work = bool(self.pending_merges) or any(
                    pr.merge_queued or pr.merge_inprogress or pr.merge_ready for pr in self.page_ranges
                )

            if active:
                if not wait:
                    return published
                sleep(0.001)
                continue

            for range_id in ready_ranges:
                with self.table_lock:
                    payload = self.pending_merges.get(range_id)
                    if payload is None or not (0 <= range_id < len(self.page_ranges)):
                        continue
                    
                    merged_pages, new_tps, covered = payload
                    pr = self.page_ranges[range_id]

                # the atomic pointer swap logic 
                batch = {}
                for ps_id, pageset in enumerate(merged_pages):
                    for col_id, page in enumerate(pageset):
                        batch[self._pid(range_id, False, ps_id, col_id)] = page

                swapped_all = self.bufferpool.install_pages_all(batch, dirty=True)

                with self.table_lock:
                    if self.pending_merges.get(range_id) is None:
                        continue

                    if not swapped_all:
                        pr.merge_ready = True
                        continue

                    self.pending_merges.pop(range_id, None)

                    for i in range(len(pr.base_tps)):
                        if i < len(new_tps) and new_tps[i] > pr.base_tps[i]:
                            pr.base_tps[i] = new_tps[i]

                    pr.merge_ready = False
                    pr.merge_inprogress = False
                    pr.merge_queued = False
                    pr.updates_postmerge = max(0, pr.updates_postmerge - covered)

                    # any new updates should get queued again here... 
                    self.merge_lock_check(range_id)

                    publish_recent = True
                    published = True

            if not wait:
                return published

            with self.table_lock:
                pending_work = bool(self.pending_merges) or any(
                    pr.merge_queued or pr.merge_inprogress or pr.merge_ready for pr in self.page_ranges
                )

            if not pending_work:
                return published

            if not publish_recent:
                sleep(0.001)


    def latest_read_rid(self, base_rid : int):
        latest_tail_rid = self.base_indirection.get(base_rid, INVALID_RID)
        
        if latest_tail_rid == INVALID_RID:
            return base_rid
        
        if latest_tail_rid == DELETED_INDIRECTION:
            return base_rid

        range_id, is_tail, base_pageset, slot = self.page_directory[base_rid]
        
        tps = self.page_ranges[range_id].base_tps[base_pageset]

        if latest_tail_rid <= tps:
            return base_rid
        
        return latest_tail_rid
        

    # milestone 3 - transactions helpers 

    def begin_transaction(self):
        with self.table_lock:
            self.active_transactions += 1

    def end_transaction(self, committed):
        with self.table_lock:
            if self.active_transactions > 0:
                self.active_transactions -= 1
            check = (self.active_transactions == 0)

        if check:
            with self.table_lock:
                for range_id in range(len(self.page_ranges)):
                    self.merge_lock_check(range_id)


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
        touched = []
        # self.page_directory[rid] = (range_id, is_tail, pageset_id, slot)

        # Write each column through bufferpool
        try: 
            for col_id, val in enumerate(values):
                pid = self._pid(range_id, is_tail, pageset_id, col_id)
                page = self.bufferpool.get_page(pid)
                touched.append((pid, page))
                # write into the correct slot
                offset = slot * 8
                page.data[offset:offset+8] = int(val).to_bytes(8, byteorder='little', signed=True)
                
                # keep num_records in sync
            for pid, page in touched:
                if slot >= page.num_records:
                    page.num_records = slot + 1
                self.bufferpool.mark_dirty(pid)

            self.page_directory[rid] = (range_id, is_tail, pageset_id, slot)
            return pageset_id, slot
        finally: 
            for pid, _ in touched:
                self.bufferpool.unpin(pid)

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
        
        if tail_rid == DELETED_INDIRECTION:
            return base_rid

        latest_rid = tail_rid if tail_rid != INVALID_RID else base_rid
        return latest_rid


    def latest_cols(self, base_rid: int):
        rid = self.latest_read_rid(base_rid)
        cols = [self.read_val(rid, 4 + c) for c in range(self.num_columns)]
        return cols

    def get_schemaenc(self, rid: int):
        return self.read_val(rid, SCHEMA_ENCODING_COLUMN)

    # more records functions 

    def insert_base_record(self, cols: list[int], transaction = None, defer_index: bool = False):
        
        if len(cols) != self.num_columns:
            raise ValueError("wrong no. of columns")

        key_val = cols[self.key]

        with self.table_lock:
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
            
            self.write_record(is_tail = False, values = values)
            self.base_indirection[base_rid] = INVALID_RID
            self.base_schema[base_rid] = 0  #not sure how to handle this 

            if transaction is not None:
                if not transaction.lock_manager.lock(transaction, base_rid, "X"):
                    if hasattr(transaction, "mark_abort"):
                        from lstore.transaction import Aborts
                        transaction.mark_abort(Aborts.LOCK_CONFLICT, f"failed to lock new RID {base_rid}")
                    self.deleted.add(base_rid)
                    self.base_indirection[base_rid] = DELETED_INDIRECTION
                    self.base_schema.pop(base_rid, None)
                    raise RuntimeError("fail to lock new RID")
       
            if not defer_index:
                self.index.add_entry(self.key, base_rid, key_val)
                
                for c in range(self.num_columns):
                    if c == self.key: # skip since self.key has already been inserted above
                        continue
                    self.index.add_entry(c, base_rid, values[4 + c])

        return base_rid


    # changed this function up completely for miletsone 3 to return the full context 
    def update_tail_record(self, base_rid: int, update_cols: dict[int, int], defer_index: bool = False):
        
        if base_rid not in self.page_directory:
            raise KeyError("record not found")
        
        if base_rid in self.deleted or self.base_indirection.get(base_rid) == DELETED_INDIRECTION:
            raise KeyError("record has been deleted")

        if self.key in update_cols:
            update_cols = {k: v for k, v in update_cols.items() if k != self.key}

        if not update_cols:
            return {
                "tail_rid": None,
                "copy_rid": None,
                "base_range_id": self.get_base_range(base_rid),
                "old_tail_rid": self.base_indirection.get(base_rid, INVALID_RID),
                "old_schema": self.base_schema.get(base_rid, 0),
                "old_values": self.latest_cols(base_rid),
                "update_cols": {}
            }

        copy_rid = None
        tail_rid = None
        base_range_id = None
        old_tail_rid = INVALID_RID
        old_schema = 0
        old_values = None
        applied_index_cols = []
        merge_scheduled = False

        # Added a try except block around the whole update so that partial updates cannot leak.
        try: 
            with self.table_lock:
                base_range_id = self.get_base_range(base_rid)

                curr_vals = self.latest_cols(base_rid)
                old_values = curr_vals.copy()
                old_schema = self.base_schema.get(base_rid, 0)
                old_tail_rid = self.base_indirection.get(base_rid, INVALID_RID)

                prev_rid = old_tail_rid if old_tail_rid != INVALID_RID else base_rid
                curr_time = int(time())

                if old_tail_rid == INVALID_RID:
                    copy_rid = self.get_RID()
                    copy_vals = [0] * self.total_columns
                    copy_vals[INDIRECTION_COLUMN] = base_rid
                    copy_vals[RID_COLUMN] = copy_rid
                    copy_vals[TIMESTAMP_COLUMN] = curr_time
                    copy_vals[SCHEMA_ENCODING_COLUMN] = (1 << self.num_columns) - 1
                    copy_vals[4:] = curr_vals.copy()

                    self.write_record(is_tail=True, values=copy_vals, target_range_id=base_range_id)
                    self.tailtobase_merge[copy_rid] = base_rid
                    prev_rid = copy_rid

                tail_rid = self.get_RID()

                new_vals = curr_vals.copy()
                for col_id, val in update_cols.items():
                    new_vals[col_id] = int(val)

                new_schema = old_schema
                for col_id in update_cols.keys():
                    new_schema |= (1 << col_id)

                values = [0] * self.total_columns
                values[INDIRECTION_COLUMN] = prev_rid
                values[RID_COLUMN] = tail_rid
                values[TIMESTAMP_COLUMN] = curr_time
                values[SCHEMA_ENCODING_COLUMN] = new_schema
                values[4:] = new_vals

                self.write_record(is_tail=True, values=values, target_range_id=base_range_id)

                self.tailtobase_merge[tail_rid] = base_rid
                self.base_indirection[base_rid] = tail_rid
                self.base_schema[base_rid] = new_schema

            self.schedule_merge(base_range_id)
            merge_scheduled = True

            if not defer_index:
                for col_id, val in update_cols.items():
                    if col_id == self.key:
                        continue
                    self.index.update_entry(col_id, base_rid, old_values[col_id], int(val))
                    applied_index_cols.append(col_id)

            return {
                "tail_rid": tail_rid,
                "copy_rid": copy_rid,
                "base_range_id": base_range_id,
                "old_tail_rid": old_tail_rid,
                "old_schema": old_schema,
                "old_values": old_values,
                "update_cols": update_cols.copy()
            }
        
        except Exception:
            if old_values is not None:
                for col_id in reversed(applied_index_cols):
                    try:
                        self.index.update_entry(col_id, base_rid, int(update_cols[col_id]), old_values[col_id])
                    except Exception:
                        pass

            # The metadata should be restored to pre-update pointers state when stuff fails
            with self.table_lock:
                if base_rid in self.page_directory and base_rid not in self.deleted:
                    self.base_indirection[base_rid] = old_tail_rid
                    self.base_schema[base_rid] = old_schema

                if tail_rid is not None:
                    self.tailtobase_merge.pop(tail_rid, None)
                if copy_rid is not None:
                    self.tailtobase_merge.pop(copy_rid, None)

                if merge_scheduled and base_range_id is not None and 0 <= base_range_id < len(self.page_ranges):
                    pr = self.page_ranges[base_range_id]
                    pr.updates_postmerge = max(0, pr.updates_postmerge - 1)

            raise


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
    
    def delete_context(self, base_rid: int, defer_index: bool = False):
        with self.table_lock:
            if base_rid not in self.page_directory:
                return None
            if base_rid in self.deleted:
                return None

            old_vals = self.latest_cols(base_rid)
            old_indirection = self.base_indirection.get(base_rid, INVALID_RID)
            old_schema = self.base_schema.get(base_rid, 0)

            self.deleted.add(base_rid)
            self.base_indirection[base_rid] = DELETED_INDIRECTION

        removed_cols = []
        if not defer_index:
            try:
                for c in range(self.num_columns):
                    self.index.remove_entry(c, base_rid, old_vals[c])
                    removed_cols.append(c)
            except Exception:
                with self.table_lock:
                    self.deleted.discard(base_rid)
                    self.base_indirection[base_rid] = old_indirection
                    self.base_schema[base_rid] = old_schema
                for c in removed_cols:
                    try:
                        self.index.add_entry(c, base_rid, int(old_vals[c]))
                    except Exception:
                        pass
                raise

        
        return {
            "old_values": old_vals,
            "old_indirection": old_indirection,
            "old_schema": old_schema
        }


    def delete(self, base_rid: int):
        if self.delete_context(base_rid) is not None:
            return True;  
        return False;  


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
        covered_updates = 0
        with self.table_lock:
            if not (0 <= range_id < len(self.page_ranges)):
                return
            pr = self.page_ranges[range_id]
            if pr.merge_inprogress == True or pr.merge_ready == True:
                return

            pr.merge_inprogress = True
            pr.merge_queued = False
            covered_updates = pr.updates_postmerge

            num_base_pagesets = pr.num_base_pagesets
            num_tail_pagesets = pr.num_tail_pagesets
            tail_copy = pr.tail_pageset_counts.copy()
            tps_copy = pr.base_tps.copy()
            page_dir_copy = {}
            for rid, loc in self.page_directory.items():
                if loc[0] == range_id:
                    page_dir_copy[rid] = loc
            deleted = set(self.deleted)

            tailtobase_merge_copy = {}
            for tail_rid, base_rid in self.tailtobase_merge.items():
                loc = page_dir_copy.get(tail_rid)
                if loc is None:
                    continue
                _, is_tail, tail_pageset, tail_slot = loc
                if not is_tail:
                    continue
                if tail_pageset >= len(tail_copy):
                    continue
                if tail_slot >= tail_copy[tail_pageset]:
                    continue
                tailtobase_merge_copy[tail_rid] = base_rid

        # Clone base pages by reading from bufferpool
        try: 
            basepages_clone = []
            for ps_id in range(num_base_pagesets):
                pageset = []
                for col_id in range(self.total_columns):
                    pid = self._pid(range_id, False, ps_id, col_id)
                    orig = self.bufferpool.get_page(pid)
                    pageset.append(orig.merge_clone())
                    self.bufferpool.unpin(pid)
                basepages_clone.append(pageset)

                # Clone tail pages
            tailpages_clone = []
            for ps_id in range(num_tail_pagesets):
                pageset = []
                for col_id in range(self.total_columns):
                    pid = self._pid(range_id, True, ps_id, col_id)
                    orig = self.bufferpool.get_page(pid)
                    pageset.append(orig.merge_clone())
                    self.bufferpool.unpin(pid)
                tailpages_clone.append(pageset)

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
                self.pending_merges[range_id] = (merged_pages, new_base_tps, covered_updates)
                pr.merge_inprogress = False
                pr.merge_queued = False
                pr.merge_ready = True

        except Exception:  # added a try and except block around the whole merge logic for safer handling
            with self.table_lock:
                if 0 <= range_id < len(self.page_ranges):
                    pr = self.page_ranges[range_id]
                    pr.merge_inprogress = False
                    pr.merge_queued = False
                    pr.merge_ready = False
        
            raise 

    #Iterates through Previous RID until desired
    def get_previous_rid(self, base_rid: int, version: int):
        rid = self.latest_rid(base_rid)

        # if no updates exist
        if rid == base_rid:
            return base_rid

        for _ in range(version):
            prev_rid = self.read_val(rid, INDIRECTION_COLUMN)

            if prev_rid == INVALID_RID:
                return base_rid

            if prev_rid == base_rid:
                range_id, is_tail, base_pageset, slot = self.page_directory[base_rid]
                tps = self.page_ranges[range_id].base_tps[base_pageset]
                if tps != INVALID_RID:
                    return rid

            rid = prev_rid

        return rid


    # a bunch of indexing helpers 

    def index_insert(self, base_rid: int, inserted_cols: list[int]) -> bool:
        added_cols = []

        try:
            for c, val in enumerate(inserted_cols):
                self.index.add_entry(c, base_rid, int(val))
                added_cols.append(c)
            return True;

        except Exception:
            for c in reversed(added_cols):
                try:
                    self.index.remove_entry(c, base_rid, int(inserted_cols[c]))
                except Exception:
                    pass
            
            return False


    def index_update(self, base_rid: int, update_cols: dict[int, int], old_values: list[int]) -> bool:
        updated_cols = []
        
        try:
            for col_id, new_val in update_cols.items():
                if col_id == self.key:
                    continue
                self.index.update_entry(col_id, base_rid, old_values[col_id], int(new_val))
                updated_cols.append(col_id)
            
            return True
        
        except Exception:
            for col_id in reversed(updated_cols):
                try:
                    self.index.update_entry(col_id, base_rid, int(update_cols[col_id]), old_values[col_id])
                except Exception:
                    pass
            return False


    def index_delete(self, base_rid: int, old_values: list[int]) -> bool:
        removed_cols = []

        try:
            for c, val in enumerate(old_values):
                self.index.remove_entry(c, base_rid, int(val))
                removed_cols.append(c)
            
            return True
        
        except Exception:
            for c in reversed(removed_cols):
                try:
                    self.index.add_entry(c, base_rid, int(old_values[c]))
                except Exception:
                    pass
            return False

    
    # a bunch of atomicity rollback helpers 

    def rollback_insert(self, base_rid: int, inserted_cols: list[int], index_applied: bool = True):
        
        with self.table_lock:
            if base_rid not in self.page_directory:
                return True

            self.deleted.add(base_rid)
            self.base_indirection[base_rid] = DELETED_INDIRECTION
            self.base_schema.pop(base_rid, None)

        if index_applied:
            for c, val in enumerate(inserted_cols):
                self.index.remove_entry(c, base_rid, int(val))
        
        return True


    def rollback_update(self, base_rid: int, old_tail_rid: int, old_schema: int,
                        update_cols: dict[int, int], old_values: list[int],
                        tail_rid: int, copy_rid: int, base_range_id: int, 
                        index_applied: bool = True):
        
        with self.table_lock:
            if base_rid not in self.page_directory or base_rid in self.deleted:
                return True

            self.base_indirection[base_rid] = old_tail_rid
            self.base_schema[base_rid] = old_schema

            if tail_rid is not None:
                self.tailtobase_merge.pop(tail_rid, None)
            
            if copy_rid is not None:
                self.tailtobase_merge.pop(copy_rid, None)

            if 0 <= base_range_id < len(self.page_ranges):
                pr = self.page_ranges[base_range_id]
                pr.updates_postmerge = max(0, pr.updates_postmerge - 1)

        if index_applied: 
            for col_id, new_val in update_cols.items():
                if col_id == self.key:
                    continue
                self.index.update_entry(col_id, base_rid, int(new_val), old_values[col_id])

        return True


    def rollback_delete(self, base_rid: int, old_indirection: int, old_schema: int, 
                        old_values: list[int], index_applied: bool = True):
        
        with self.table_lock:
            if base_rid not in self.page_directory:
                return True

            self.deleted.discard(base_rid)
            self.base_indirection[base_rid] = old_indirection
            self.base_schema[base_rid] = old_schema

        if index_applied: 
            for c, val in enumerate(old_values):
                self.index.add_entry(c, base_rid, int(val))
        
        return True
