from lstore.page import Page
from collections import OrderedDict
import os
import struct
import threading

DEFAULT_CAPACITY = 32


def page_id(table_name: str, range_id: int, is_tail: bool, pageset_id: int, col_id: int) -> str:
    side = "tail" if is_tail else "base"
    return f"{table_name}_{range_id}_{side}_{pageset_id}_{col_id}"


class BufferPool:
    def __init__(self, capacity: int = DEFAULT_CAPACITY, path: str = "."):
        self.capacity = capacity
        self.path = path
        self.frames: dict[str, dict] = {}
        self.lru: OrderedDict = OrderedDict()  # oldest → newest
        self._lock = threading.Lock()  

    def get_page(self, pid: str) -> Page:
        with self._lock:
            if pid in self.frames:
                self.lru.move_to_end(pid)
                self.frames[pid]['pin'] += 1
                return self.frames[pid]['page']

            if len(self.frames) >= self.capacity:
                self._evict()

            page = self._load_from_disk(pid)
            self.frames[pid] = {'page': page, 'dirty': False, 'pin': 1}
            self.lru[pid] = None
            return page

    def mark_dirty(self, pid: str):
        with self._lock:
            if pid in self.frames:
                self.frames[pid]['dirty'] = True

    def unpin(self, pid: str):
        with self._lock:
            if pid in self.frames:
                self.frames[pid]['pin'] = max(0, self.frames[pid]['pin'] - 1)

    def install_page(self, pid: str, page: Page, dirty: bool = True):
        with self._lock:
            if pid in self.frames:
                if self.frames[pid]['pin'] > 0:
                    return False
                self.frames[pid]['page'] = page
                self.frames[pid]['dirty'] = self.frames[pid]['dirty'] or dirty
                self.lru.move_to_end(pid)
                return True

            if len(self.frames) >= self.capacity:
                self._evict()

            self.frames[pid] = {'page': page, 'dirty': dirty, 'pin': 0}
            self.lru[pid] = None
        
        return True

    def flush_all(self):
        with self._lock:
            for pid, frame in self.frames.items():
                if frame['dirty']:
                    self._write_to_disk(pid, frame['page'])
                    frame['dirty'] = False


    def install_pages_all(self, pages: dict, dirty: bool = True):
        with self._lock:
            target_pids = set(pages.keys())

            # don't overwrite any of the target pages that are currently pinned 
            for pid in target_pids: 
                frame = self.frames.get(pid)
                if frame is not None and frame["pin"] >0:
                    return False
            
            # added a fix here 
            # if the merge batch is actually bigger than the bufferpool capacity, the memory install 
            # would be kind of impossible --> idea to do writethrough for the uncached pages? 
            if len(target_pids) > self.capacity:
                for pid, page in pages.items():
                    frame = self.frames.get(pid)
                    if frame is not None:
                        frame["page"] = page
                        frame["dirty"] = frame["dirty"] or dirty
                        self.lru.move_to_end(pid)
                    else:
                        self._write_to_disk(pid, page)
                return True

            needed = 0
            for pid in target_pids:
                if pid not in self.frames:
                    needed += 1
            free_slots = self.capacity - len(self.frames)

            if needed > free_slots:
                evict = needed - free_slots
                candidates = []

                for pid in self.lru.keys():
                    if pid not in target_pids and self.frames[pid]["pin"] == 0:
                        candidates.append(pid)
                if len(candidates) < evict: 
                    return False
                
                for each in candidates[:evict]:
                    if self.frames[each]["dirty"]:
                        self._write_to_disk(each, self.frames[each]["page"])
                    del self.frames[each]
                    del self.lru[each]
            
            for pid, page in pages.items():
                if pid in self.frames:
                    self.frames[pid]["page"] = page
                    self.frames[pid]["dirty"] = self.frames[pid]["dirty"] or dirty
                    self.lru.move_to_end(pid)
                else: 
                    self.frames[pid] = {"page": page, "dirty": dirty, "pin": 0}
                    self.lru[pid] = None

            return True 


    def _evict(self):
        # first pass: evict clean unpinned pages
        for pid in list(self.lru.keys()):
            if self.frames[pid]['pin'] == 0 and not self.frames[pid]['dirty']:
                del self.frames[pid]
                del self.lru[pid]
                return
        # second pass: evict dirty unpinned pages
        for pid in list(self.lru.keys()):
            if self.frames[pid]['pin'] == 0:
                self._write_to_disk(pid, self.frames[pid]['page'])
                del self.frames[pid]
                del self.lru[pid]
                return
        raise RuntimeError("BufferPool: all pages pinned, cannot evict")

    def _filepath(self, pid: str) -> str:
        # pid format: tablename_rangeid_side_pagesetid_colid
        # files live at: db_path/tablename/rangeid_side_pagesetid_colid.pg
        parts = pid.rsplit('_', 4)  # saee - changed to use rsplit and split everything 
        if len(parts) != 5:
            raise ValueError(f"page id is invalid. pid: {pid}")
        
        table_name, range_id, side, pageset_id, col_id = parts
        rest = f"{range_id}_{side}_{pageset_id}_{col_id}"
        return os.path.join(self.path, table_name, rest + '.pg')

    def _load_from_disk(self, pid: str) -> Page:
        page = Page()
        filepath = self._filepath(pid)
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                header = f.read(8)
                raw = f.read(4096)
            page.num_records = struct.unpack('<q', header)[0]
            page.data = bytearray(raw)
        #else:
            #print(f"MISSING FROM DISK: {filepath}")
        return page

    def _write_to_disk(self, pid: str, page: Page):
        filepath = self._filepath(pid)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            f.write(struct.pack('<q', page.num_records))
            f.write(page.data)
