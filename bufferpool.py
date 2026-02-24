from lstore.page import Page
from collections import OrderedDict
import os
import struct

DEFAULT_CAPACITY = 1024


def page_id(table_name: str, range_id: int, is_tail: bool, pageset_id: int, col_id: int) -> str:
    side = "tail" if is_tail else "base"
    return f"{table_name}_{range_id}_{side}_{pageset_id}_{col_id}"


class BufferPool:
    def __init__(self, capacity: int = DEFAULT_CAPACITY, path: str = "."):
        self.capacity = capacity
        self.path = path
        self.frames: dict[str, dict] = {}
        self.lru: OrderedDict = OrderedDict()  # oldest → newest





    def get_page(self, pid: str) -> Page:
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
        if pid in self.frames:
            self.frames[pid]['dirty'] = True

    def unpin(self, pid: str):
        if pid in self.frames:
            self.frames[pid]['pin'] = max(0, self.frames[pid]['pin'] - 1)

    def flush_all(self):
        for pid, frame in self.frames.items():
            if frame['dirty']:
                self._write_to_disk(pid, frame['page'])
                frame['dirty'] = False





    def _evict(self):
        for pid in self.lru:  # oldest first
            if self.frames[pid]['pin'] == 0:
                if self.frames[pid]['dirty']:
                    self._write_to_disk(pid, self.frames[pid]['page'])
                del self.frames[pid]
                del self.lru[pid]
                return
        raise RuntimeError("BufferPool: all pages pinned, cannot evict")

    def _filepath(self, pid: str) -> str:
        return os.path.join(self.path, pid + ".pg")

    def _load_from_disk(self, pid: str) -> Page:
        page = Page()
        filepath = self._filepath(pid)
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                header = f.read(8)
                raw = f.read(4096)
            page.num_records = struct.unpack('<q', header)[0]
            page.data = bytearray(raw)
        return page

    def _write_to_disk(self, pid: str, page: Page):
        filepath = self._filepath(pid)
        with open(filepath, 'wb') as f:
            f.write(struct.pack('<q', page.num_records))
            f.write(page.data)
