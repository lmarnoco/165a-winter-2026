class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        current_use = self.num_records*8
        current_capacity = 4096 - current_use
        return current_capacity >= 8

    def write(self, value):
        if not self.has_capacity(): 
            return False
        offset = self.num_records * 8
        #write the value to the array at position offset
        self.data[offset:offset+8] = value.to_bytes(8, byteorder='little', signed=True)
        self.num_records += 1
        return True
    
    def read(self, index):
        if index >= self.num_records:
            raise IndexError(f"Index {index} out of range")
        offset = index*8
        return int.from_bytes(self.data[offset:offset+8], byteorder='little', signed=True)

    def merge_write(self, index, value):
        if index < 0 or index >= self.num_records:
            raise IndexError("index value out of range")
       
        offset = index * 8
        self.data[offset:offset+8] = value.to_bytes(8, byteorder='little', signed=True)
    
    def merge_clone(self):
        p = Page()
        p.num_records = self.num_records
        p.data[:] = self.data
        return p
