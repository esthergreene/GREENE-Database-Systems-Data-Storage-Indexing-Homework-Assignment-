"""
 * Name: indexing.py
 * Author: esthergreene
 * Project: Homework 1
 * Version: 09/23/2025
 * Purpose: Implement a Python-based storage system for variable-length records using the
 *    slotted-page structure as described in database systems theory. You will create a system
 *    that can store, retrieve, and manage records with variable-length fields within fixed-size pages.
 * Note: Sources have been cited and appear above respective code (e.g., "References:")
 * ChatGPT was used in the making of this code. 
"""

"""
References: 
https://www.w3schools.com/python/
https://www.python.org/about/gettingstarted/
https://www.geeksforgeeks.org/python/os-module-python-examples/
https://www.geeksforgeeks.org/python/struct-module-python/
"""

import struct, os

PAGE_SIZE, HEADER_SIZE, SLOT_SIZE = 4096, 4, 8

"""
References: 
https://www.geeksforgeeks.org/python/__init__-in-python/
https://www.tutorialspoint.com/python/python_serialization.htm
https://www.geeksforgeeks.org/python/serialize-and-deserialize-complex-json-in-python/
https://stackoverflow.com/questions/63228415/def-serializeself-what-does-it-do-in-django-models
https://stackoverflow.com/questions/45232298/null-bitmap-is-set-when-there-is-no-null-columns
https://www.geeksforgeeks.org/python/enumerate-in-python/
https://www.geeksforgeeks.org/python/python-check-if-nonetype-or-empty/
https://www.w3schools.com/python/ref_list_append.asp
https://www.w3schools.com/python/python_conditions.asp
https://www.w3schools.com/python/ref_string_encode.asp
https://www.geeksforgeeks.org/python/python-pack-method-in-tkinter/
https://www.geeksforgeeks.org/python/classmethod-in-python/
https://docs.python.org/3/library/struct.html
https://www.tutorialspoint.com/python/string_decode.htm
https://www.geeksforgeeks.org/python/python-repr-function/
https://www.w3schools.com/python/ref_func_range.asp
ChatGPT was used as an outline for this code (lines 47-80).
"""

"""
VariableLengthRecord
Represents a single record (like one row in a database table) with fields: id, name, dept, and salary.
Serialize: Converts the record into a sequence of bytes so it can be stored inside a page. Handles None 
values by marking them in a “null bitmap” and putting placeholders in the byte layout.
Deserialize: Does the reverse—takes the raw bytes from storage and reconstructs the original record as Python objects (id, name, etc.).
__repr__: Makes it easy to print a record in readable form like (EMP001, Alice Johnson, CS, 75000).
Takes a record (id, name, dept, salary) and (serialize) turns it into bytes so it can be stored, 
then (deserialize) turns those bytes back into a normal record when you need it.
Big idea: This class is the translator between human-readable records and machine-readable bytes.
"""
class VariableLengthRecord:
    def __init__(self, id, name, dept, salary): self.id, self.name, self.dept, self.salary = id, name, dept, salary
    def serialize(self):
        nullBitmap=0; emptyList=[]
        for i, v in enumerate([self.id, self.name, self.dept]):
            if v is None: nullBitmap|=1<<i; emptyList.append(b'')
            else: emptyList.append(str(v).encode())
        if self.salary is None: nullBitmap|=1<<3; sal=0.0
        else: sal=float(self.salary)
        offsets,lengths,offsetCounter=[],[],0
        for f in emptyList:
            offsets.append(offsetCounter if f else 0)
            lengths.append(len(f))
            offsetCounter+=len(f)
        out=[struct.pack("<B",nullBitmap)]+[struct.pack("<I",x) for x in sum(zip(offsets,lengths),())]+[struct.pack("<f",sal),b''.join(emptyList)]
        return b''.join(out)
    @classmethod
    def deserialize(classMethod, data):
        nullBitmap=data[0]; offsets=[struct.unpack("<I",data[1+8*i:5+8*i])[0] for i in range(3)]
        lengths=[struct.unpack("<I",data[5+8*i:9+8*i])[0] for i in range(3)]
        sal=struct.unpack("<f",data[25:29])[0]; vd=data[29:]
        get=lambda i: None if nullBitmap>>i&1 else vd[offsets[i]:offsets[i]+lengths[i]].decode()
        return classMethod(get(0),get(1),get(2),None if nullBitmap>>3&1 else sal)
    def __repr__(self): return f"({self.id},{self.name},{self.dept},{self.salary})"

"""
References: 
https://docs.python.org/3/c-api/memory.html
https://www.w3schools.com/python/ref_list_insert.asp
https://www.w3schools.com/python/ref_func_len.asp
https://www.geeksforgeeks.org/python/python-pack-method-in-tkinter/
https://docs.python.org/3/library/struct.html
https://www.w3schools.com/python/ref_func_range.asp
ChatGPT was used as an outline for this code (lines 93-133).
"""

"""
SlottedPage
A container for multiple VariableLengthRecords inside one page of fixed size (4096 bytes here). 
This follows the slotted-page structure from database systems. 
insert: Adds a new record’s bytes into the page if there’s space. Keeps track of where each record starts and its length using “slots.”
get: Fetches a record from the page by using its slot (index).
delete: Marks a slot as empty (doesn’t immediately remove the bytes).
pack: Converts the page into raw bytes so it can be written to a file.
unpack: Reads bytes from a file and rebuilds the page with its records and slots.
free: Reports how many bytes are still available in the page.
Holds a bunch of records in one page and keeps track of where each record starts and how long it is.
Big idea: This class is the page manager, organizing records within one block of storage.
"""
class SlottedPage:
    def __init__(s): s.data, s.slots=[],[]
    def free(s): return PAGE_SIZE-HEADER_SIZE-len(s.data)-len(s.slots)*SLOT_SIZE
    def insert(s,rec):
        b = rec.serialize()
        if s.free() < len(b) + SLOT_SIZE: return -1
        off = HEADER_SIZE + len(s.data)
        s.data += b; s.slots.append((off, len(b)))
        return len(s.slots) - 1
    def get(s, i):
        if i >= len(s.slots) or not s.slots[i][1]: return None
        off, ln = s.slots[i]
        return VariableLengthRecord.deserialize(bytes(s.data[off-HEADER_SIZE:off-HEADER_SIZE+ln]))
    def delete(s, i): s.slots[i] = (0, 0)
    def pack(s):
        p = bytearray(PAGE_SIZE)
        struct.pack_into("<HH", p, 0, len(s.slots), HEADER_SIZE+len(s.data))
        p[HEADER_SIZE:HEADER_SIZE+len(s.data)] = bytes(s.data)
        pos = PAGE_SIZE - len(s.slots) * SLOT_SIZE
        for o, l in s.slots: struct.pack_into("<II", p, pos, o, l); pos += SLOT_SIZE
        return bytes(p)
    @classmethod
    def unpack(c, d):
        n, f = struct.unpack_from("<HH", d, 0)
        pg = c(); pg.data = list(d[HEADER_SIZE:f])
        pos = PAGE_SIZE - n * SLOT_SIZE
        pg.slots = [struct.unpack_from("<II", d, pos+i*SLOT_SIZE) for i in range(n)]
        return pg

"""
References: 
https://www.geeksforgeeks.org/python/python-open-function/
https://www.w3schools.com/python/ref_file_close.asp
https://www.geeksforgeeks.org/python/python-os-path-size-method/
https://www.tutorialspoint.com/python/file_seek.htm
https://www.w3schools.com/python/ref_file_write.asp
https://www.w3schools.com/python/ref_func_range.asp
https://www.w3schools.com/python/ref_list_insert.asp
https://www.geeksforgeeks.org/python/python-dictionary-get-method/
https://www.geeksforgeeks.org/python/__name__-a-special-variable-in-python/
https://www.geeksforgeeks.org/python/python-list-remove/
https://dev.to/smac89/better-way-to-tryexceptpass-in-python-2460
ChatGPT was used as an outline for this code (lines 152-187).
"""   

"""
RecordFile
Manages the entire file made up of multiple pages (SlottedPages). Think of it like the database file on disk.
Looks after the whole file of pages and lets me put records in, pull them out, or delete them.
ins: Finds a page with enough free space and inserts the record there. If none exist, it creates a new page.
get: Retrieves a record by giving page number (pn) and slot number (sn).
delete: Removes a record from the file by deleting it from the right page.
rp / wp: Reads or writes a page from/to the file.
np: Counts how many pages are currently stored in the file.
Big idea: This class is the file-level manager, coordinating multiple pages and ensuring records get stored and retrieved properly across the whole file.
"""
class RecordFile:
    def __init__(s,f): s.f = f; open(f,"ab").close()
    def __init__(s, f): s.f = f; open(f, "ab").close()
    def np(s): return os.path.getsize(s.f)//PAGE_SIZE
    def rp(s, i): return SlottedPage.unpack(open(s.f, "rb").read()[i*PAGE_SIZE:(i+1)*PAGE_SIZE])
    def wp(s, p, i):
        with open(s.f, "r+b") as f: f.seek(i*PAGE_SIZE); f.write(p.pack())
    def ins(s, r):
        for i in range(s.np()):
            p = s.rp(i); sl = p.insert(r)
            if sl != -1: s.wp(p, i); return i, sl
        p = SlottedPage(); sl = p.insert(r); open(s.f, "ab").write(p.pack()); return s.np()-1, sl
    def get(s, pn, sn): return s.rp(pn).get(sn)
    def delete(s, pn, sn): p = s.rp(pn); p.delete(sn); s.wp(p, pn)

if __name__ == "__main__":
    fn = "instructors.dat"; 
    try: os.remove(fn)
    except: pass
    rf = RecordFile(fn)
    recs = [("EMP001","Alice Johnson","CS",75000),("EMP002","Bob Smith","Math",68000),
            ("EMP003","Carol Williams","Physics",None),("EMP004","David Brown","CS",82000)]
    locs = [rf.ins(VariableLengthRecord(*r)) for r in recs]
    for (pn,sn),r in zip(locs,recs): print("Inserted",r,"at",pn,sn)
    print("Retrieve:",[rf.get(p,s) for p,s in locs])
    rf.delete(*locs[1]); print("After delete:",[rf.get(p,s) for p,s in locs])
