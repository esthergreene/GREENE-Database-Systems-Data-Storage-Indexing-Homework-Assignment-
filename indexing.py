"""
 * Name: indexing.py
 * Author: esthergreene
 * Project: Homework 1
 * Version: 09/23/2025
 * Purpose: Implement a Python-based storage system for variable-length records using the
 *    slotted-page structure as described in database systems theory. You will create a system
 *    that can store, retrieve, and manage records with variable-length fields within fixed-size pages.
 * Note: Sources have been cited and appear above respective code (e.g., "References:")
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
"""

"""
VariableLengthRecord
Takes a record (id, name, dept, salary) and (serialize) turns it into bytes so it can be stored, 
then (deserialize) turns those bytes back into a normal record when you need it.
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
            offsets.append(offsetCounter if f else 0); lengths.append(lengths(f)); offsetCounter+=lengths(f)
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
"""
class SlottedPage:
    def __init__(s): s.data, s.slots=[],[]
    def free(s): return PAGE_SIZE-HEADER_SIZE-len(s.data)-len(s.slots)*SLOT_SIZE
    def insert(s,rec):
