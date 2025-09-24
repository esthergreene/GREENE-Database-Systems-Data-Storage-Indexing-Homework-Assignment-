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
