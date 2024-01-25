# Disk Manager Placeholder

# The sequencial scan operator usually calls the buffer manager and disk manager to get pages from the Disk and put them into memory,
# and access method to get data from Tables in the pages.
# We are ignoring these modules, and therefore this file contains the iterator that the sequential scan calls

# This file represents the interface for database files on disk. Each table is represented by a
# single DbFile. 
# Each file has a unique id used to store metadata about the table in the Catalog.
# Please note that disk files are generally accessed through the buffer pool, rather than directly
# by operators.

class DBFile():
    def __init__(self,iterator):
        self.iterator = iterator
    
    def getIterator(self):
        return self.iterator
    
