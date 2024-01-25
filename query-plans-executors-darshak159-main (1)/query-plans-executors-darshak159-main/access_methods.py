# Access Methods Placeholder

# The sequencial scan operator usually calls the buffer manager and disk manager to get pages from the Disk and put them into memory,
# and access method to get data from Tables in the pages.
# We are ignoring these modules, and therefore this file contains the iterator that the sequential scan calls

# This file represents the interface for database files on disk. Access methods can fetch pages and iterate through tuples. 
# Please note that tuples from tables are generally accessed through the buffer pool by access methods, rather than directly
# by operators.

# setup_logger.py
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DBFileIterator():
    def __init__(self,db_file):
        self.db_file = db_file
        self.file = None
    
    def openIt(self):
        self.file = open(self.db_file, "r") 
    
    def getNext(self):
        if(self.file == None):
            logging.info(f"File not opened")
        else:         
            next_line = self.file.readline()
            if next_line != "":
                next_line = next_line.strip()
                content = next_line.split(",")
                return content
        return None
        
    def closeIt(self):
        # Close opend file
        self.file.close()
        
    def resetIt(self):
        self.file.seek(0)
        