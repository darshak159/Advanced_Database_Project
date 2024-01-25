import disk_manager as dm
import lru_replacer as lru

# setup_logger.py
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BufferPoolManager:
    """
    A class to represent the BufferPoolManager
    The BufferPoolManager is responsible for fetching database pages from the DiskManager and 
    storing them in memory. 
    
    The BufferPoolManager must write the contents of a dirty Page back to disk before that object can be reused. It 
    writes dirty pages out to disk when it is either explicitly instructed to do so or when it needs to evict a page to make space for a new page.
    
    The BufferPoolManager is not allowed to free a Page that is pinned. 
    
    This BufferPoolManager implementation will use the LRUReplacer class. 
    

    Attributes
    ----------
    buffer_pool : array
        list with page objects
    page_table: array
        list of page_ids current in the buffer pool 
    buffer_total_no_of_frames: int
        total of frames that the buffer pool can hold
    disk_manager: DiskManager
        diskManager object
    replacer: LRUReplacer
        replacer object
   
    Methods
    -------
    getBufferPoll():
        returns all page objects in the buffer pool
    getPageTable():
        return the index of all pages objects currently in the buffer pool
    getDiskManager():
        return disk manager object
    getReplacer():
        return replace object
    fetchPage(page_id):
        returns a page stored in memory. If the page is not in memory, then the page must be read from the disk.
    newPage(page_id):
        reads a page from the disk using the disk manager
    deletePage(page_id):
        deletes a page from the buffer pool and from the disk
    unpinPage(page_id, is_dirty):
        uping a page so the replacer knows it is a free frame that can be evited if needed.
    flushPage(page_id):
        flushs a page with id = page_id from the buffer pool to disk regardless of its pin status.
    flushAllPages():
        flush all pages from the buffer pool to disk regardless of its pin status.
    
    """
    
    def __init__(self, no_of_frames):
        self.buffer_pool = []
        self.page_table = []
        self.disk_manager = dm.DiskManager()
        self.replacer = lru.LRUReplacer()
        self.buffer_total_no_of_frames = no_of_frames
        
    def getBufferPool(self):
        return self.buffer_pool
    
    def getPageTable(self):
        return self.page_table
    
    def getReplacer(self):
        return self.replacer
    
    def getDiskManager(self):
        return self.disk_manager
    
    def fetchPage(self, page_id):
        # Fetch page from the buffer pool, if not in memory than get from disk
        logging.info("Accessing the page {page_id} from the buffer pool")
        #  1. search the page table for the requested page_id.
        if page_id in self.page_table:
            for page in self.buffer_pool:
                if page.page_id == page_id:
                    page.incrementPinCount += 1
                    return page
        else:
            if self.buffer_total_no_of_frames > len(self.buffer_pool):
                page = self.disk_manager.readPage(page_id)
                if page:
                    self.buffer_pool.append(page)
                    self.page_table.append(page_id)
                    self.replacer.pin(page_id)
                    return page
                else:
                    logging.warning("All pages are pinned")
        

        return
                    

    def loadPage(self, page_id):
        logger.info(f"Fetching a new page to the bufferpool. Page {page_id} is not in bufferpool, it has to be read from disk")
        
        # Goal: Fetch page from the disk manager and put in the buffer pool. 
        # Checks if page_id is valid in disk, if valid increment its pin count, pin it and return the page. 
        # Otherwise return a error message and page = None.
        if page_id in self.buffer_pool:
            page = self.buffer_pool[page_id]
            page.pin.count +=1
            return page
        else:
            logger.warning("Error: page is not in the disk")
        return None
    
    def deletePage(self, page_id):
        logger.info(f"Deleting page {page_id} from bufferpool")
        
        #only delete the pages that are not pinned. if pinned print a error message and return False
        if page_id in self.page_table:
            del self.buffer_pool[page_id]
      
        return
   
    def unpinPage(self, page_id, is_dirty):
        logger.info(f"Unpinning page {page_id} in the bufferpool")
        #decrements the page pin counter and update page "dirtyness" with the value of is_dirty parameter".
        #if pin_count == 0 then unpin the page.
        for page in self.buffer_pool:
            if page.page_id == page_id:
                page.pin_count -= 1
                if page.pin_count == 0:
                    self.replacer.unpin(page_id)
                    if is_dirty:
                        self.flushPage(page_id)
                break
        return

    def flushPage(self, page_id):
        logger.info(f"Flushing page {page_id} out of the bufferpool")
        #write page to disk and update its metadata
        for page in self.buffer_pool:
            if page.page_id == page_id:
                self.disk_manager.writePage(page)
                break
        return

    def flushAllPages(self):
        logger.info(f"Flushing all pages out of the bufferpool")
        #write all pages to disk and update metadata
        for page in self.buffer_pool:
            self.disk_manager.writePage(page)
        return
