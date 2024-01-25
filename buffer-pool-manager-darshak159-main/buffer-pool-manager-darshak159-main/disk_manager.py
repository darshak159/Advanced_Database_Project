import page

# setup_logger.py
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DiskManager:
    """
    A class to represent the disk manager
    The Disk manager is responsible to read and write data to disk

    Attributes
    ----------
    pages : array
        list of page_id read from disk
    invalid : array
        pages that have been deleted from disk
        
    Methods
    -------
    readPage(page_id):
        reads the page from disk with id = page_id.
    writePage(page):
        writes a page to disk.
    deletePage():
        deletes a page in disk and set the page_id as invalid.
    createPage():
        creates a page in disk.
    """
        
    def __init__(self):
        self.pages = []
        self.invalid = []
        
    def readPage(self, page_id):
        """Reads the page from disk with id = page_id..

        Parameters
        ----------
        page_id : int
            The identifier of the page that is being requested

        Raises
        ------
        Error
           If the page_id is invalid.
        """
        logger.info(f"Reading page {page_id} from disk")
        if page_id not in self.invalid:
            if page_id not in self.pages:
                self.createPage(page_id)
            self.pages.append(page_id)
            return page.Page(page_id)
        else:
            logger.warning(f"Page {page_id} has been deleted from disk, and therefore it cannot be found")
            return None
    
    def createPage(self, page_id):
        logger.info(f"Creating page {page_id} in disk")
        pass
    
    def writePage(self, page):
        logger.info(f"Writing page {page.page_id} to disk") 
        pass
    
    def deletePage(self, page):
        logger.info(f"Deleting page {page.page_id} from disk")
        page_id = page.page_id
        self.pages.remove(page_id)
        self.invalid.append(page_id)
        
    