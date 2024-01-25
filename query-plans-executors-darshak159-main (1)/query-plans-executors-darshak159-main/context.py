# ExecutorContext 
# It stores all the context necessary for the transaction to 
# run a query i.e., an executor.

# setup_logger.py
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Context():
    """
    A class storing all the necesasry info (e.g., DB Catalog, BufferPoolManager, LockManager, TransactionManager) for the transaction that is executing the query.

    Attributes
    ----------
    catalog : Catalog
        DB catalog

    Methods
    ------
    getCatalog():
        return the DB catalog
    """
    
    def __init__(self, catalog, transaction_manager=None):
        self.catalog = catalog
       
    def getCatalog(self):
        return self.catalog