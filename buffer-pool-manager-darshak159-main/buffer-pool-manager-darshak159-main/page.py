class Page:
    """
    A class to represent a in-memory page.
    A page object maintains a counter for the number of threads that have pinned that page, and keeps track of 
    whether it is dirty or not. 

    Attributes
    ----------
    page_id : int
        the page object's identifier
    pin_count : int
        counter of how many threads have pin this page
    dirty : bool
        tracks if a page has been modified by a thread

    Methods
    -------
    incrementPinCount():
        increments the page's pin counter.
    decrementPinCount():
        decrements the page's pin counter.
    isDirty():
        returns if a page has been modified.
    getPinCount():
        return the page's pin counter.
    """
    def __init__(self, id):
        self.page_id = id
        self.pin_count = 0
        self.dirty = False

    def incrementPinCount(self):
        self.pin_count += 1

    def decrementPinCount(self):
        self.pin_count -= 1

    def isDirty(self):
        return self.dirty

    def getPinCount(self):
        return self.pin_count