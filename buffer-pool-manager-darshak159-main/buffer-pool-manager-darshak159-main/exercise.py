import buffer_pool_manager as bm
from buffer_pool_manager import BufferPoolManager

def main():


    ## IMPLEMENT THE FOLLOWING STEPS USING YOUR BUFFER POOL MANAGER


    #   # 1. Create a BufferPool that can hold 16 frames
    buffer_pool = BufferPoolManager(16)

    # 2. Add 16 pages (page 0 to 15) to the BufferPool
    for i in range(16):
        buffer_pool.fetchPage(i)

    # 3. Print the Buffer's Pool PageTable
    print("Buffer Pools page table", buffer_pool.getPageTable())

    # 4. Print the replacer free frames list
    print("Free frame list", buffer_pool.replacer.getFreeFrames())

    # 5. Fetch page #14 from your bufferPool and prints the page_id
    fetched_page = buffer_pool.fetchPage(14)
    print("Fetched page number 14", fetched_page.page_id)

    # 6. Print page #14 pin counter
    print("page 14 pin counter", fetched_page.pin_count)

    # 7. Fetch page #16 from your bufferPool and prints the page_id. Did it work? Why?
    fetched_page_16 = buffer_pool.fetchPage(16)
    if fetched_page_16:
        print("Fetched page number 16", fetched_page_16.page_id)
    else:
        print("Page 16 not in buffer pool. It didn't work because the buffer pool is full.")

    # 8. Print the Buffer's Pool PageTable
    print("Buffer Pools page table", buffer_pool.getPageTable())

    # 9. Unpin page #14. Page #14 is not dirty
    buffer_pool.unpinPage(14, is_dirty=False)

    # 10. Try again to fetch page #16. Did it work? Why?
    fetched_page_16_retry = buffer_pool.fetchPage(16)
    if fetched_page_16_retry:
        print("Fetched page number 16", fetched_page_16_retry.page_id)
    else:
        print("Page 16 not in buffer pool. It didn't work because the buffer pool is full and page 14 is still pinned.")

    # 11. Print the replacer free frame
    print("Free frame list", buffer_pool.replacer.getFreeFrames())

    # 12. Unpin page #14 again. Page 14 is not dirty
    buffer_pool.unpinPage(14, is_dirty=False)

    # 13. Print the replacer free frame
    print("Free frame list", buffer_pool.replacer.getFreeFrames())

    # 14. Try again to fetch page #16. Did it work? Why?
    fetched_page_16_retry_2 = buffer_pool.fetchPage(16)
    if fetched_page_16_retry_2:
        print("Fetched page number 16", fetched_page_16_retry_2.page_id)
    else:
        print("Page 16 not in buffer pool. It didn't work because the buffer pool is full and page 14 is still pinned.")

    # 15. Print the Buffer's Pool PageTable
    print("Buffer Pools page table", buffer_pool.getPageTable())

    # 16. Unpin page #9 and #12. They are both dirty
    buffer_pool.unpinPage(9, is_dirty=True)
    buffer_pool.unpinPage(12, is_dirty=True)

    # 17. Fetch page #14 from your bufferPool and prints the page_id. What happened with the page that was replaced to make room for page #14?
    fetched_page_14_retry = buffer_pool.fetchPage(14)
    if fetched_page_14_retry:
        print("Fetched page number 14", fetched_page_14_retry.page_id)
    else:
        print("Page 14 not in buffer pool. It didn't work because the buffer pool is full and pages 9 and 12 are still dirty and pinned.")

    # 18. Delete page #5. Did it work? Why?
    buffer_pool.deletePage(5)

    # 19. Unpin page #5. The page is not dirty
    buffer_pool.unpinPage(5, is_dirty=False)

    # 20. Try to delete page #5 again. Did it work?
    buffer_pool.deletePage(5)

    # 21. Print page table
    print("Buffer Pools page table", buffer_pool.getPageTable())

    # 22. Fetch page #5x
    fetched_page_5x = buffer_pool.fetchPage(5)
    if fetched_page_5x:
        print("Fetched page number 5x", fetched_page_5x.page_id)
    else:
        print("Page 5x not in buffer pool. It didn't work because page 5 is deleted.")

    return

main()
