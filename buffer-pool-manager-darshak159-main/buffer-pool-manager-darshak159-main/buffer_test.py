import buffer_pool_manager as bm

def test_init():
    buffer_mng = bm.BufferPoolManager(4)
    assert len(buffer_mng.getPageTable()) == 0
    assert len(buffer_mng.getBufferPool()) == 0
    assert len(buffer_mng.getReplacer().getFreeFrames()) == 0 

def test_add1():
    buffer_mng = bm.BufferPoolManager(4)
    pg = buffer_mng.fetchPage(0)
    assert len(buffer_mng.getPageTable()) == 1
    assert len(buffer_mng.getBufferPool()) == 1
    assert pg.getPinCount() == 1
    assert len(buffer_mng.getReplacer().getFreeFrames()) == 0 

def test_add2():
    buffer_mng = bm.BufferPoolManager(4)
    pg = buffer_mng.fetchPage(0)
    pg = buffer_mng.fetchPage(0)
    assert len(buffer_mng.getPageTable()) == 1
    assert len(buffer_mng.getBufferPool()) == 1
    assert pg.getPinCount() > 1

def test_add3():
    buffer_mng = bm.BufferPoolManager(4)
    pg = buffer_mng.fetchPage(0)
    pg = buffer_mng.fetchPage(1)
    assert len(buffer_mng.getPageTable()) == 2
    assert len(buffer_mng.getBufferPool()) == 2


def test_add4():
    buffer_mng = bm.BufferPoolManager(4)
    pg = buffer_mng.fetchPage(0)
    pg = buffer_mng.fetchPage(1)
    pg = buffer_mng.fetchPage(2)
    pg = buffer_mng.fetchPage(3)
    pg = buffer_mng.fetchPage(4)
    assert pg == False

def test_add5():
    buffer_mng = bm.BufferPoolManager(4)
    pg = buffer_mng.fetchPage(0)
    pg = buffer_mng.fetchPage(1)
    pg = buffer_mng.fetchPage(2)
    pg = buffer_mng.fetchPage(3)

    buffer_mng.unpinPage(0, False)

    assert len(buffer_mng.getReplacer().getFreeFrames()) == 1
    pg = buffer_mng.fetchPage(4)
    assert len(buffer_mng.getReplacer().getFreeFrames()) == 0

def test_add5():
    buffer_mng = bm.BufferPoolManager(4)
    pg = buffer_mng.fetchPage(0)
    pg = buffer_mng.fetchPage(1)
    pg = buffer_mng.fetchPage(2)
    pg = buffer_mng.fetchPage(3)

    buffer_mng.unpinPage(1, False)
    buffer_mng.unpinPage(0, False)
    buffer_mng.unpinPage(2, False)

    assert len(buffer_mng.getReplacer().getFreeFrames()) == 3
    assert buffer_mng.getReplacer().victim() == 2

def test_pin():
    buffer_mng = bm.BufferPoolManager(4)
    pg = buffer_mng.fetchPage(0)
    assert pg.getPinCount() == 1
    pg = buffer_mng.fetchPage(0)
    assert pg.getPinCount() == 2
    buffer_mng.unpinPage(0, False)
    assert pg.getPinCount() == 1
    buffer_mng.unpinPage(0, False)
    assert pg.getPinCount() == 0
    assert len(buffer_mng.getReplacer().getFreeFrames()) == 1
    assert buffer_mng.getReplacer().getFreeFrames()[0] == 0


def test_delete1():
    buffer_mng = bm.BufferPoolManager(4)
    pg = buffer_mng.fetchPage(0)
    assert pg.getPinCount() == 1
    pg = buffer_mng.fetchPage(0)
    assert pg.getPinCount() == 2
    buffer_mng.unpinPage(0, False)
    assert pg.getPinCount() == 1
    assert buffer_mng.deletePage(0) == False
 

def test_delete2():
    buffer_mng = bm.BufferPoolManager(4)
    pg = buffer_mng.fetchPage(0)
    assert pg.getPinCount() == 1
    pg = buffer_mng.fetchPage(0)
    assert pg.getPinCount() == 2
    buffer_mng.unpinPage(0, False)
    assert pg.getPinCount() == 1
    buffer_mng.unpinPage(0, False)
    assert pg.getPinCount() == 0
    assert buffer_mng.deletePage(0) == True


