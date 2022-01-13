package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;

    private ConcurrentHashMap<PageId, Page> pageTable;

    private Replacer replacer;

    private LockManager lockManager;

    private final int READ_LOCK = 1;
    private final int WRITE_LOCK = 2;
    private class Lock {
        private int lockType;
        private TransactionId tid;

        public Lock(TransactionId tid, int lockType) {
            this.lockType = lockType;
            this.tid = tid;
        }
    }

    private class LockManager {
        private ConcurrentHashMap<PageId, ConcurrentLinkedQueue<Lock>> lockMap;
        LockManager() {
            lockMap = new ConcurrentHashMap<>();
        }

        public synchronized boolean acquiredLock(TransactionId tid, PageId pid, int lockType) {
            // current page is not being locked
            if (!lockMap.containsKey(pid) || lockMap.get(pid).size() == 0) {
                Lock lock = new Lock(tid, lockType);
                ConcurrentLinkedQueue locks = new ConcurrentLinkedQueue();
                locks.offer(lock);
                lockMap.put(pid, locks);
                return true;
            }
            // current page has been locked
            ConcurrentLinkedQueue<Lock> locks = lockMap.get(pid);
            for (Lock lock : locks) {
                if (lock.tid == tid) {
                    if (lock.lockType == lockType) {
                        // hold read  request read /   hold write  request write
                        return true;
                    } else if (lock.lockType == WRITE_LOCK) {
                        return true;
                    } else if (locks.size() == 1){
                        lock.lockType = WRITE_LOCK; // update
                        return true;
                    } else {
                        return false;
                    }
                } else if (lock.lockType == WRITE_LOCK) {
                    // other transaction holds write lock
                    return false;
                }
            }

            if (lockType == READ_LOCK) {
                Lock lock = new Lock(tid, READ_LOCK);
                locks.add(lock);
                return true;
            }
            return false;
        }

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            ConcurrentLinkedQueue<Lock> locks = lockMap.get(pid);
            Iterator<Lock> iterator = locks.iterator();
            while (iterator.hasNext()) {
                Lock lock = iterator.next();
                if (lock.tid == tid) {
                    iterator.remove();
                }
            }
        }

        public synchronized boolean hasLock(TransactionId tid, PageId pid) {
            if (!lockMap.containsKey(pid))
                return false;
            for (Lock lock : lockMap.get(pid)) {
                if (lock.tid == tid)
                    return true;
            }
            return false;
        }
    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageTable = new ConcurrentHashMap<>();
        replacer = new LRUReplacer(numPages);
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int lockType;
        if (perm.equals(Permissions.READ_ONLY)) {
            lockType = READ_LOCK;
        } else {
            lockType = WRITE_LOCK;
        }

        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 2000;
        while (!lockManager.acquiredLock(tid, pid, lockType)) {
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new TransactionAbortedException();
            }

        }
        if (!pageTable.containsKey(pid)) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);

            if (pageTable.size() >= numPages) {
                evictPage();
            }
            if (lockType == READ_LOCK) {
                replacer.add(pid);
            }

            pageTable.put(pid, page);
        }


        return pageTable.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
//        replacer.unpin(pid);
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.hasLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            for (PageId pageId : pageTable.keySet()) {
                if (holdsLock(tid, pageId)) {
                    if (commit) {
                        flushPage(pageId);
                    } else {
                        discardPage(pageId);
                    }
                    lockManager.releaseLock(tid, pageId);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid, t);
        for (Page page : pages) {
//            getPage(tid, page.getId(), Permissions.READ_WRITE);
            page.markDirty(true, tid);
            if (pageTable.size() >= numPages)
                evictPage();
            pageTable.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId recordId = t.getRecordId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(recordId.getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            if (pageTable.size() >= numPages)
                evictPage();
            pageTable.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry : pageTable.entrySet()) {
            Page page = entry.getValue();
            if (page.isDirty() != null) {
                flushPage(entry.getValue().getId());
            }
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (pageTable.containsKey(pid)) {
            pageTable.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        if (pageTable.containsKey(pid)) {
            Page page = pageTable.get(pid);
            TransactionId tid = page.isDirty();
            page.markDirty(false, tid);
            dbFile.writePage(page);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, Page> entry : pageTable.entrySet()) {
            Page page = entry.getValue();
            if (page.isDirty() == tid) {
                flushPage(entry.getKey());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId victimPid = replacer.victim();
        if (victimPid == null)
            return;
        try {
            HeapPage page = (HeapPage) pageTable.get(victimPid);
            if (page.isDirty() != null) {
                flushPage(victimPid);
            }
            pageTable.remove(victimPid);
        } catch (IOException e) {
            throw new DbException(e.getMessage());
        }
    }

}
