package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    private BufferPool bufferPool;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
        this.bufferPool = Database.getBufferPool();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();

        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(file, "r");
//            if ( (pgNo + 1) * BufferPool.getPageSize() > f.length()) {
//                f.close();
//                throw new IllegalArgumentException(String.format("table %d is invalid", tableId));
//            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            f.seek(pgNo * BufferPool.getPageSize());
            // read
            int read = f.read(bytes, 0, BufferPool.getPageSize());
//            if (read != BufferPool.getPageSize()) {
//                throw new IllegalArgumentException(String.format("table %d is invalid", tableId));
//            }
            HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(heapPageId, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d is invalid", tableId));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNumber = page.getId().getPageNumber();
        int offset = BufferPool.getPageSize() * pageNumber;
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.seek(offset);
        f.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        int pgNum;
        for ( pgNum = 0; ; pgNum++) {
            HeapPageId pageId = new HeapPageId(getId(), pgNum);
            HeapPage page = (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                if (pgNum >= numPages()) {
                    writePage(page);
                }
                return Collections.singletonList(page);
            }
        }
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId recordId = t.getRecordId();
        HeapPage page = (HeapPage) bufferPool.getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return Collections.singletonList(page);

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapDbFileIterator(this, tid);
    }

    class HeapDbFileIterator implements DbFileIterator {
        private final HeapFile hf;
        private final TransactionId tid;
        private final int tableId;
        private Iterator<Tuple> pageIterator;
        private int pageCur = 0;

        public HeapDbFileIterator(HeapFile hf, TransactionId tid) {
            this.hf = hf;
            this.tid = tid;
            this.tableId = hf.getId();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (pageCur >= hf.numPages()) {
                return;
            }
            PageId pageId = new HeapPageId(tableId, pageCur);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            pageIterator = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (pageIterator == null) {
                return false;
            }
            for (; ; ) {
                if (pageIterator.hasNext()) {
                    return true;
                }
                pageCur++;
                if (pageCur >= hf.numPages()) {
                    pageIterator = null;
                    return false;
                }
                PageId pageId = new HeapPageId(tableId, pageCur);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                pageIterator = page.iterator();
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (pageIterator == null) {
                throw new NoSuchElementException();
            }
            for (; ; ) {
                if (pageIterator.hasNext()) {
                    return pageIterator.next();
                }
                pageCur++;
                if (pageCur >= hf.numPages()) {
                    pageIterator = null;
                    throw new NoSuchElementException();
                }
                PageId pageId = new HeapPageId(tableId, pageCur);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                pageIterator = page.iterator();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageCur = 0;
            PageId pageId = new HeapPageId(tableId, pageCur);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            pageIterator = page.iterator();
        }

        @Override
        public void close() {
            pageIterator = null;
        }
    }

    private static final class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> iter;
        private int currPage;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.heapFile = file;
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            currPage = 0;
            iter = getPageTuples(currPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNo) throws TransactionAbortedException, DbException {
            if (pageNo >= 0 && pageNo < heapFile.numPages()) {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool()
                        .getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            } else {
                throw new DbException(String.format("heapfile %d does not contain page %d", pageNo, heapFile.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
//            if (iter == null)
//                return false;
            while (iter == null || !iter.hasNext()) {
                if (currPage < (heapFile.numPages() - 1)) {
                    currPage ++;
                    iter = getPageTuples(currPage);
                } else {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iter == null)
                throw new NoSuchElementException();
            while (!iter.hasNext()) {
                if (currPage < (heapFile.numPages() - 1)) {
                    currPage ++;
                    iter = getPageTuples(currPage);
                } else {
                    return null;
                }
            }
//            iter = getPageTuples(currPage);
            return iter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            iter = null;
        }
    }

}

