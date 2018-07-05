package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File file;
    TupleDesc tupleDesc;
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
        Database.getCatalog().addTable(this);
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
        return this.file.getAbsolutePath().hashCode();
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
        byte[] bytes = new byte[BufferPool.getPageSize()];
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(this.file, "r");
            if(pid.getPageNumber() * BufferPool.getPageSize() >= file.length()) {
                return null;
            }
            int len = (int)Math.min(BufferPool.getPageSize(),file.length()-pid.getPageNumber() * BufferPool.getPageSize());
            raf.seek(pid.getPageNumber() * BufferPool.getPageSize());
            raf.read(bytes,0,len);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            System.out.println("pid.getPageNumber(): "+pid.getPageNumber() );
            System.out.println("file.length(): "+file.length() );
        }
        Page p = null;
        try {
            p = new HeapPage(new HeapPageId(pid.getTableId(),pid.getPageNumber()),bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return p;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        long offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        raf.seek(offset);
        raf.write(page.getPageData(),0,BufferPool.getPageSize());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)file.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> res = new ArrayList<>();
        for (int pgNo =0;pgNo<numPages();pgNo++) {
            HeapPageId heapPageId = new HeapPageId(this.getId(),pgNo);
            HeapPage p =(HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY);
            if (p != null && p.getNumEmptySlots() > 0) {
                TransactionLockMap.releasePage(tid,heapPageId);
                p = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
                p.insertTuple(t);
//                writePage(p);
                res.add(p);
                return res;
            }
            TransactionLockMap.releasePage(tid,heapPageId);
        }
        // 需要新增
        HeapPageId heapPageId = new HeapPageId(this.getId(),numPages());
        HeapPage p =new HeapPage(heapPageId,HeapPage.createEmptyPageData());
        TransactionLockMap.dbFileLock(tid,this);
        writePage(p);
        TransactionLockMap.releaseDbFile(tid,this);
        // 加入到buffer pool 中
        p = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
        p.insertTuple(t);
        res.add(p);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> res = new ArrayList<>();
        for (int pgNo =0;pgNo<numPages();pgNo++) {
            HeapPageId heapPageId = new HeapPageId(this.getId(),pgNo);
            HeapPage p =(HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
            int previousEmpty = p.getNumEmptySlots();
            if (p != null && previousEmpty < p.numSlots) {
                p.deleteTuple(t);
                if (previousEmpty - 1 == p.getNumEmptySlots()) {
                    res.add(p);
                }
            }
        }

        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

    public class HeapFileIterator extends AbstractDbFileIterator {
        private Iterator<Tuple> tupleIterator;
        private HeapFile heapFile;
        private TransactionId transactionId;
        private int pageNo;
        public HeapFileIterator(HeapFile f,TransactionId tid){
            this.heapFile = f;
            this.transactionId = tid;
        }
        @Override
        public void close() {
            // Ensures that a future call to next() will fail
            super.close();
            this.tupleIterator = null;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) return null;
            Tuple tuple = null;
            if(tupleIterator.hasNext()) {
                tuple = tupleIterator.next();
            }else {
                while (tuple == null && this.pageNo+1 < heapFile.numPages() ) {
                    this.pageNo++;
                    HeapPageId heapPageId = new HeapPageId(this.heapFile.getId(),pageNo);
                    HeapPage p =(HeapPage) Database.getBufferPool().getPage(transactionId,heapPageId,Permissions.READ_ONLY);
                    if (p != null) {
                        tupleIterator = p.iterator();
                        tuple =tupleIterator.hasNext() ?tupleIterator.next(): null;
                    }
                }

            }
            return tuple;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId heapPageId = new HeapPageId(this.heapFile.getId(),pageNo);
            HeapPage p =(HeapPage) Database.getBufferPool().getPage(transactionId,heapPageId,Permissions.READ_ONLY);
            if(p != null) {
                this.tupleIterator = p.iterator();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.pageNo = 0;
            open();
        }
    }

}

